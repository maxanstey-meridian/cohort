using System.Reflection;
using System.Text.Json;
using System.Text.Json.Serialization;

using Xunit.Sdk;

namespace Cohort.Tests;

public sealed class ComplianceManifestContractTests
{
    private const string ThisAssembly = "Cohort.Tests";
    private static readonly string[] AllowedAssemblies = [ThisAssembly, "Cohort.Sample.Tests"];
    private static readonly string[] AllowedStatuses = ["supported", "non-goal"];
    private static readonly string[] AllowedKinds = ["public-postgresql", "hosted-postgresql", "migration-postgresql", "package-consumer", "pure-contract", "architecture", "internal-mechanism"];

    [Fact]
    public void Manifest_contract_is_well_formed_and_unit_evidence_is_structurally_linked()
    {
        var options = new JsonSerializerOptions
        {
            PropertyNameCaseInsensitive = false,
            PropertyNamingPolicy = JsonNamingPolicy.CamelCase,
            UnmappedMemberHandling = JsonUnmappedMemberHandling.Disallow,
        };
        var manifest = JsonSerializer.Deserialize<ComplianceManifest>(
            File.ReadAllText(Path.Combine(AppContext.BaseDirectory, "compliance-manifest.json")),
            options
        )!;
        var discovered = DiscoverTests();

        manifest.ManifestVersion.Should().MatchRegex(@"^2\.\d+\.\d+$");
        manifest.Requirements.Select(requirement => requirement.Id).Should().OnlyHaveUniqueItems();
        manifest.Requirements.Should().OnlyContain(requirement =>
            !string.IsNullOrWhiteSpace(requirement.Source)
            && !string.IsNullOrWhiteSpace(requirement.Requirement)
            && AllowedStatuses.Contains(requirement.Status, StringComparer.Ordinal)
            && requirement.Evidence.Select(Identity).Distinct(StringComparer.Ordinal).Count() == requirement.Evidence.Count
        );
        manifest.Requirements.Where(requirement => requirement.Status == "supported")
            .Should().OnlyContain(requirement => requirement.Evidence.Any(item => item.Primary == true));
        manifest.Requirements.Where(requirement => requirement.Status != "supported")
            .Should().OnlyContain(requirement => requirement.Evidence.Count == 0);
        manifest.Requirements.Should().OnlyContain(requirement => HasStructurallyValidPrimaryEvidence(requirement));

        var evidence = manifest.Requirements.SelectMany(requirement => requirement.Evidence).ToArray();
        evidence.Should().OnlyContain(item =>
            AllowedAssemblies.Contains(item.Assembly, StringComparer.Ordinal)
            && AllowedKinds.Contains(item.Kind, StringComparer.Ordinal)
            && item.Primary.HasValue
            && !string.IsNullOrWhiteSpace(item.Type)
            && !string.IsNullOrWhiteSpace(item.Method)
        );
        evidence.Where(item => item.Assembly == ThisAssembly)
            .Should().OnlyContain(item => discovered.Contains(Identity(item)),
                "every Cohort.Tests evidence identity must resolve uniquely to a structurally executable xUnit target");
    }

    [Theory]
    [InlineData("MIGRATION-999", "Cohort product invariant: migration", "supported", "public-postgresql", true, false)]
    [InlineData("MIGRATION-999", "Cohort product invariant: migration", "supported", "migration-postgresql", false, false)]
    [InlineData("PUBLIC-999", "Cohort product invariant: observable behavior", "supported", "internal-mechanism", true, false)]
    [InlineData("NONGOAL-999", "Cohort product boundary", "non-goal", "architecture", false, false)]
    [InlineData("INTERNAL-999", "Cohort internal invariant: cleanup", "supported", "internal-mechanism", true, true)]
    public void Manifest_primary_evidence_rules_reject_malformed_requirements(
        string id,
        string source,
        string status,
        string kind,
        bool primary,
        bool expected
    )
    {
        var evidence = new ComplianceEvidence(ThisAssembly, "Example.Type", "Example_method", kind, primary);
        var requirement = new ComplianceRequirement(id, source, "Example requirement", [evidence], status);

        HasStructurallyValidPrimaryEvidence(requirement).Should().Be(expected);
    }

    [Fact]
    public void Dispatcher_Poll_And_Heartbeat_Waits_Use_OperationalTime_DelayAsync()
    {
        var source = ReadProductionSource("Infrastructure", "Handlers", "RetentionRowDispatcher.cs");

        source.Split("OperationalTime.DelayAsync", StringSplitOptions.None)
            .Should().HaveCount(3, "the dispatcher poll and heartbeat waits must use bounded delay chunking");
    }

    [Fact]
    public void Sweep_Orchestrators_Do_Not_Accumulate_Run_History_Record_Ids()
    {
        var sources = new[]
        {
            ReadProductionSource("Infrastructure", "RetentionSweepEngine.cs"),
            ReadProductionSource("Infrastructure", "RetentionErasureService.cs"),
        };

        sources.Should().OnlyContain(source =>
            !source.Contains("failedRecordIds", StringComparison.OrdinalIgnoreCase)
            && !source.Contains("HashSet<string>", StringComparison.Ordinal));
    }

    private static HashSet<string> DiscoverTests()
    {
        var tests = typeof(ComplianceManifestContractTests).Assembly.GetTypes()
            .Where(type => type.IsPublic && !type.IsAbstract && type.FullName is not null)
            .SelectMany(type => type.GetMethods(BindingFlags.Public | BindingFlags.Instance | BindingFlags.DeclaredOnly)
                .Where(method => method.GetCustomAttribute<FactAttribute>() is not null)
                .Select(method => (Identity: $"{ThisAssembly}|{type.FullName}|{method.Name}", Method: method)))
            .ToArray();
        tests.Select(test => test.Identity).Should().OnlyHaveUniqueItems("overloaded xUnit method names are not collision-proof manifest targets");
        tests.Should().OnlyContain(test => IsStructurallyExecutable(test.Method));
        return tests.Select(test => test.Identity).ToHashSet(StringComparer.Ordinal);
    }

    private static string ReadProductionSource(params string[] segments)
    {
        var directory = new DirectoryInfo(AppContext.BaseDirectory);
        while (directory is not null && !File.Exists(Path.Combine(directory.FullName, "Cohort.slnx")))
        {
            directory = directory.Parent;
        }

        directory.Should().NotBeNull("the test output must be beneath the repository root");
        return File.ReadAllText(Path.Combine([directory!.FullName, "Cohort", .. segments]));
    }

    private static bool IsStructurallyExecutable(MethodInfo method)
    {
        var fact = method.GetCustomAttribute<FactAttribute>()!;
        if (!string.IsNullOrWhiteSpace(fact.Skip))
        {
            return false;
        }
        if (method.GetCustomAttribute<TheoryAttribute>() is null)
        {
            return method.GetParameters().Length == 0;
        }
        var data = method.GetCustomAttributes().OfType<DataAttribute>().ToArray();
        return data.Length > 0
            && data.All(attribute => string.IsNullOrWhiteSpace(attribute.Skip));
    }

    private static string Identity(ComplianceEvidence evidence) => $"{evidence.Assembly}|{evidence.Type}|{evidence.Method}";

    private static bool HasStructurallyValidPrimaryEvidence(ComplianceRequirement requirement)
    {
        if (requirement.Status != "supported")
        {
            return requirement.Evidence.Count == 0;
        }

        if (requirement.Id.StartsWith("MIGRATION-", StringComparison.Ordinal))
        {
            return requirement.Evidence.Any(item => item.Primary == true && item.Kind == "migration-postgresql");
        }

        return requirement.Source.StartsWith("Cohort internal invariant:", StringComparison.Ordinal)
            ? requirement.Evidence.Any(item => item.Primary == true)
            : requirement.Evidence.Any(item => item.Primary == true && item.Kind != "internal-mechanism");
    }

    private sealed record ComplianceManifest(string ManifestVersion, string Product, string Claim, IReadOnlyList<ComplianceRequirement> Requirements);
    private sealed record ComplianceRequirement(string Id, string Source, string Requirement, IReadOnlyList<ComplianceEvidence> Evidence, string Status);
    private sealed record ComplianceEvidence(string Assembly, string Type, string Method, string Kind, bool? Primary);
}
