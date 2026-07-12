using System.Reflection;
using System.Text.Json;
using System.Text.Json.Serialization;

using Xunit.Sdk;

namespace Cohort.Sample.Tests.ComplianceCorpus;

public sealed class ComplianceCorpusManifestTests
{
    private const string ThisAssembly = "Cohort.Sample.Tests";
    private static readonly string[] AllowedAssemblies = [ThisAssembly, "Cohort.Tests"];
    private static readonly string[] AllowedStatuses = ["supported", "non-goal"];
    private static readonly string[] AllowedKinds =
    [
        "public-postgresql",
        "hosted-postgresql",
        "migration-postgresql",
        "package-consumer",
        "pure-contract",
        "architecture",
        "internal-mechanism",
    ];
    private static readonly JsonSerializerOptions JsonOptions = new()
    {
        PropertyNameCaseInsensitive = false,
        PropertyNamingPolicy = JsonNamingPolicy.CamelCase,
        UnmappedMemberHandling = JsonUnmappedMemberHandling.Disallow,
    };

    [Fact]
    public void Manifest_is_complete_and_bidirectional()
    {
        var manifest = LoadManifest();
        var discoveredTests = DiscoverTests();
        var evidence = manifest.Requirements.SelectMany(requirement => requirement.Evidence).ToArray();

        manifest.ManifestVersion.Should().MatchRegex(@"^2\.\d+\.\d+$");
        manifest.Requirements.Select(requirement => requirement.Id).Should().OnlyHaveUniqueItems();
        manifest.Requirements.Should().OnlyContain(requirement =>
            !string.IsNullOrWhiteSpace(requirement.Source)
            && !string.IsNullOrWhiteSpace(requirement.Requirement)
            && AllowedStatuses.Contains(requirement.Status, StringComparer.Ordinal)
        );
        manifest.Requirements.Where(requirement => requirement.Status == "supported")
            .Should().OnlyContain(requirement => requirement.Evidence.Any(item => item.Primary == true));
        manifest.Requirements.Where(requirement => requirement.Status != "supported")
            .Should().OnlyContain(requirement => requirement.Evidence.Count == 0);
        manifest.Requirements.Should().OnlyContain(requirement => HasStructurallyValidPrimaryEvidence(requirement));
        manifest.Requirements.Should().OnlyContain(requirement =>
            requirement.Evidence.Select(Identity).Distinct(StringComparer.Ordinal).Count()
            == requirement.Evidence.Count
        );
        evidence.Should().OnlyContain(item =>
            AllowedAssemblies.Contains(item.Assembly, StringComparer.Ordinal)
            && AllowedKinds.Contains(item.Kind, StringComparer.Ordinal)
            && item.Primary.HasValue
            && !string.IsNullOrWhiteSpace(item.Type)
            && !string.IsNullOrWhiteSpace(item.Method)
        );

        var localEvidence = evidence.Where(item => item.Assembly == ThisAssembly).ToArray();
        localEvidence.Should().OnlyContain(item => discoveredTests.ContainsKey(Identity(item)),
            "every Cohort.Sample.Tests evidence identity must resolve uniquely to a structurally executable xUnit target");

        var namedCorpusTests = localEvidence.Select(Identity).ToHashSet(StringComparer.Ordinal);
        discoveredTests.Where(test => test.Value.IsDedicatedCorpusTest)
            .Select(test => test.Key)
            .Should().BeSubsetOf(namedCorpusTests);
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
    public void Manifest_declares_scope_and_non_goals()
    {
        var manifest = LoadManifest();
        var requiredPrefixes = new[]
        {
            "CORPUS-", "SCOPE-", "STORAGE-", "STRATEGY-", "LEGALMIN-", "HOLD-",
            "ERASURE-", "ANON-", "ACCOUNT-", "ISOLATION-", "FAILURE-", "READY-",
            "CONCURRENCY-", "DOMAIN-", "RULE-", "SCHEMA-", "AUDIT-", "TENANT-", "DISPATCH-",
            "PACKAGE-", "MIGRATION-", "NONGOAL-",
        };

        requiredPrefixes.Should().OnlyContain(prefix =>
            manifest.Requirements.Any(requirement => requirement.Id.StartsWith(prefix, StringComparison.Ordinal))
        );
        manifest.Claim.Should().Contain("does not certify complete GDPR compliance");
        manifest.Requirements.Should().Contain(requirement => requirement.Status == "non-goal");
    }

    private static ComplianceManifest LoadManifest()
    {
        var path = Path.Combine(AppContext.BaseDirectory, "compliance-manifest.json");
        return JsonSerializer.Deserialize<ComplianceManifest>(File.ReadAllText(path), JsonOptions)
            ?? throw new InvalidOperationException("The compliance manifest deserialized to null.");
    }

    private static IReadOnlyDictionary<string, DiscoveredTest> DiscoverTests()
    {
        var corpusNamespace = typeof(ComplianceCorpusManifestTests).Namespace;
        var tests = typeof(ComplianceCorpusManifestTests).Assembly.GetTypes()
            .Where(type => type.IsPublic && !type.IsAbstract && type.FullName is not null)
            .SelectMany(type => type.GetMethods(BindingFlags.Public | BindingFlags.Instance | BindingFlags.DeclaredOnly)
                .Select(method => (Type: type, Method: method, Fact: method.GetCustomAttribute<FactAttribute>(), Theory: method.GetCustomAttribute<TheoryAttribute>()))
                .Where(test => test.Fact is not null)
                .Select(test => new DiscoveredTest(
                    $"{ThisAssembly}|{test.Type.FullName}|{test.Method.Name}",
                    GetNonExecutableReason(test.Method, test.Fact!, test.Theory),
                    test.Type.Namespace == corpusNamespace
                )))
            .ToArray();

        tests.Select(test => test.Identity).Should().OnlyHaveUniqueItems("overloaded xUnit method names are not collision-proof manifest targets");
        tests.Should().OnlyContain(test => test.NonExecutableReason == null, "{0}",
            string.Join(Environment.NewLine, tests.Where(test => test.NonExecutableReason is not null)
                .Select(test => $"{test.Identity}: {test.NonExecutableReason}")));
        return tests.ToDictionary(test => test.Identity, StringComparer.Ordinal);
    }

    private static string? GetNonExecutableReason(MethodInfo method, FactAttribute fact, TheoryAttribute? theory)
    {
        if (!string.IsNullOrWhiteSpace(fact.Skip))
        {
            return $"test is skipped: {fact.Skip}";
        }
        if (theory is null)
        {
            return method.GetParameters().Length == 0 ? null : "fact has parameters";
        }
        var data = method.GetCustomAttributes().OfType<DataAttribute>().ToArray();
        if (data.Length == 0 || data.Any(attribute => !string.IsNullOrWhiteSpace(attribute.Skip)))
        {
            return "theory has no unskipped data source";
        }
        return null;
    }

    private static string Identity(ComplianceEvidence evidence) =>
        $"{evidence.Assembly}|{evidence.Type}|{evidence.Method}";

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

    private sealed record DiscoveredTest(string Identity, string? NonExecutableReason, bool IsDedicatedCorpusTest);
    private sealed record ComplianceManifest(string ManifestVersion, string Product, string Claim, IReadOnlyList<ComplianceRequirement> Requirements);
    private sealed record ComplianceRequirement(string Id, string Source, string Requirement, IReadOnlyList<ComplianceEvidence> Evidence, string Status);
    private sealed record ComplianceEvidence(string Assembly, string Type, string Method, string Kind, bool? Primary);
}
