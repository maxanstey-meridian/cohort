using System.Xml.Linq;

using ArchUnitNET.Domain;
using ArchUnitNET.Loader;
using ArchUnitNET.xUnit;

using Microsoft.Extensions.Hosting;

using static ArchUnitNET.Fluent.ArchRuleDefinition;

namespace Cohort.Tests;

// Layer rules from CLAUDE.md, enforced over the compiled Cohort assembly (type signatures and
// method bodies). Folder = namespace = layer.
public sealed class ArchitectureBoundaryTests
{
    private const string Domain = @"^Cohort\.Domain(\..+)?$";
    private const string Application = @"^Cohort\.Application(\..+)?$";
    private const string Infrastructure = @"^Cohort\.Infrastructure(\..+)?$";
    private const string Hosting = @"^Cohort\.Hosting(\..+)?$";
    private const string Bcl = @"^System(\..+)?$";

    // Compiler-emitted attributes (EmbeddedAttribute, NullableAttribute, ...) and generated regex
    // helpers live in the Cohort assembly but belong to no layer.
    private const string CompilerGenerated =
        @"^(Microsoft\.CodeAnalysis|System\.Runtime\.CompilerServices|System\.Text\.RegularExpressions\.Generated)$";

    private static readonly Architecture Architecture = new ArchLoader()
        .LoadAssemblies(
            typeof(Cohort.Domain.RetentionRule).Assembly,
            // Loaded so AreAssignableTo(BackgroundService) can resolve the framework base type.
            typeof(BackgroundService).Assembly
        )
        .Build();

    [Fact]
    public void Production_Types_Belong_To_A_Layer_Namespace()
    {
        Types()
            .That()
            .ResideInAssembly(typeof(Cohort.Domain.RetentionRule).Assembly)
            .And()
            .DoNotResideInNamespaceMatching(CompilerGenerated)
            .And()
            .DoNotHaveNameStartingWith("<")
            .Should()
            .ResideInNamespaceMatching(@"^Cohort\.(Domain|Application|Infrastructure|Hosting)(\..+)?$")
            .Check(Architecture);
    }

    [Fact]
    public void Domain_Depends_Only_On_Domain_And_The_Bcl()
    {
        Types()
            .That()
            .ResideInNamespaceMatching(Domain)
            .Should()
            .NotDependOnAnyTypesThat()
            .DoNotResideInNamespaceMatching($"{Domain}|{Bcl}|{CompilerGenerated}")
            .Check(Architecture);
    }

    [Fact]
    public void Application_Depends_Only_On_Domain_The_Bcl_And_DbContext()
    {
        Types()
            .That()
            .ResideInNamespaceMatching(Application)
            .Should()
            .NotDependOnAnyTypesThat()
            .DoNotHaveFullNameMatching(
                @"^(Cohort\.Application|Cohort\.Domain|System|Microsoft\.CodeAnalysis)\."
                    + @"|^Microsoft\.EntityFrameworkCore\.DbContext$"
            )
            .Check(Architecture);
    }

    [Fact]
    public void Infrastructure_Does_Not_Depend_On_Hosting()
    {
        Types()
            .That()
            .ResideInNamespaceMatching(Infrastructure)
            .Should()
            .NotDependOnAnyTypesThat()
            .ResideInNamespaceMatching(Hosting)
            .Check(Architecture);
    }

    [Fact(Skip = "Phase 3: RetentionRowDispatcher moves to Hosting (D1)")]
    public void Background_Services_Live_In_Hosting()
    {
        Classes()
            .That()
            .AreAssignableTo(typeof(BackgroundService))
            .Should()
            .NotResideInNamespaceMatching(Infrastructure)
            .Check(Architecture);
    }

    [Theory]
    [InlineData("Cohort.Tests/Cohort.Tests.csproj")]
    [InlineData("Cohort.Sample.Tests/Cohort.Sample.Tests.csproj")]
    [InlineData("Directory.Packages.props")]
    public void Test_Projects_Do_Not_Reference_Mocking_Libraries(string projectFile)
    {
        string[] banned = ["NSubstitute", "Moq", "FakeItEasy"];
        var packages = XDocument
            .Load(Path.Combine(FindRepoRoot(), projectFile))
            .Descendants()
            .Where(element => element.Name.LocalName is "PackageReference" or "PackageVersion")
            .Select(element => (string?)element.Attribute("Include") ?? "");

        packages
            .Should()
            .NotContain(
                package => banned.Any(name => package.StartsWith(name, StringComparison.OrdinalIgnoreCase)),
                "the mock ban is structural; write an end-to-end test instead (see CLAUDE.md)"
            );
    }

    private static string FindRepoRoot()
    {
        var directory = new DirectoryInfo(AppContext.BaseDirectory);
        while (directory is not null)
        {
            if (File.Exists(Path.Combine(directory.FullName, "Cohort.slnx")))
            {
                return directory.FullName;
            }

            directory = directory.Parent;
        }

        throw new InvalidOperationException("Could not locate the repository root.");
    }
}
