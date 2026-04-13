namespace Cohort.Tests;

public sealed class PackageReleaseContractTests
{
    [Fact]
    public void Release_Docs_Preserve_The_Upgrade_And_Verification_Contract()
    {
        var repoRoot = FindRepoRoot();
        var changelogPath = Path.Combine(repoRoot, "CHANGELOG.md");
        var readmePath = Path.Combine(repoRoot, "README.md");

        File.Exists(changelogPath).Should().BeTrue("the release handover artifact must ship in-repo");

        var changelog = File.ReadAllText(changelogPath);
        var readme = File.ReadAllText(readmePath);

        changelog.Should().Contain("## 0.2.0");
        changelog.Should().Contain("[RetentionTenantless]");
        changelog.Should().Contain("effective retention cutoff");
        changelog.Should().Contain("max(Period, LegalMin)");
        changelog.Should().Contain("refresh their Cohort-owned table migrations");
        changelog.Should().Contain("RetentionRowDispatcher");
        changelog.Should().Contain("dotnet pack Cohort/Cohort.csproj");
        changelog.Should().Contain("AnonymiseWithAttribute");
        changelog.Should().Contain("PreviewEraseAsync");
        changelog.Should().Contain("IRetentionRowDispatcher");
        changelog.Should().Contain("Apply that migration before booting the new package version");

        readme.Should().Contain("## Upgrade from `0.1.1`");
        readme.Should().Contain("[CHANGELOG.md](CHANGELOG.md)");
        readme.Should().Contain("regenerate and apply your Cohort table migration before booting the new package version");
        readme.Should().Contain("AnonymiseWithAttribute");
        readme.Should().Contain("PreviewEraseAsync(...)");
        readme.Should().Contain("row-dispatch surface");
    }

    private static string FindRepoRoot()
    {
        var directory = new DirectoryInfo(AppContext.BaseDirectory);

        while (directory is not null)
        {
            var candidate = Path.Combine(directory.FullName, "Cohort.slnx");
            if (File.Exists(candidate))
            {
                return directory.FullName;
            }

            directory = directory.Parent;
        }

        throw new InvalidOperationException("Could not locate the repository root from the test output directory.");
    }
}
