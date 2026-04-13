using System.Diagnostics;
using System.IO.Compression;
using System.Xml.Linq;

namespace Cohort.Tests;

public sealed class PackageReleaseContractTests
{
    private static readonly Lazy<PackedArtifact> Artifact = new(BuildPackedArtifact);
    private static readonly Lazy<string> Changelog = new(BuildChangelog);

    [Fact]
    public void Packed_Package_Uses_A_Version_Greater_Than_0_1_1()
    {
        Artifact.Value.PackageVersion.Should().BeGreaterThan(new Version(0, 1, 1));
    }

    [Fact]
    public void Packed_Readme_Documents_Explicit_Tenantless_Opt_Out_Without_A_Broken_Changelog_Link()
    {
        Artifact.Value.Readme.Should().Contain("tenant-scoped by default");
        Artifact.Value.Readme.Should().Contain("[RetentionTenantless]");
        Artifact.Value.Readme.Should().Contain("startup configuration error");
        Artifact.Value.Readme.Should().NotContain("[CHANGELOG.md](CHANGELOG.md)");
    }

    [Fact]
    public void Packed_Readme_Documents_Subject_And_Effective_Cutoff_Gating_For_Erasure()
    {
        Artifact.Value.Readme.Should().Contain("matches the requested `[ErasureSubject]`");
        Artifact.Value.Readme.Should().Contain("effective retention cutoff");
        Artifact.Value.Readme.Should().Contain("max(Period, LegalMin)");
    }

    [Fact]
    public void Packed_Readme_Points_To_Release_Notes_With_A_Package_Safe_Link()
    {
        var expectedReleaseNotesUrl = BuildVersionPinnedChangelogUrl(Artifact.Value.PackageVersion);

        Artifact.Value.Readme.Should().Contain("## Upgrade from `0.1.1`");
        Artifact.Value.Readme.Should().Contain("regenerate and apply your Cohort migration before booting `0.2.0`");
        Artifact.Value.Readme.Should().Contain("version-pinned changelog entry");
        Artifact.Value.Readme.Should().Contain(expectedReleaseNotesUrl);
        Artifact.Value.Readme.Should().NotContain("/blob/main/CHANGELOG.md");
        Artifact.Value.Readme.Should().Contain("AnonymiseWithAttribute");
        Artifact.Value.Readme.Should().Contain("PreviewEraseAsync(...)");
        Artifact.Value.Readme.Should().Contain("row-dispatch surface");
    }

    [Fact]
    public void Changelog_Release_Notes_Cover_The_Runtime_Upgrade_Contract()
    {
        Changelog.Value.Should().Contain("Startup tenant enforcement is now fail-closed");
        Changelog.Value.Should().Contain("effective retention cutoff");
        Changelog.Value.Should().Contain("must refresh their Cohort-owned table migrations before booting this package version");
        Changelog.Value.Should().Contain("RetentionRowDispatcher");
    }

    [Fact]
    public void Changelog_Release_Checklist_Covers_Pack_And_Clean_Consumer_Restore()
    {
        Changelog.Value.Should().Contain("dotnet pack Cohort/Cohort.csproj");
        Changelog.Value.Should().Contain("Restore the packed version into a clean consumer.");
        Changelog.Value.Should().Contain("AnonymiseWithAttribute");
        Changelog.Value.Should().Contain("IRetentionSweepStrategy.PreviewEraseAsync(...)");
        Changelog.Value.Should().Contain("IRetentionRowDispatcher");
        Changelog.Value.Should().Contain("Regenerate your host migration against the `0.2.0` package");
        Changelog.Value.Should().Contain("Apply that migration before booting the new package version");
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

    private static PackedArtifact BuildPackedArtifact()
    {
        var repoRoot = FindRepoRoot();
        var projectPath = Path.Combine(repoRoot, "Cohort", "Cohort.csproj");
        var outputDirectory = Path.Combine(Path.GetTempPath(), $"cohort-package-contract-{Guid.NewGuid():N}");
        Directory.CreateDirectory(outputDirectory);

        try
        {
            using var process = new Process
            {
                StartInfo = new ProcessStartInfo("dotnet")
                {
                    RedirectStandardOutput = true,
                    RedirectStandardError = true,
                    WorkingDirectory = repoRoot,
                },
            };
            process.StartInfo.ArgumentList.Add("pack");
            process.StartInfo.ArgumentList.Add(projectPath);
            process.StartInfo.ArgumentList.Add("--configuration");
            process.StartInfo.ArgumentList.Add("Release");
            process.StartInfo.ArgumentList.Add("-o");
            process.StartInfo.ArgumentList.Add(outputDirectory);

            process.Start();
            var standardOutput = process.StandardOutput.ReadToEnd();
            var standardError = process.StandardError.ReadToEnd();
            process.WaitForExit();

            process.ExitCode.Should().Be(
                0,
                $"dotnet pack must succeed for the shipped package surface.{Environment.NewLine}{standardOutput}{Environment.NewLine}{standardError}"
            );

            var packagePath = Directory
                .EnumerateFiles(outputDirectory, "*.nupkg", SearchOption.TopDirectoryOnly)
                .Single(path => !path.EndsWith(".snupkg", StringComparison.OrdinalIgnoreCase));

            using var archive = ZipFile.OpenRead(packagePath);
            var readmeEntry = archive.GetEntry("README.md");
            readmeEntry.Should().NotBeNull("the shipped package must include the packed README surface");

            var nuspecEntry = archive.Entries.Single(entry => entry.FullName.EndsWith(".nuspec", StringComparison.OrdinalIgnoreCase));

            string readme;
            using (var reader = new StreamReader(readmeEntry!.Open()))
            {
                readme = reader.ReadToEnd();
            }

            string nuspec;
            using (var reader = new StreamReader(nuspecEntry.Open()))
            {
                nuspec = reader.ReadToEnd();
            }

            var version = Version.Parse(
                XDocument.Parse(nuspec)
                    .Root!
                    .Descendants()
                    .Single(element => element.Name.LocalName == "version")
                    .Value
            );

            return new PackedArtifact(version, readme);
        }
        finally
        {
            if (Directory.Exists(outputDirectory))
            {
                Directory.Delete(outputDirectory, recursive: true);
            }
        }
    }

    private static string BuildChangelog()
    {
        var repoRoot = FindRepoRoot();
        var changelogPath = Path.Combine(repoRoot, "CHANGELOG.md");

        File.Exists(changelogPath).Should().BeTrue("slice 8 ships the release handover artifact in-repo");

        return File.ReadAllText(changelogPath);
    }

    private static string BuildVersionPinnedChangelogUrl(Version packageVersion)
    {
        return $"https://github.com/maxanstey-meridian/cohort/blob/v{packageVersion}/CHANGELOG.md#020";
    }

    private sealed record PackedArtifact(Version PackageVersion, string Readme);
}
