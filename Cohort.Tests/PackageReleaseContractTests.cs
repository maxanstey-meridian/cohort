using System.Diagnostics;
using System.IO.Compression;
using System.Xml.Linq;

using Cohort.Application;
using Cohort.Domain;

namespace Cohort.Tests;

public sealed class PackageReleaseContractTests
{
    private static readonly Lazy<PackedArtifact> Artifact = new(BuildPackedArtifact);
    private static readonly Lazy<string> Changelog = new(BuildChangelog);
    private static readonly string GlobalPackagesFolder = GetGlobalPackagesFolder();

    [Fact]
    public void Packed_Package_Uses_A_Version_Greater_Than_0_1_1()
    {
        Artifact.Value.PackageVersion.Should().BeGreaterThan(new Version(0, 1, 1));
    }

    [Fact]
    public void Changelog_Release_Notes_Cover_The_Current_Runtime_Surface()
    {
        Changelog.Value.Should().Contain("Startup tenant enforcement is now fail-closed");
        Changelog.Value.Should().Contain("effective retention cutoff");
        Changelog.Value.Should().Contain("multiple `[ErasureSubject]` properties");
        Changelog.Value.Should().Contain("RetentionRowDispatcher");
        Changelog.Value.Should().Contain("sweep_run_entity_summary");
        Changelog.Value.Should().Contain("RuleSource");
        Changelog.Value.Should().Contain("RuleReason");
        Changelog.Value.Should().Contain("Upgrade notes");
        Changelog.Value.Should().Contain("refresh or regenerate and apply the Cohort migration");
        Changelog.Value.Should().Contain("ApplyMigrations");
    }

    [Fact]
    public void Changelog_Release_Checklist_Covers_Pack_And_Clean_Consumer_Restore()
    {
        Changelog.Value.Should().Contain("dotnet pack Cohort/Cohort.csproj");
        Changelog.Value.Should().Contain("Restore the packed version into a clean consumer.");
        Changelog.Value.Should().Contain("AnonymiseWithAttribute");
        Changelog.Value.Should().Contain("IRetentionSweepStrategy.PreviewEraseAsync(...)");
        Changelog.Value.Should().Contain("ErasureSubjectPredicate");
        Changelog.Value.Should().Contain("IRetentionRowDispatcher");
        Changelog.Value.Should().Contain(
            "Refresh or regenerate your host migration against the `0.3.0` package"
        );
        Changelog.Value.Should().Contain("confirm it adds `RuleSource` and `RuleReason`");
        Changelog.Value.Should().Contain("`sweep_run_entity_summary`");
        Changelog.Value.Should().Contain(
            "apply that migration before booting the new package version"
        );
    }

    [Fact]
    public void Packed_Package_Can_Be_Restored_Into_A_Clean_Consumer_And_Expose_Release_Gate_Symbols()
    {
        var workspace = Path.Combine(
            Path.GetTempPath(),
            $"cohort-clean-consumer-{Guid.NewGuid():N}"
        );
        var consumerDirectory = Path.Combine(workspace, "consumer");
        var packageSourceDirectory = Path.Combine(workspace, "feed");
        var packageCacheDirectory = Path.Combine(workspace, "packages");
        Directory.CreateDirectory(consumerDirectory);
        Directory.CreateDirectory(packageSourceDirectory);
        Directory.CreateDirectory(packageCacheDirectory);

        try
        {
            var packageFileName = $"Cohort.{Artifact.Value.PackageVersion}.nupkg";
            File.WriteAllBytes(
                Path.Combine(packageSourceDirectory, packageFileName),
                Artifact.Value.PackageBytes
            );

            File.WriteAllText(
                Path.Combine(workspace, "NuGet.Config"),
                $$"""
                <?xml version="1.0" encoding="utf-8"?>
                <configuration>
                  <packageSources>
                    <clear />
                    <add key="local" value="{{packageSourceDirectory}}" />
                    <add key="nuget.org" value="https://api.nuget.org/v3/index.json" />
                  </packageSources>
                </configuration>
                """
            );

            File.WriteAllText(
                Path.Combine(consumerDirectory, "Consumer.csproj"),
                $$"""
                <Project Sdk="Microsoft.NET.Sdk">
                  <PropertyGroup>
                    <OutputType>Exe</OutputType>
                    <TargetFramework>net10.0</TargetFramework>
                    <ImplicitUsings>enable</ImplicitUsings>
                    <Nullable>enable</Nullable>
                  </PropertyGroup>
                  <ItemGroup>
                    <PackageReference Include="Cohort" Version="{{Artifact.Value.PackageVersion}}" />
                  </ItemGroup>
                </Project>
                """
            );

            File.WriteAllText(
                Path.Combine(consumerDirectory, "Program.cs"),
                """
                using System.Data.Common;
                using Cohort.Application;
                using Cohort.Domain;
                using Cohort.Hosting;
                using Microsoft.EntityFrameworkCore;
                using Microsoft.Extensions.Configuration;
                using Microsoft.Extensions.DependencyInjection;

                var services = new ServiceCollection();
                services.AddLogging();
                services.AddSingleton<IConfiguration>(
                    new ConfigurationBuilder().AddInMemoryCollection().Build()
                );
                services.AddDbContext<ConsumerDbContext>(options => { });
                services.AddCohort<ConsumerDbContext>();

                using var provider = services.BuildServiceProvider();
                var dispatcher = provider.GetRequiredService<IRetentionRowDispatcher>();

                _ = PreviewEraseSignature(
                    (strategy, entry, rule, predicate, tenant, now, conn, ct) =>
                        strategy.PreviewEraseAsync(entry, rule, predicate, tenant, now, conn, ct)
                );

                Console.WriteLine(typeof(AnonymiseWithAttribute).FullName);
                Console.WriteLine(typeof(ErasureSubjectPredicate).FullName);
                Console.WriteLine(typeof(IRetentionSweepStrategy).FullName);
                Console.WriteLine(dispatcher.GetType().FullName);

                return 0;

                static object PreviewEraseSignature(
                    Func<
                        IRetentionSweepStrategy,
                        RetentionEntry,
                        RetentionRule,
                        ErasureSubjectPredicate,
                        TenantContext,
                        DateTimeOffset,
                        DbConnection,
                        CancellationToken,
                        Task<int>
                    > method
                ) => method;

                public sealed class ConsumerDbContext(DbContextOptions<ConsumerDbContext> options)
                    : DbContext(options);

                public sealed class ConsumerFactory : IAnonymiseValueFactory
                {
                    public object? Create(AnonymiseValueContext context) => "value";
                }

                public sealed class ConsumerRecord
                {
                    public Guid Id { get; set; }

                    [AnonymiseWith(typeof(ConsumerFactory))]
                    public string? ExternalId { get; set; }
                }
                """
            );

            var nugetConfigPath = Path.Combine(workspace, "NuGet.Config");
            var restoreArgs = new[]
            {
                "restore",
                Path.Combine(consumerDirectory, "Consumer.csproj"),
                "--configfile",
                nugetConfigPath,
                "/p:RestoreFallbackFolders=" + GlobalPackagesFolder,
                "/p:RestoreIgnoreFailedSources=true",
            };
            RunDotnet(
                restoreArgs,
                consumerDirectory,
                packageCacheDirectory,
                "clean consumer restore from the packed Cohort package"
            );

            RunDotnet(
                new[]
                {
                    "build",
                    Path.Combine(consumerDirectory, "Consumer.csproj"),
                    "--no-restore",
                    "/p:RestoreFallbackFolders=" + GlobalPackagesFolder,
                },
                consumerDirectory,
                packageCacheDirectory,
                "clean consumer build against the packed Cohort package"
            );

            var run = RunDotnet(
                new[]
                {
                    "run",
                    "--project",
                    Path.Combine(consumerDirectory, "Consumer.csproj"),
                    "--no-build",
                },
                consumerDirectory,
                packageCacheDirectory,
                "clean consumer run against the packed Cohort package"
            );

            run.Should().Contain(typeof(AnonymiseWithAttribute).FullName);
            run.Should().Contain(typeof(ErasureSubjectPredicate).FullName);
            run.Should().Contain(typeof(IRetentionSweepStrategy).FullName);
            run.Should().Contain("RetentionRowDispatcher");
        }
        finally
        {
            if (Directory.Exists(workspace))
            {
                Directory.Delete(workspace, recursive: true);
            }
        }
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
            readme.Should().Contain("multiple `[ErasureSubject]` properties");
            readme.Should().Contain("Any marked subject column equals the requested subject");
            readme.Should().Contain("Existing hosts must regenerate and apply the Cohort migration");
            readme.Should().Contain("ApplyMigrations");

            string nuspec;
            using (var reader = new StreamReader(nuspecEntry.Open()))
            {
                nuspec = reader.ReadToEnd();
            }

            var packageBytes = File.ReadAllBytes(packagePath);
            var version = Version.Parse(
                XDocument.Parse(nuspec)
                    .Root!
                    .Descendants()
                    .Single(element => element.Name.LocalName == "version")
                    .Value
            );

            return new PackedArtifact(version, readme, packageBytes);
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

    private static string RunDotnet(
        IReadOnlyList<string> arguments,
        string workingDirectory,
        string packageCacheDirectory,
        string purpose
    )
    {
        using var process = new Process
        {
            StartInfo = new ProcessStartInfo("dotnet")
            {
                RedirectStandardOutput = true,
                RedirectStandardError = true,
                WorkingDirectory = workingDirectory,
            },
        };
        foreach (var argument in arguments)
        {
            process.StartInfo.ArgumentList.Add(argument);
        }

        process.StartInfo.Environment["NUGET_PACKAGES"] = packageCacheDirectory;

        process.Start();
        var standardOutput = process.StandardOutput.ReadToEnd();
        var standardError = process.StandardError.ReadToEnd();
        process.WaitForExit();

        process.ExitCode.Should().Be(
            0,
            $"dotnet {string.Join(" ", arguments)} must succeed for {purpose}.{Environment.NewLine}{standardOutput}{Environment.NewLine}{standardError}"
        );

        return string.Concat(standardOutput, standardError);
    }

    private static string GetGlobalPackagesFolder()
    {
        var profile = Environment.GetFolderPath(Environment.SpecialFolder.UserProfile);
        return Path.Combine(profile, ".nuget", "packages");
    }

    private sealed record PackedArtifact(Version PackageVersion, string Readme, byte[] PackageBytes);
}
