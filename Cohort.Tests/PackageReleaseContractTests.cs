using System.Diagnostics;
using System.IO.Compression;
using System.Xml.Linq;
using Cohort.Application;
using Cohort.Domain;

namespace Cohort.Tests;

public sealed class PackageReleaseContractTests
{
    private static readonly TimeSpan ProcessTimeout = TimeSpan.FromMinutes(2);
    private static readonly Lazy<PackedArtifact> Artifact = new(BuildPackedArtifact);
    private static readonly string GlobalPackagesFolder = GetGlobalPackagesFolder();

    [Fact]
    public void Packed_Package_Uses_A_Version_Greater_Than_The_Latest_Release()
    {
        Artifact.Value.PackageVersion.Should().BeGreaterThan(new Version(0, 5, 0));
    }

    [Fact]
    public void Publish_Workflow_Requires_The_Tag_To_Exactly_Match_The_Project_Version()
    {
        var gateScript = ExtractWorkflowStepScript("Validate tag matches package version");

        gateScript
            .Should()
            .Be(
                """
                tag_version="${GITHUB_REF_NAME#v}"
                project_version="$(dotnet msbuild Cohort/Cohort.csproj -getProperty:Version -nologo)"

                if [[ "$tag_version" != "$project_version" ]]; then
                  echo "Tag version '$tag_version' does not match Cohort.csproj Version '$project_version'." >&2
                  exit 1
                fi
                """
            );
        ExtractWorkflowStepScript("Pack").Should().NotContain("-p:Version=");
    }

    [Fact]
    public void Publish_Workflow_Fails_When_The_Package_Version_Already_Exists()
    {
        ExtractWorkflowStepScript("Push to NuGet")
            .Should()
            .Be(
                "dotnet nuget push ./nupkgs/*.nupkg --source https://api.nuget.org/v3/index.json --api-key ${{ secrets.NUGET_API_KEY }}"
            );
    }

    [Fact]
    public void Infrastructure_Exports_Only_The_Explicit_Package_API()
    {
        var exportedInfrastructureTypeNames = typeof(IRetentionSweep)
            .Assembly.GetExportedTypes()
            .Where(type =>
                type.Namespace == "Cohort.Infrastructure"
                || type.Namespace?.StartsWith("Cohort.Infrastructure.", StringComparison.Ordinal)
                    == true
            )
            .Select(type => type.FullName)
            .Order(StringComparer.Ordinal)
            .ToArray();

        exportedInfrastructureTypeNames
            .Should()
            .Equal("Cohort.Infrastructure.Migrations.CohortModelBuilder");
    }

    [Fact]
    public void Readme_Explains_The_Product_Surface()
    {
        Artifact.Value.Readme.Should().Contain("Annotation-driven data retention");
        Artifact.Value.Readme.Should().Contain("Postgres-only.");
        Artifact
            .Value.Readme.Should()
            .Contain("Annotations declare membership. Category rules declare policy.");
        Artifact.Value.Readme.Should().Contain("ConfigureCohortTables()");
        Artifact.Value.Readme.Should().Contain("AddCohort<MyDbContext>()");
        Artifact.Value.Readme.Should().Contain("IRetentionPreview");
        Artifact.Value.Readme.Should().Contain("IRetentionSweep");
        Artifact.Value.Readme.Should().Contain("IRetentionErasureService");
        Artifact.Value.Readme.Should().Contain("multiple `[ErasureSubject]` properties");
        Artifact.Value.Readme.Should().Contain("sweep_run_entity_summary");
        Artifact.Value.Readme.Should().Contain("durable correlation metadata");
        Artifact
            .Value.Readme.Should()
            .Contain(
                "`sweep_run_entity_summary` rows are uniquely identified by the durable retention entity ID"
            );
        Artifact.Value.Readme.Should().Contain("RuleSource");
        Artifact.Value.Readme.Should().Contain("RuleReason");
        Artifact.Value.Readme.Should().Contain("RetentionRowDispatcher");
        Artifact.Value.Readme.Should().Contain("At this point the entity annotation is wired.");
        Artifact.Value.Readme.Should().Contain("sweep_run.DryRun");
        Artifact.Value.Readme.Should().Contain("Status = Started");
        Artifact.Value.Readme.Should().Contain("SettledAt = NULL");
        Artifact.Value.Readme.Should().Contain("0.6 intentionally narrows");
        Artifact.Value.Readme.Should().NotContain("run row's `FailedAt`");
        Artifact.Value.Readme.Should().NotContain("both `CompletedAt`");
    }

    [Fact]
    public void Readme_Covers_The_Useful_Greenfield_Paths()
    {
        Artifact.Value.Readme.Should().Contain("old `SessionNote` rows are deleted");
        Artifact.Value.Readme.Should().Contain("marked fields are scrubbed");
        Artifact.Value.Readme.Should().Contain("AnonymiseWithAttribute");
        Artifact.Value.Readme.Should().Contain("IRetentionRowDispatcher");
        Artifact.Value.Readme.Should().Contain("RetentionHoldRequest");
        Artifact
            .Value.Readme.Should()
            .Contain("[RetentionEntityId(\"a3f467fe-c5d0-4f17-9897-83c373cc1dc8\")]");
        Artifact
            .Value.Readme.Should()
            .Contain("[RetentionEntityId(\"b7316df4-7db5-46ad-aea7-f65c4b430f73\")]");
        Artifact
            .Value.Readme.Should()
            .Contain("[RetentionEntityId(\"6b619c19-6e3c-44e8-a87f-975c68fd3988\")]");
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
            var packageFileName = $"Cohort.{Artifact.Value.PackedVersion}.nupkg";
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
                    <PackageReference Include="Cohort" Version="{{Artifact.Value.PackedVersion}}" />
                  </ItemGroup>
                </Project>
                """
            );

            File.WriteAllText(
                Path.Combine(consumerDirectory, "Program.cs"),
                """
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

                Console.WriteLine(typeof(AnonymiseWithAttribute).FullName);
                Console.WriteLine(typeof(RetentionEntityIdAttribute).FullName);
                Console.WriteLine(dispatcher.GetType().FullName);

                return 0;

                public sealed class ConsumerDbContext(DbContextOptions<ConsumerDbContext> options)
                    : DbContext(options);

                public sealed class ConsumerFactory : IAnonymiseValueFactory
                {
                    public object? Create(AnonymiseValueContext context) => "value";
                }

                public sealed class ConsumerAuditWriter : IRetentionAuditWriter
                {
                    public Task WriteAsync(SweepEvent evt, CancellationToken ct) =>
                        Task.CompletedTask;
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

        throw new InvalidOperationException(
            "Could not locate the repository root from the test output directory."
        );
    }

    private static string ExtractWorkflowStepScript(string stepName)
    {
        var lines = File.ReadAllLines(
            Path.Combine(FindRepoRoot(), ".github", "workflows", "publish.yml")
        );
        var nameLine = Array.FindIndex(lines, line => line.Trim() == $"- name: {stepName}");
        nameLine
            .Should()
            .BeGreaterThanOrEqualTo(0, $"the workflow must contain the '{stepName}' step");

        var stepIndent = lines[nameLine].Length - lines[nameLine].TrimStart().Length;
        var stepEnd = Array.FindIndex(
            lines,
            nameLine + 1,
            line =>
                !string.IsNullOrWhiteSpace(line)
                && line.Length - line.TrimStart().Length == stepIndent
                && line.TrimStart().StartsWith("- ", StringComparison.Ordinal)
        );
        if (stepEnd < 0)
        {
            stepEnd = lines.Length;
        }

        var runLine = Array.FindIndex(
            lines,
            nameLine + 1,
            stepEnd - nameLine - 1,
            line =>
                line.Length - line.TrimStart().Length > stepIndent
                && line.TrimStart().StartsWith("run:", StringComparison.Ordinal)
        );
        runLine.Should().BeGreaterThan(nameLine, $"the '{stepName}' step must have a run script");

        var runValue = lines[runLine].TrimStart()["run:".Length..].TrimStart();
        if (runValue != "|")
        {
            return runValue;
        }

        var scriptLines = lines
            .Skip(runLine + 1)
            .TakeWhile(line =>
                string.IsNullOrWhiteSpace(line)
                || line.Length - line.TrimStart().Length > stepIndent
            )
            .ToArray();
        var contentIndent = scriptLines
            .Where(line => !string.IsNullOrWhiteSpace(line))
            .Min(line => line.Length - line.TrimStart().Length);

        return string.Join(
                Environment.NewLine,
                scriptLines.Select(line =>
                    string.IsNullOrWhiteSpace(line) ? string.Empty : line[contentIndent..]
                )
            )
            .TrimEnd();
    }

    private static PackedArtifact BuildPackedArtifact()
    {
        var repoRoot = FindRepoRoot();
        var projectPath = Path.Combine(repoRoot, "Cohort", "Cohort.csproj");
        var outputDirectory = Path.Combine(
            Path.GetTempPath(),
            $"cohort-package-contract-{Guid.NewGuid():N}"
        );
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
            // Packed under a prerelease suffix so the consumer restore can never be
            // shadowed by an already-published package of the same version sitting in
            // the global packages folder (used as a restore fallback for offline deps).
            process.StartInfo.ArgumentList.Add(
                $"/p:Version={ReadCsprojVersion(projectPath)}-releasegate"
            );

            var result = RunProcess(process, "dotnet pack for the shipped package surface");
            result.ExitCode.Should().Be(0, result.AssertionOutput);

            var packagePath = Directory
                .EnumerateFiles(outputDirectory, "*.nupkg", SearchOption.TopDirectoryOnly)
                .Single(path => !path.EndsWith(".snupkg", StringComparison.OrdinalIgnoreCase));

            using var archive = ZipFile.OpenRead(packagePath);
            var readmeEntry = archive.GetEntry("README.md");
            readmeEntry
                .Should()
                .NotBeNull("the shipped package must include the packed README surface");

            var nuspecEntry = archive.Entries.Single(entry =>
                entry.FullName.EndsWith(".nuspec", StringComparison.OrdinalIgnoreCase)
            );

            string readme;
            using (var reader = new StreamReader(readmeEntry!.Open()))
            {
                readme = reader.ReadToEnd();
            }
            readme.Should().Contain("Annotation-driven data retention");
            readme.Should().Contain("multiple `[ErasureSubject]` properties");
            readme.Should().Contain("Any marked subject column equals the requested subject");
            readme.Should().Contain("RetentionRowDispatcher");

            string nuspec;
            using (var reader = new StreamReader(nuspecEntry.Open()))
            {
                nuspec = reader.ReadToEnd();
            }

            var packageBytes = File.ReadAllBytes(packagePath);
            var packedVersion = XDocument
                .Parse(nuspec)
                .Root!.Descendants()
                .Single(element => element.Name.LocalName == "version")
                .Value;
            var version = Version.Parse(packedVersion.Split('-')[0]);

            return new PackedArtifact(version, packedVersion, readme, packageBytes);
        }
        finally
        {
            if (Directory.Exists(outputDirectory))
            {
                Directory.Delete(outputDirectory, recursive: true);
            }
        }
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

        var result = RunProcess(process, $"dotnet {string.Join(" ", arguments)} for {purpose}");
        result.ExitCode.Should().Be(0, result.AssertionOutput);

        return string.Concat(result.StandardOutput, result.StandardError);
    }

    private static ProcessResult RunProcess(Process process, string description)
    {
        return RunProcessAsync(process, description).GetAwaiter().GetResult();
    }

    private static async Task<ProcessResult> RunProcessAsync(Process process, string description)
    {
        process.Start();
        var standardOutputTask = process.StandardOutput.ReadToEndAsync();
        var standardErrorTask = process.StandardError.ReadToEndAsync();

        using var timeout = new CancellationTokenSource(ProcessTimeout);
        try
        {
            await process.WaitForExitAsync(timeout.Token);
        }
        catch (OperationCanceledException) when (timeout.IsCancellationRequested)
        {
            if (!process.HasExited)
            {
                process.Kill(entireProcessTree: true);
            }

            await process.WaitForExitAsync();
            var timedOutOutput = await standardOutputTask;
            var timedOutError = await standardErrorTask;
            throw new Xunit.Sdk.XunitException(
                FormatProcessOutput(
                    $"{description} timed out after {ProcessTimeout} and was killed.",
                    timedOutOutput,
                    timedOutError
                )
            );
        }

        return new ProcessResult(
            process.ExitCode,
            await standardOutputTask,
            await standardErrorTask,
            description
        );
    }

    private static string FormatProcessOutput(
        string summary,
        string standardOutput,
        string standardError
    )
    {
        return $"{summary}{Environment.NewLine}--- stdout ---{Environment.NewLine}{standardOutput}{Environment.NewLine}--- stderr ---{Environment.NewLine}{standardError}";
    }

    private static string GetGlobalPackagesFolder()
    {
        var profile = Environment.GetFolderPath(Environment.SpecialFolder.UserProfile);
        return Path.Combine(profile, ".nuget", "packages");
    }

    private static string ReadCsprojVersion(string projectPath)
    {
        return XDocument.Load(projectPath).Root!.Descendants("Version").Single().Value;
    }

    private sealed record PackedArtifact(
        Version PackageVersion,
        string PackedVersion,
        string Readme,
        byte[] PackageBytes
    );

    private sealed record ProcessResult(
        int ExitCode,
        string StandardOutput,
        string StandardError,
        string Description
    )
    {
        public string AssertionOutput =>
            FormatProcessOutput(
                $"{Description} exited with code {ExitCode}.",
                StandardOutput,
                StandardError
            );
    }
}
