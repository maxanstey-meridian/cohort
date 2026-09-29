using System.Security.Cryptography;

namespace Cohort.Tests;

// Shipped EF migrations are immutable: consumers have already applied them. Any edit to a
// historical migration file must be a new migration instead.
public sealed class HistoricalMigrationBaselineTests
{
    [Fact]
    public void Historical_Migration_Filenames_And_SHA256_Hashes_Match_The_Approved_Baseline()
    {
        var migrationsDirectory = Path.Combine(FindRepoRoot(), "Cohort.Sample", "Migrations");
        var actual = Directory
            .EnumerateFiles(migrationsDirectory, "*.cs", SearchOption.TopDirectoryOnly)
            .Where(path => Path.GetFileName(path) != "SampleDbContextModelSnapshot.cs")
            .Select(path =>
                $"{Path.GetFileName(path)} {Convert.ToHexString(SHA256.HashData(File.ReadAllBytes(path))).ToLowerInvariant()}"
            )
            .Order(StringComparer.Ordinal)
            .ToArray();
        var approved = File.ReadAllLines(
            Path.Combine(AppContext.BaseDirectory, "HistoricalMigrations.approved.txt")
        );

        actual.Should().Equal(approved);
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
