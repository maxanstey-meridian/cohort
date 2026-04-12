using System.Text.Json;

using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Design;

namespace Cohort.Sample;

public sealed class SampleDbContextFactory : IDesignTimeDbContextFactory<SampleDbContext>
{
    public SampleDbContext CreateDbContext(string[] args)
    {
        var appSettingsPath = ResolveAppSettingsPath();
        using var document = JsonDocument.Parse(File.ReadAllText(appSettingsPath));

        var connectionString = document
            .RootElement.GetProperty(SampleOptions.SectionName)
            .GetProperty(nameof(SampleOptions.ConnectionString))
            .GetString();

        if (string.IsNullOrWhiteSpace(connectionString))
        {
            throw new InvalidOperationException(
                $"'{SampleOptions.SectionName}:{nameof(SampleOptions.ConnectionString)}' was not found in {appSettingsPath}."
            );
        }

        var options = new DbContextOptionsBuilder<SampleDbContext>()
            .UseNpgsql(connectionString)
            .Options;

        return new SampleDbContext(options);
    }

    private static string ResolveAppSettingsPath()
    {
        var rootRelativePath = Path.Combine(Directory.GetCurrentDirectory(), "Cohort.Sample", "appsettings.json");
        if (File.Exists(rootRelativePath))
        {
            return rootRelativePath;
        }

        var localPath = Path.Combine(Directory.GetCurrentDirectory(), "appsettings.json");
        if (File.Exists(localPath))
        {
            return localPath;
        }

        throw new FileNotFoundException("Could not locate Cohort.Sample/appsettings.json for design-time DbContext creation.");
    }
}
