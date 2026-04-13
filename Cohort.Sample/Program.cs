using Cohort.Domain;
using Cohort.Sample;

using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;

var builder = Host.CreateApplicationBuilder(args);
builder.Services.AddSampleRetentionServices();

var host = builder.Build();
await host.StartAsync();

try
{
    using var scope = host.Services.CreateScope();
    var logger = scope.ServiceProvider.GetRequiredService<ILogger<Program>>();
    var previewTenant = scope.ServiceProvider.GetRequiredService<TenantContext>();
    var startup = scope.ServiceProvider.GetRequiredService<SampleRetentionStartupService>();

    var entries = await startup.RunAsync();
    var preview = await startup.RunPreviewAsync(previewTenant, DateTimeOffset.UtcNow);

    logger.LogInformation("Found {Count} retention entries", entries.Count);
    foreach (var entry in entries.Values)
    {
        logger.LogInformation(
            "  {EntityType} → table={Table} category={Category} anchor={Anchor}",
            entry.EntityType.Name,
            entry.TableName,
            entry.Category,
            entry.AnchorMember
        );
    }

    foreach (var count in preview.Counts)
    {
        logger.LogInformation(
            "Preview {EntityType} → category={Category} strategy={Strategy} tenant={TenantId} candidates={Candidates}",
            count.EntityType.Name,
            count.Category,
            count.Strategy,
            count.TenantId,
            count.Affected
        );
    }
}
finally
{
    await host.StopAsync();
}
