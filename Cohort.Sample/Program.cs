using Cohort.Application;
using Cohort.Domain;
using Cohort.Sample;

using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;

var builder = Host.CreateApplicationBuilder(args);

builder
    .Services.AddOptions<SampleOptions>()
    .BindConfiguration(SampleOptions.SectionName)
    .ValidateDataAnnotations()
    .ValidateOnStart();

builder.Services.AddDbContext<SampleDbContext>(
    (sp, opts) =>
    {
        var cohort = sp.GetRequiredService<IOptions<SampleOptions>>().Value;
        opts.UseNpgsql(cohort.ConnectionString);
    }
);

builder.Services.AddSingleton<IRetentionRuleResolver>(
    new StaticRetentionRuleResolver(new RetentionRule(TimeSpan.FromDays(30), Strategy.Purge))
);

var host = builder.Build();

using var scope = host.Services.CreateScope();
var db = scope.ServiceProvider.GetRequiredService<SampleDbContext>();
var logger = scope.ServiceProvider.GetRequiredService<ILogger<Program>>();

var registry = new RetentionRegistry(db);
var entries = registry.Scan();

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
