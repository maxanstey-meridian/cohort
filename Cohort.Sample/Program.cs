using Cohort.Application;
using Cohort.Domain;
using Cohort.Infrastructure.Sweep;
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
builder.Services.AddScoped<DbContext>(sp => sp.GetRequiredService<SampleDbContext>());

builder.Services.AddSingleton<IRetentionCategoryRepository, SampleCategoryRepository>();
builder.Services.AddScoped<IRetentionSweepStrategy, PurgeSweepStrategy>();
builder.Services.AddScoped<IRetentionSweepStrategy, SoftDeleteSweepStrategy>();
builder.Services.AddScoped<IRetentionSweepStrategy, AnonymiseSweepStrategy>();
builder.Services.AddScoped<IRetentionPreview, RetentionPreviewService>();
builder.Services.AddScoped<RetentionRegistry>();
builder.Services.AddScoped<RetentionStartupValidator>();
builder.Services.AddScoped<RetentionSweepEngine>();
builder.Services.AddScoped<SampleRetentionStartupService>();

var host = builder.Build();

using var scope = host.Services.CreateScope();
var logger = scope.ServiceProvider.GetRequiredService<ILogger<Program>>();
var startup = scope.ServiceProvider.GetRequiredService<SampleRetentionStartupService>();
var previewTenant = new TenantContext(
    Guid.Parse("11111111-1111-1111-1111-111111111111"),
    "sample",
    new Dictionary<string, string>()
);

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
