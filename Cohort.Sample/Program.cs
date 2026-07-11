using Cohort.Domain;
using Cohort.Sample;
using Cohort.Sample.Entities;
using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;

var builder = Host.CreateApplicationBuilder(args);
builder.Services.AddSampleRetentionServices();

var host = builder.Build();

await using (var migrationScope = host.Services.CreateAsyncScope())
{
    var db = migrationScope.ServiceProvider.GetRequiredService<SampleDbContext>();
    await db.Database.MigrateAsync();
}

await host.StartAsync();

try
{
    using var scope = host.Services.CreateScope();
    var logger = scope.ServiceProvider.GetRequiredService<ILogger<Program>>();
    var previewTenant = scope.ServiceProvider.GetRequiredService<TenantContext>();
    var startup = scope.ServiceProvider.GetRequiredService<SampleRetentionStartupService>();

    var preview = await startup.RunPreviewAsync(previewTenant, DateTimeOffset.UtcNow);

    logger.LogInformation(
        "Attributes wired: found {Count} retention entries",
        preview.Counts.Count
    );

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

    // Demonstrate an actual sweep: seed a note past its 30-day category, sweep, verify.
    var db = scope.ServiceProvider.GetRequiredService<SampleDbContext>();
    var demoNoteId = Guid.NewGuid();
    db.Notes.Add(
        new Note
        {
            Id = demoNoteId,
            TenantId = previewTenant.Id,
            CreatedAt = DateTimeOffset.UtcNow.AddDays(-120),
            Body = "sweep-demo",
        }
    );
    await db.SaveChangesAsync();

    var sweep = await startup.RunSweepAsync(previewTenant, DateTimeOffset.UtcNow);
    foreach (var count in sweep.Counts)
    {
        logger.LogInformation(
            "Sweep {EntityType} → affected={Affected} held={Held} skipped={Skipped} nullAnchors={NullAnchors}",
            count.EntityType.Name,
            count.Affected,
            count.HeldCount,
            count.SkippedCount,
            count.NullAnchorCount
        );
    }

    var demoNoteRemoved = !await db.Notes.AnyAsync(note => note.Id == demoNoteId);
    logger.LogInformation("Sweep removed the expired demo note: {Removed}", demoNoteRemoved);
}
finally
{
    await host.StopAsync();
}
