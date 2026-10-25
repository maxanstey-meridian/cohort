using System.Data;

using Cohort.Application;
using Cohort.Domain;
using Cohort.Infrastructure.Sweep;

using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.DependencyInjection;

namespace Cohort.Infrastructure;

internal sealed class RetentionPreviewService(
    [FromKeyedServices(CohortServiceKeys.DbContext)] DbContext db,
    RetentionRegistry registry,
    IRetentionRuleProvider ruleProvider,
    RetentionRuntimeReadinessValidator readinessValidator,
    IEnumerable<SweepStrategy> sweepStrategies,
    IRetentionExecutionSettings settings
)
{
    private readonly IReadOnlyDictionary<Strategy, SweepStrategy> strategies =
        sweepStrategies.ToDictionary(strategy => strategy.HandlesStrategy);

    public async Task<RetentionSweepResult> ExecuteAsync(
        RetentionPreviewRequest request,
        CancellationToken ct = default
    )
    {
        ArgumentNullException.ThrowIfNull(request);

        var tenant = request.RunTenant;
        Func<RetentionEntry, bool> includeEntry = request is RetentionPreviewRequest.TenantlessRequest
            ? entry => entry.Tenant is null
            : entry => entry.Tenant is not null;

        await readinessValidator.ValidateAsync(ct);
        // A preview changes nothing, so any requested time is allowed, later ones included.
        var now = request.At ?? await DatabaseClock.NowAsync(db, ct);

        var startedAt = request.At is null ? now : await DatabaseClock.NowAsync(db, ct);
        var counts = new List<EntitySweepCount>();
        var timeZone = settings.TimeZone;
        var connection = db.Database.GetDbConnection();
        await db.Database.OpenConnectionAsync(ct);
        try
        {
            foreach (
                var entry in registry
                    .Scan()
                    .Values.Where(includeEntry)
                    .OrderBy(entry => entry.EntityType.FullName, StringComparer.Ordinal)
            )
            {
                var context = new RetentionResolutionContext(entry.Category, tenant, now);
                var rule = await RetentionRuleProviderResolution.ResolveAsync(
                    ruleProvider,
                    readinessValidator.ValidatedCapabilities,
                    context,
                    ct
                );
                long affected = 0, held = 0, nullAnchors = 0;
                if (rule.Strategy != Strategy.Exempt)
                {
                    var strategy = strategies[rule.Strategy];
                    var scope = SweepScope.ForSweep(entry, rule, context, timeZone);
                    affected = await strategy.CountAsync(scope, SweepCount.Eligible, connection, ct);
                    held = await strategy.CountAsync(scope, SweepCount.Held, connection, ct);
                    nullAnchors = await strategy.CountAsync(scope, SweepCount.NullAnchor, connection, ct);
                }

                counts.Add(
                    new EntitySweepCount(
                        entry.EntityType,
                        entry.Category,
                        tenant.Id,
                        rule.Strategy,
                        affected,
                        held,
                        NullAnchorCount: nullAnchors
                    )
                );
            }
        }
        finally
        {
            await db.Database.CloseConnectionAsync();
        }

        return new RetentionSweepResult(
            Guid.NewGuid(),
            startedAt,
            await DatabaseClock.NowAsync(db, ct),
            counts
        );
    }
}
