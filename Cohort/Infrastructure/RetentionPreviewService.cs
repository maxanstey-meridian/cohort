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
        var entities = SweepEntityScopes.For(tenant);

        await readinessValidator.ValidateAsync(ct);
        // A preview changes nothing, so it resolves its time as a dry run: any requested
        // time is allowed, later ones included.
        var (now, startedAt) = await DatabaseClock.ResolveRunTimeAsync(db, request.At, dryRun: true, ct);
        var counts = new List<EntitySweepCount>();
        var timeZone = settings.TimeZone;
        var connection = db.Database.GetDbConnection();
        await db.Database.OpenConnectionAsync(ct);
        try
        {
            foreach (
                var entry in registry
                    .Scan()
                    .Values.Where(entry => entities.Includes(entry))
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
                long affected = 0, held = 0, nullAnchors = 0, blocked = 0;
                if (rule.Strategy != Strategy.Exempt)
                {
                    var strategy = strategies[rule.Strategy];
                    var scope = (
                        await ReferencingForeignKeySql.AttachAsync(connection, [SweepScope.ForSweep(entry, rule, context, timeZone)], ct)
                    )[0];
                    affected = await strategy.CountAsync(scope, SweepCount.Eligible, connection, ct);
                    held = await strategy.CountAsync(scope, SweepCount.Held, connection, ct);
                    nullAnchors = await strategy.CountAsync(scope, SweepCount.NullAnchor, connection, ct);
                    blocked = await strategy.CountAsync(scope, SweepCount.Blocked, connection, ct);
                }

                counts.Add(
                    new EntitySweepCount(
                        entry.EntityType,
                        entry.Category,
                        tenant.Id,
                        rule.Strategy,
                        affected,
                        held,
                        SkippedCount: 0,
                        NullAnchorCount: nullAnchors,
                        // A preview erases nothing and runs nothing, so nothing is retained
                        // from an erasure or left behind by a run. Rows a reference blocks
                        // are known up front.
                        RetainedCount: 0,
                        DeferredCount: 0,
                        BlockedCount: blocked
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
