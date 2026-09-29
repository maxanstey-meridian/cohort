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
    IEnumerable<SweepStrategy> sweepStrategies
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

        var (tenant, includeEntry) = request switch
        {
            RetentionPreviewRequest.TenantedRequest tenanted =>
                (tenanted.Tenant, (Func<RetentionEntry, bool>)(entry => entry.Tenant is not null)),
            RetentionPreviewRequest.TenantlessRequest =>
                (TenantContext.Tenantless, entry => entry.Tenant is null),
            _ => throw new ArgumentOutOfRangeException(nameof(request)),
        };

        await readinessValidator.ValidateAsync(ct);

        var startedAt = DateTimeOffset.UtcNow;
        var counts = new List<EntitySweepCount>();
        var connection = db.Database.GetDbConnection();
        var shouldCloseConnection = connection.State != ConnectionState.Open;
        Exception? primaryException = null;

        try
        {
            if (shouldCloseConnection)
            {
                await db.Database.OpenConnectionAsync(ct);
            }

            foreach (
                var entry in registry
                    .Scan()
                    .Values.Where(includeEntry)
                    .OrderBy(entry => entry.EntityType.FullName, StringComparer.Ordinal)
            )
            {
                var context = new RetentionResolutionContext(entry.Category, tenant, request.At);
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
                    var scope = SweepScope.ForSweep(entry, rule, context);
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
        catch (Exception ex)
        {
            primaryException = ex;
            throw;
        }
        finally
        {
            await OperationalConnectionCleanup.RunAsync(
                null,
                shouldCloseConnection
                    ? cleanupToken => db.Database.CloseConnectionAsync().WaitAsync(cleanupToken)
                    : null,
                primaryException,
                null
            );
        }

        return new RetentionSweepResult(Guid.NewGuid(), startedAt, DateTimeOffset.UtcNow, counts);
    }
}
