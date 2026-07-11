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
    IRetentionCategoryRepository categoryRepository,
    RetentionStartupValidator validator,
    IEnumerable<IRetentionSweepStrategy> sweepStrategies
)
{
    private readonly IReadOnlyDictionary<Strategy, IRetentionSweepStrategy> strategies =
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

        await validator.ValidateAsync(ct);

        var startedAt = DateTimeOffset.UtcNow;
        var counts = new List<EntitySweepCount>();
        var connection = db.Database.GetDbConnection();
        var shouldCloseConnection = connection.State != ConnectionState.Open;

        if (shouldCloseConnection)
        {
            await db.Database.OpenConnectionAsync(ct);
        }

        try
        {
            foreach (
                var entry in registry
                    .Scan()
                    .Values.Where(includeEntry)
                    .OrderBy(entry => entry.EntityType.FullName, StringComparer.Ordinal)
            )
            {
                var resolver = await categoryRepository.GetAsync(entry.Category, ct);
                if (resolver is null)
                {
                    throw new InvalidOperationException(
                        $"Retention category '{entry.Category}' for entity {entry.EntityType.FullName} could not be resolved at runtime."
                    );
                }

                var context = new RetentionResolutionContext(entry.Category, tenant, request.At, []);
                var rule = await resolver.ResolveAsync(context, ct);
                if (rule.Strategy != Strategy.Exempt && !strategies.ContainsKey(rule.Strategy))
                {
                    throw new InvalidOperationException(
                        $"Retention strategy '{rule.Strategy}' is not supported by the preview path."
                    );
                }

                var measurement =
                    rule.Strategy == Strategy.Exempt
                        ? (Affected: 0L, HeldCount: 0L, NullAnchorCount: 0L)
                        : await RetentionPreviewMeasurement.MeasureAsync(
                            strategies[rule.Strategy],
                            entry,
                            rule,
                            context,
                            connection,
                            ct
                        );

                counts.Add(
                    new EntitySweepCount(
                        entry.EntityType,
                        entry.Category,
                        tenant.Id,
                        rule.Strategy,
                        measurement.Affected,
                        measurement.HeldCount,
                        NullAnchorCount: measurement.NullAnchorCount
                    )
                );
            }
        }
        finally
        {
            if (shouldCloseConnection)
            {
                await db.Database.CloseConnectionAsync();
            }
        }

        return new RetentionSweepResult(Guid.NewGuid(), startedAt, DateTimeOffset.UtcNow, counts);
    }
}
