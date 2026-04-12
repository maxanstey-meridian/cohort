using System.Data;

using Cohort.Domain;

using Microsoft.EntityFrameworkCore;

namespace Cohort.Application;

public sealed class RetentionPreviewService(
    DbContext db,
    RetentionRegistry registry,
    IRetentionCategoryRepository categoryRepository,
    IEnumerable<IRetentionSweepStrategy> sweepStrategies
) : IRetentionPreview
{
    private readonly IReadOnlyDictionary<Strategy, IRetentionSweepStrategy> strategies = sweepStrategies
        .ToDictionary(strategy => strategy.HandlesStrategy);

    public async Task<RetentionSweepResult> PreviewAsync(
        TenantContext tenant,
        DateTimeOffset now,
        CancellationToken ct = default
    )
    {
        ArgumentNullException.ThrowIfNull(tenant);

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
                    .Values.OrderBy(entry => entry.EntityType.FullName, StringComparer.Ordinal)
            )
            {
                var resolver = await categoryRepository.GetAsync(entry.Category, ct);
                if (resolver is null)
                {
                    throw new InvalidOperationException(
                        $"Retention category '{entry.Category}' for entity {entry.EntityType.FullName} could not be resolved at runtime."
                    );
                }

                var context = new RetentionResolutionContext(entry.Category, tenant, now, []);
                var rule = await resolver.ResolveAsync(context, ct);
                if (rule.Strategy != Strategy.Exempt && !strategies.ContainsKey(rule.Strategy))
                {
                    throw new InvalidOperationException(
                        $"Retention strategy '{rule.Strategy}' is not supported by the preview path."
                    );
                }

                var affected = rule.Strategy switch
                {
                    Strategy.Exempt => 0,
                    _ => await strategies[rule.Strategy].PreviewAsync(entry, rule, context, connection, ct),
                };

                counts.Add(
                    new EntitySweepCount(
                        entry.EntityType,
                        entry.Category,
                        tenant.Id,
                        rule.Strategy,
                        affected
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
