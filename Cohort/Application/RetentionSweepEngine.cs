using System.Data;
using System.Diagnostics;

using Cohort.Domain;

using Microsoft.EntityFrameworkCore;

namespace Cohort.Application;

public sealed class RetentionSweepEngine(
    DbContext db,
    RetentionRegistry registry,
    IRetentionCategoryRepository categoryRepository,
    IRetentionSweepStrategy purgeSweepStrategy
)
{
    public async Task<RetentionSweepResult> SweepAsync(
        TenantContext tenant,
        DateTimeOffset now,
        CancellationToken ct = default
    )
    {
        ArgumentNullException.ThrowIfNull(tenant);

        var startedAt = DateTimeOffset.UtcNow;
        var executionPlan = new List<(RetentionEntry Entry, RetentionResolutionContext Context, RetentionRule Rule)>();

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
            if (rule.Strategy is not Strategy.Purge and not Strategy.Exempt)
            {
                throw new InvalidOperationException(
                    $"Retention strategy '{rule.Strategy}' is not supported by the Milestone A sweep engine."
                );
            }

            executionPlan.Add((entry, context, rule));
        }

        var counts = new List<EntitySweepCount>();
        if (executionPlan.Count == 0)
        {
            return new RetentionSweepResult(Guid.NewGuid(), startedAt, DateTimeOffset.UtcNow, counts);
        }

        var connection = db.Database.GetDbConnection();
        var shouldCloseConnection = connection.State != ConnectionState.Open;

        if (shouldCloseConnection)
        {
            await db.Database.OpenConnectionAsync(ct);
        }

        try
        {
            await using var transaction = await db.Database.BeginTransactionAsync(ct);

            foreach (var (entry, context, rule) in executionPlan)
            {
                var affected = rule.Strategy switch
                {
                    Strategy.Purge => await purgeSweepStrategy.SweepAsync(
                        entry,
                        rule,
                        context,
                        connection,
                        ct
                    ),
                    Strategy.Exempt => 0,
                    _ => throw new UnreachableException("Execution plan should only contain supported strategies."),
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

            await transaction.CommitAsync(ct);
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
