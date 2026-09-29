using Cohort.Application;
using Cohort.Domain;
using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;

namespace Cohort.Infrastructure.Sweep;

internal sealed class AnonymiseSweepStrategy(
    [FromKeyedServices(CohortServiceKeys.DbContext)] DbContext db,
    IEnumerable<IAnonymiseValueFactory> anonymiseValueFactories,
    IServiceProvider services,
    ILogger<AnonymiseSweepStrategy> logger
) : SweepStrategy(db, services, logger)
{
    private readonly AnonymiseAssignmentResolver assignments = new(db, anonymiseValueFactories);

    public override Strategy HandlesStrategy => Strategy.Anonymise;

    protected override void Validate(RetentionEntry entry)
    {
        if (entry.AnonymiseFields.Count == 0)
        {
            throw new InvalidOperationException(
                $"Retention entry for {entry.EntityType.FullName} must expose anonymise metadata for anonymisation."
            );
        }

        // The startup validator only enforces the marker for rules it can resolve at boot.
        // A runtime-resolved Anonymise rule on an entity without it would reselect the same
        // rows every batch, because the mutation never shrinks the candidate filter.
        if (entry.AnonymisedAt is null)
        {
            throw new InvalidOperationException(
                $"Retention entry for {entry.EntityType.FullName} resolves to the Anonymise strategy but has no AnonymisedAt marker (a nullable DateTimeOffset named AnonymisedAt by convention, or marked with [RetentionAnonymisedAt]). Without it anonymisation cannot tell scrubbed rows from pending ones and batched anonymisation would never terminate."
            );
        }
    }

    // The AnonymisedAt marker keeps anonymisation idempotent: rows scrubbed by an earlier
    // run fall out of scope instead of being re-anonymised forever.
    protected override string EligibilitySql(RetentionEntry entry) =>
        $"target.{PostgreSqlIdentifier.Quote(entry.AnonymisedAt!.AnonymisedAtColumn)} IS NULL";

    protected override SweepMutation CreateMutation(SweepScope scope)
    {
        var entry = scope.Entry;
        var set = entry.AnonymiseFields.Select((field, index) => field.AssignmentSql($"value{index}"));
        var staticValues = assignments.CreateStaticAssignments(entry, scope.Tenant.Id, scope.Now);

        return new SweepMutation(
            $"""
            UPDATE {PostgreSqlIdentifier.Format(entry.Table)} AS target
            SET {string.Join(", ", set)}, {PostgreSqlIdentifier.Quote(entry.AnonymisedAt!.AnonymisedAtColumn)} = @anonymisedAt
            """,
            (parameters, row) =>
            {
                var values = row is null
                    ? assignments.ToProviderValues(entry, staticValues)
                    : assignments.CreatePerRowAssignmentValues(entry, scope.Tenant, scope.Now, row, staticValues);
                for (var index = 0; index < values.Count; index++)
                {
                    parameters[$"value{index}"] = values[index];
                }
                parameters["anonymisedAt"] = scope.Now;
            },
            RowByRow: assignments.RequiresPerRowExecution(entry)
        );
    }
}
