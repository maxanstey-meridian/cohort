using Cohort.Domain;
using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;

namespace Cohort.Infrastructure.Sweep;

internal sealed class SoftDeleteSweepStrategy(
    [FromKeyedServices(CohortServiceKeys.DbContext)] DbContext db,
    IServiceProvider services,
    ILogger<SoftDeleteSweepStrategy> logger
) : SweepStrategy(db, services, logger)
{
    public override Strategy HandlesStrategy => Strategy.SoftDelete;

    protected override void Validate(RetentionEntry entry)
    {
        if (entry.SoftDelete is null)
        {
            throw new InvalidOperationException(
                $"Retention entry for {entry.EntityType.FullName} must expose soft-delete metadata for soft-delete operations."
            );
        }
    }

    protected override string EligibilitySql(RetentionEntry entry) =>
        $"target.{PostgreSqlIdentifier.Quote(entry.SoftDelete!.IsDeletedColumn)} = FALSE";

    protected override SweepMutation CreateMutation(SweepScope scope)
    {
        var entry = scope.Entry;
        var softDelete = entry.SoftDelete!;
        if (softDelete.DeletedAtColumn is null)
        {
            return new(
                $"UPDATE {PostgreSqlIdentifier.Format(entry.Table)} AS target SET {PostgreSqlIdentifier.Quote(softDelete.IsDeletedColumn)} = TRUE",
                static (_, _) => { }
            );
        }

        var deletedAt = DeletedAtValue(entry, softDelete, scope.Now);
        return new(
            $"UPDATE {PostgreSqlIdentifier.Format(entry.Table)} AS target SET {PostgreSqlIdentifier.Quote(softDelete.IsDeletedColumn)} = TRUE, {PostgreSqlIdentifier.Quote(softDelete.DeletedAtColumn)} = @deletedAt",
            (parameters, _) => parameters["deletedAt"] = deletedAt
        );
    }

    private static object DeletedAtValue(
        RetentionEntry entry,
        SoftDeleteConvention softDelete,
        DateTimeOffset now
    )
    {
        var type = ReflectionMemberResolver
            .FindPropertyByName(entry.EntityType, softDelete.DeletedAtMember!)
            ?.PropertyType;
        return type == typeof(DateTime) || type == typeof(DateTime?)
            ? now.UtcDateTime
            : type == typeof(DateTimeOffset) || type == typeof(DateTimeOffset?)
                ? now
                : throw new InvalidOperationException(
                    $"Soft-delete DeletedAt member '{softDelete.DeletedAtMember}' on {entry.EntityType.FullName} must be DateTime or DateTimeOffset (nullable allowed), got {type?.Name ?? "no property"}."
                );
    }
}
