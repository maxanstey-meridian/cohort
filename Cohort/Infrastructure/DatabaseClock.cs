using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Storage;

namespace Cohort.Infrastructure;

/// <summary>
/// The database's clock, the same one hold expiry and hold reporting compare against, so a
/// run's "today" can never run ahead of the clock that decides which holds are active.
/// </summary>
internal static class DatabaseClock
{
    internal static async Task<DateTimeOffset> NowAsync(DbContext db, CancellationToken ct)
    {
        await db.Database.OpenConnectionAsync(ct);
        try
        {
            // statement_timestamp(), not now(): inside a caller's long transaction now() is
            // the transaction's start.
            await using var command = new SqlParams().CreateCommand(
                db.Database.GetDbConnection(),
                db.Database.CurrentTransaction?.GetDbTransaction(),
                "SELECT pg_catalog.statement_timestamp()"
            );
            var value = await command.ExecuteScalarAsync(ct);
            return value switch
            {
                DateTimeOffset offset => offset.ToUniversalTime(),
                DateTime utc => new DateTimeOffset(DateTime.SpecifyKind(utc, DateTimeKind.Utc)),
                _ => throw new InvalidOperationException(
                    $"PostgreSQL returned {value?.GetType().Name ?? "null"} for statement_timestamp()."
                ),
            };
        }
        finally
        {
            await db.Database.CloseConnectionAsync();
        }
    }

    /// <summary>
    /// Resolves the run's "today": the requested time, or the database clock when none was
    /// given. A run that changes rows refuses a time later than the database clock, because
    /// it would act before rows reach their cutoff. Called before any run exists.
    /// </summary>
    internal static async Task<DateTimeOffset> ResolveRunTimeAsync(
        DbContext db,
        DateTimeOffset? requested,
        bool dryRun,
        CancellationToken ct
    )
    {
        var databaseNow = await NowAsync(db, ct);
        var now = requested?.ToUniversalTime() ?? databaseNow;
        if (!dryRun && now > databaseNow)
        {
            throw new ArgumentOutOfRangeException(
                "at",
                now,
                $"A run that changes rows can't use a time later than the database's ({databaseNow:O}); it would act before rows reach their cutoff. Omit the time to use the database clock, or dry-run to look ahead."
            );
        }

        return now;
    }
}
