using Cohort.Application;
using Cohort.Domain;
using Cohort.Sample.Entities;

namespace Cohort.Sample;

/// <summary>
/// Deposits a message saying a note was retired in the sample's stand-in outbox, in the
/// transaction that retires the note, so it commits and rolls back with it. The sample only
/// deposits: nothing delivers the message.
/// </summary>
public sealed class NoteOutboxStep(SampleDbContext db) : IRetentionDependents<Note>
{
    public async Task RetireAsync(Note row, RetentionDependentsContext ctx, CancellationToken ct)
    {
        // A message saved outside Cohort's transaction would commit on its own, even when the
        // note it announces is never retired.
        if (db.Database.CurrentTransaction is null)
        {
            throw new InvalidOperationException(
                "NoteOutboxStep writes the outbox only inside the transaction that retires the note."
            );
        }

        db.OutboxMessages.Add(
            new OutboxMessage
            {
                Id = Guid.NewGuid(),
                TenantId = ctx.TenantId,
                Type = "note-retired",
                Payload = row.Id.ToString(),
                CreatedAt = ctx.At,
            }
        );
        await db.SaveChangesAsync(ct);
    }
}
