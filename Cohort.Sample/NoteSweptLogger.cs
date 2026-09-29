using Cohort.Application;
using Cohort.Domain;
using Cohort.Sample.Entities;
using Microsoft.Extensions.Logging;

namespace Cohort.Sample;

/// <summary>
/// Runs after the sweep that removed a note has committed, from the note's captured snapshot.
/// </summary>
public sealed class NoteSweptLogger(ILogger<NoteSweptLogger> logger) : IRetentionHandler<Note>
{
    public Task OnAfterAsync(RetentionAfterContext<Note> ctx, CancellationToken ct)
    {
        logger.LogInformation(
            "Note {RecordId} was {Strategy}d by sweep {SweepId} (attempt {Attempt}).",
            ctx.RecordId,
            ctx.Strategy,
            ctx.SweepId,
            ctx.Attempt
        );
        return Task.CompletedTask;
    }
}
