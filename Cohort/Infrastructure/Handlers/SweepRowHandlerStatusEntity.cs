#nullable enable

using Cohort.Application;

namespace Cohort.Infrastructure.Handlers;

internal sealed class SweepRowHandlerStatusEntity
{
    public long Id { get; set; }

    public long SweepRunRowDetailId { get; set; }

    public string HandlerType { get; set; } = "";

    public RowHandlerDispatchPhase DispatchPhase { get; set; }

    public SweepRowHandlerDispatchState State { get; set; }

    public int Attempt { get; set; }

    public DateTimeOffset QueuedAt { get; set; }

    public DateTimeOffset NextAttemptAt { get; set; }

    public DateTimeOffset? ClaimedAt { get; set; }

    public DateTimeOffset? CompletedAt { get; set; }

    public string? LastError { get; set; }
}

internal enum SweepRowHandlerDispatchState
{
    Pending,
    InFlight,
    Succeeded,
    DeadLettered,
}
