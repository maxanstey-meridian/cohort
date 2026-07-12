namespace Cohort.Infrastructure;

internal static class OperationalTime
{
    internal static readonly TimeSpan MaxDelayChunk = TimeSpan.FromMinutes(1);

    internal static DateTimeOffset SubtractSaturating(
        DateTimeOffset value,
        TimeSpan duration
    )
    {
        if (duration <= TimeSpan.Zero)
        {
            return value;
        }

        return duration.Ticks > value.UtcTicks - DateTimeOffset.MinValue.UtcTicks
            ? DateTimeOffset.MinValue
            : value.Subtract(duration);
    }

    internal static TimeSpan GetDelayChunk(TimeSpan remaining)
    {
        if (remaining <= TimeSpan.Zero)
        {
            return TimeSpan.Zero;
        }

        return remaining < MaxDelayChunk ? remaining : MaxDelayChunk;
    }

    internal static async Task DelayAsync(TimeSpan duration, CancellationToken ct)
    {
        var remaining = duration;
        while (remaining > TimeSpan.Zero)
        {
            var chunk = GetDelayChunk(remaining);
            await Task.Delay(chunk, ct);
            remaining -= chunk;
        }
    }
}
