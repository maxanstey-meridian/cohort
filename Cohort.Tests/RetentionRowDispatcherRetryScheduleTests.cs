using Cohort.Infrastructure.Handlers;

namespace Cohort.Tests;

public sealed class RetentionRowDispatcherRetryScheduleTests
{
    [Theory]
    [InlineData(1, 1)]
    [InlineData(2, 2)]
    [InlineData(4, 8)]
    public void CalculateNextAttemptAt_Applies_Exponential_Backoff(
        int attempt,
        int expectedDelaySeconds
    )
    {
        var now = new DateTimeOffset(2026, 1, 1, 0, 0, 0, TimeSpan.Zero);

        var nextAttemptAt = RetentionRowDispatcher.CalculateNextAttemptAt(
            now,
            TimeSpan.FromSeconds(1),
            attempt
        );

        nextAttemptAt.Should().Be(now.AddSeconds(expectedDelaySeconds));
    }

    [Fact]
    public void CalculateNextAttemptAt_Saturates_Huge_Valid_Backoff_And_Attempt()
    {
        var now = new DateTimeOffset(2026, 1, 1, 0, 0, 0, TimeSpan.Zero);

        var nextAttemptAt = RetentionRowDispatcher.CalculateNextAttemptAt(
            now,
            TimeSpan.MaxValue,
            int.MaxValue
        );

        nextAttemptAt.Should().Be(RetentionRowDispatcher.RetryScheduleUpperBound);
    }

    [Fact]
    public void CalculateNextAttemptAt_Saturates_When_Ordinary_Backoff_Crosses_Upper_Bound()
    {
        var now = RetentionRowDispatcher.RetryScheduleUpperBound.AddSeconds(-1);

        var nextAttemptAt = RetentionRowDispatcher.CalculateNextAttemptAt(
            now,
            TimeSpan.FromSeconds(1),
            2
        );

        nextAttemptAt.Should().Be(RetentionRowDispatcher.RetryScheduleUpperBound);
    }
}
