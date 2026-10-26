using Cohort.Hosting;

namespace Cohort.Tests;

public sealed class RetentionRowDispatcherRetryScheduleTests
{
    [Theory]
    [InlineData(1, 1, 3600, 1)]
    [InlineData(2, 1, 3600, 2)]
    [InlineData(4, 1, 3600, 8)]
    [InlineData(10, 5, 3600, 2560)]
    [InlineData(11, 5, 3600, 3600)]
    [InlineData(12, 5, 3600, 3600)]
    [InlineData(1000, 5, 3600, 3600)]
    [InlineData(3, 5, 5, 5)]
    public void CalculateNextAttemptAt_Doubles_The_Backoff_Up_To_MaxBackoff(
        int attempt,
        int baseBackoffSeconds,
        int maxBackoffSeconds,
        int expectedDelaySeconds
    )
    {
        var now = new DateTimeOffset(2026, 1, 1, 0, 0, 0, TimeSpan.Zero);

        var nextAttemptAt = RetentionRowDispatcher.CalculateNextAttemptAt(
            now,
            TimeSpan.FromSeconds(baseBackoffSeconds),
            TimeSpan.FromSeconds(maxBackoffSeconds),
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
            TimeSpan.MaxValue,
            int.MaxValue
        );

        nextAttemptAt.Should().Be(RetentionRowDispatcher.RetryScheduleUpperBound);
    }

    [Theory]
    // Defaults: 5 s doubling to the 1 h cap over 24 waits (5 115 s + 14 h), plus 25 claim
    // timeouts of 5 min.
    [InlineData(25, 5, 3600, 300, 5115 + (14 * 3600) + (25 * 300))]
    [InlineData(1, 5, 3600, 300, 300)]
    [InlineData(3, 60, 90, 30, 60 + 90 + (3 * 30))]
    public void RetryScheduleSpan_Adds_Every_Capped_Backoff_And_Every_Claim_Timeout(
        int maxAttempts,
        int baseBackoffSeconds,
        int maxBackoffSeconds,
        int claimTimeoutSeconds,
        int expectedSpanSeconds
    )
    {
        var span = RetentionRowDispatcher.RetryScheduleSpan(
            new RowHandlerDispatchOptions
            {
                MaxAttempts = maxAttempts,
                BaseBackoff = TimeSpan.FromSeconds(baseBackoffSeconds),
                MaxBackoff = TimeSpan.FromSeconds(maxBackoffSeconds),
                ClaimTimeout = TimeSpan.FromSeconds(claimTimeoutSeconds),
            }
        );

        span.Should().Be(TimeSpan.FromSeconds(expectedSpanSeconds));
    }

    [Theory]
    // One handler at the defaults: the retry schedule plus 25 polls of 10 s.
    [InlineData(1, 25, 5, 3600, 300, 10, 5115 + (14 * 3600) + (25 * 300) + (25 * 10))]
    // A row's handlers run one after another.
    [InlineData(3, 2, 60, 60, 30, 5, 3 * (60 + (2 * 30) + (2 * 5)))]
    public void RowRetryScheduleSpan_Adds_Each_Handlers_Schedule_And_Poll_Delays(
        int handlerCount,
        int maxAttempts,
        int baseBackoffSeconds,
        int maxBackoffSeconds,
        int claimTimeoutSeconds,
        int pollIntervalSeconds,
        int expectedSpanSeconds
    )
    {
        var span = RetentionRowDispatcher.RowRetryScheduleSpan(
            new RowHandlerDispatchOptions
            {
                MaxAttempts = maxAttempts,
                BaseBackoff = TimeSpan.FromSeconds(baseBackoffSeconds),
                MaxBackoff = TimeSpan.FromSeconds(maxBackoffSeconds),
                ClaimTimeout = TimeSpan.FromSeconds(claimTimeoutSeconds),
                PollInterval = TimeSpan.FromSeconds(pollIntervalSeconds),
            },
            handlerCount
        );

        span.Should().Be(TimeSpan.FromSeconds(expectedSpanSeconds));
    }

    [Fact]
    public void RowRetryScheduleSpan_Saturates_A_Huge_PollInterval()
    {
        var span = RetentionRowDispatcher.RowRetryScheduleSpan(
            new RowHandlerDispatchOptions { PollInterval = TimeSpan.MaxValue },
            256
        );

        span.Should().Be(TimeSpan.MaxValue);
    }
}
