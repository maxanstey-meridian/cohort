using Cohort.Hosting;

namespace Cohort.Tests;

public sealed class CohortOptionsValidatorTests
{
    [Fact]
    public void Validate_Rejects_Audit_Observer_Timeout_Above_Safe_Ceiling()
    {
        var options = new CohortOptions
        {
            AuditObservers = new AuditObserverOptions { Timeout = TimeSpan.FromHours(1).Add(TimeSpan.FromTicks(1)) },
        };

        var result = new CohortOptionsValidator().Validate(null, options);

        result.Failed.Should().BeTrue();
        result.Failures.Should().ContainSingle(message => message.Contains("AuditObservers Timeout"));
    }

    [Theory]
    [InlineData("")]
    [InlineData("Mars/Olympus_Mons")]
    public void Validate_Rejects_A_Time_Zone_The_Host_Does_Not_Know(string timeZone)
    {
        var result = new CohortOptionsValidator().Validate(null, new CohortOptions { TimeZone = timeZone });

        result.Failed.Should().BeTrue();
        result.Failures.Should().ContainSingle(message => message.Contains("TimeZone"));
    }

    [Theory]
    [InlineData("UTC")]
    [InlineData("Europe/London")]
    [InlineData("GMT Standard Time")]
    public void Validate_Accepts_IANA_And_Windows_Time_Zone_Ids(string timeZone)
    {
        var result = new CohortOptionsValidator().Validate(null, new CohortOptions { TimeZone = timeZone });

        result.Succeeded.Should().BeTrue();
    }

    [Theory]
    [InlineData("OccurrenceRetryDelay", 0, 3)]
    [InlineData("OccurrenceRetryDelay", 86_401, 3)]
    [InlineData("OccurrenceMaxAttempts", 900, 0)]
    [InlineData("OccurrenceMaxAttempts", 900, 101)]
    public void Validate_Rejects_Occurrence_Retry_Options_Outside_Their_Bounds(
        string option,
        int retryDelaySeconds,
        int maxAttempts
    )
    {
        var result = new CohortOptionsValidator().Validate(
            null,
            new CohortOptions
            {
                OccurrenceRetryDelay = TimeSpan.FromSeconds(retryDelaySeconds),
                OccurrenceMaxAttempts = maxAttempts,
            }
        );

        result.Failed.Should().BeTrue();
        result.Failures.Should().ContainSingle(message => message.Contains(option));
    }

    [Theory]
    [InlineData(1, 1)]
    [InlineData(86_400, 100)]
    public void Validate_Accepts_Occurrence_Retry_Options_At_Their_Bounds(int retryDelaySeconds, int maxAttempts)
    {
        new CohortOptionsValidator()
            .Validate(
                null,
                new CohortOptions
                {
                    OccurrenceRetryDelay = TimeSpan.FromSeconds(retryDelaySeconds),
                    OccurrenceMaxAttempts = maxAttempts,
                }
            )
            .Succeeded.Should()
            .BeTrue();
    }

    [Theory]
    [InlineData("PollInterval")]
    [InlineData("BatchSize")]
    [InlineData("MaxAttempts")]
    [InlineData("MaxParallelism")]
    [InlineData("BaseBackoff")]
    [InlineData("MaxBackoff")]
    [InlineData("ClaimTimeout")]
    [InlineData("SweepSettleTimeout")]
    [InlineData("PayloadRetention")]
    public void Validate_Rejects_Nonsensical_Row_Handler_Dispatch_Options(string option)
    {
        var dispatch = option switch
        {
            "PollInterval" => new RowHandlerDispatchOptions { PollInterval = TimeSpan.Zero },
            "BatchSize" => new RowHandlerDispatchOptions { BatchSize = 0 },
            "MaxAttempts" => new RowHandlerDispatchOptions { MaxAttempts = 0 },
            "MaxParallelism" => new RowHandlerDispatchOptions { MaxParallelism = 0 },
            "BaseBackoff" => new RowHandlerDispatchOptions { BaseBackoff = TimeSpan.Zero },
            "MaxBackoff" => new RowHandlerDispatchOptions
            {
                BaseBackoff = TimeSpan.FromMinutes(2),
                MaxBackoff = TimeSpan.FromMinutes(1),
            },
            "ClaimTimeout" => new RowHandlerDispatchOptions
            {
                ClaimTimeout = TimeSpan.FromSeconds(29),
            },
            "SweepSettleTimeout" => new RowHandlerDispatchOptions
            {
                SweepSettleTimeout = TimeSpan.FromSeconds(59),
            },
            "PayloadRetention" => new RowHandlerDispatchOptions
            {
                PayloadRetention = TimeSpan.FromMinutes(59),
            },
            _ => throw new ArgumentOutOfRangeException(nameof(option)),
        };

        var result = new CohortOptionsValidator().Validate(
            null,
            new CohortOptions { RowHandlerDispatch = dispatch }
        );

        result.Failed.Should().BeTrue();
        result.Failures.Should().ContainSingle(message => message.Contains(option));
    }

    [Theory]
    [InlineData("BatchSize")]
    [InlineData("MaxAttempts")]
    [InlineData("MaxParallelism")]
    [InlineData("MaxBackoff")]
    [InlineData("ClaimTimeout")]
    public void Validate_Rejects_Row_Handler_Dispatch_Options_Above_Safe_Ceilings(
        string option
    )
    {
        var dispatch = option switch
        {
            "BatchSize" => new RowHandlerDispatchOptions { BatchSize = 10_001 },
            "MaxAttempts" => new RowHandlerDispatchOptions { MaxAttempts = 1_001 },
            "MaxParallelism" => new RowHandlerDispatchOptions { MaxParallelism = 257 },
            "MaxBackoff" => new RowHandlerDispatchOptions
            {
                MaxBackoff = TimeSpan.FromDays(1).Add(TimeSpan.FromTicks(1)),
            },
            "ClaimTimeout" => new RowHandlerDispatchOptions
            {
                ClaimTimeout = TimeSpan.FromDays(1).Add(TimeSpan.FromTicks(1)),
            },
            _ => throw new ArgumentOutOfRangeException(nameof(option)),
        };

        var result = new CohortOptionsValidator().Validate(
            null,
            new CohortOptions { RowHandlerDispatch = dispatch }
        );

        result.Failed.Should().BeTrue();
        result.Failures.Should().ContainSingle(message => message.Contains(option));
    }

    [Theory]
    [InlineData("BatchSize")]
    [InlineData("MaxAttempts")]
    [InlineData("MaxParallelism")]
    [InlineData("MaxBackoff")]
    [InlineData("ClaimTimeout")]
    public void Validate_Accepts_Row_Handler_Dispatch_Options_At_Safe_Ceilings(string option)
    {
        var dispatch = option switch
        {
            "BatchSize" => new RowHandlerDispatchOptions { BatchSize = 10_000 },
            // 1000 attempts retry for about 45 days, so the snapshot must outlive that.
            "MaxAttempts" => new RowHandlerDispatchOptions
            {
                MaxAttempts = 1_000,
                PayloadRetention = TimeSpan.FromDays(60),
            },
            "MaxParallelism" => new RowHandlerDispatchOptions { MaxParallelism = 256 },
            "MaxBackoff" => new RowHandlerDispatchOptions
            {
                MaxBackoff = TimeSpan.FromDays(1),
                PayloadRetention = TimeSpan.FromDays(60),
            },
            "ClaimTimeout" => new RowHandlerDispatchOptions { ClaimTimeout = TimeSpan.FromDays(1) },
            _ => throw new ArgumentOutOfRangeException(nameof(option)),
        };

        new CohortOptionsValidator()
            .Validate(null, new CohortOptions { RowHandlerDispatch = dispatch })
            .Succeeded.Should()
            .BeTrue();
    }

    [Fact]
    public void Validate_Rejects_A_PayloadRetention_The_Retry_Schedule_Outlasts()
    {
        // The defaults retry for about 17.5 hours, so a 1 hour snapshot retention would scrub
        // work that is still retrying.
        var result = new CohortOptionsValidator().Validate(
            null,
            new CohortOptions
            {
                RowHandlerDispatch = new RowHandlerDispatchOptions { PayloadRetention = TimeSpan.FromHours(1) },
            }
        );

        result.Failed.Should().BeTrue();
        result.Failures.Should().ContainSingle(message =>
            message.Contains("PayloadRetention") && message.Contains("retry schedule")
        );
    }

    [Fact]
    public void Validate_Accepts_A_Short_PayloadRetention_With_A_Retry_Schedule_That_Fits()
    {
        new CohortOptionsValidator()
            .Validate(
                null,
                new CohortOptions
                {
                    RowHandlerDispatch = new RowHandlerDispatchOptions
                    {
                        MaxAttempts = 3,
                        PayloadRetention = TimeSpan.FromHours(1),
                    },
                }
            )
            .Succeeded.Should()
            .BeTrue();
    }

    [Theory]
    [InlineData("SucceededRunRetention")]
    [InlineData("FailedRunRetention")]
    [InlineData("InactiveHoldRetention")]
    public void Validate_Rejects_Nonsensical_History_Pruning_Options(string option)
    {
        var pruning = option switch
        {
            "SucceededRunRetention" => new HistoryPruningOptions { SucceededRunRetention = TimeSpan.FromHours(23) },
            "FailedRunRetention" => new HistoryPruningOptions { FailedRunRetention = TimeSpan.Zero },
            "InactiveHoldRetention" => new HistoryPruningOptions { InactiveHoldRetention = TimeSpan.FromDays(-90) },
            _ => throw new ArgumentOutOfRangeException(nameof(option)),
        };

        var result = new CohortOptionsValidator().Validate(
            null,
            new CohortOptions { HistoryPruning = pruning }
        );

        result.Failed.Should().BeTrue();
        result.Failures.Should().ContainSingle(message => message.Contains($"HistoryPruning {option}"));
    }

    [Fact]
    public void Validate_Accepts_Unset_And_Minimum_History_Pruning_Retentions()
    {
        new CohortOptionsValidator()
            .Validate(null, new CohortOptions())
            .Succeeded.Should()
            .BeTrue();
        new CohortOptionsValidator()
            .Validate(
                null,
                new CohortOptions
                {
                    HistoryPruning = new HistoryPruningOptions
                    {
                        SucceededRunRetention = TimeSpan.FromDays(1),
                        FailedRunRetention = TimeSpan.FromDays(1),
                        InactiveHoldRetention = TimeSpan.FromDays(1),
                    },
                }
            )
            .Succeeded.Should()
            .BeTrue();
    }
}
