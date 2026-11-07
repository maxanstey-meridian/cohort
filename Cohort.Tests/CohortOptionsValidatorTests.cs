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

    [Fact]
    public void Validate_Rejects_A_SweepSettleTimeout_Under_One_Minute()
    {
        var result = new CohortOptionsValidator().Validate(
            null,
            new CohortOptions { SweepSettleTimeout = TimeSpan.FromSeconds(59) }
        );

        result.Failed.Should().BeTrue();
        result.Failures.Should().ContainSingle(message => message.Contains("SweepSettleTimeout"));
    }

    [Fact]
    public void Validate_Accepts_A_SweepSettleTimeout_Of_Exactly_One_Minute()
    {
        new CohortOptionsValidator()
            .Validate(null, new CohortOptions { SweepSettleTimeout = TimeSpan.FromMinutes(1) })
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
