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
    [InlineData("PollInterval")]
    [InlineData("BatchSize")]
    [InlineData("MaxAttempts")]
    [InlineData("MaxParallelism")]
    [InlineData("BaseBackoff")]
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
    [InlineData("ClaimTimeout")]
    public void Validate_Accepts_Row_Handler_Dispatch_Options_At_Safe_Ceilings(string option)
    {
        var dispatch = option switch
        {
            "BatchSize" => new RowHandlerDispatchOptions { BatchSize = 10_000 },
            "MaxAttempts" => new RowHandlerDispatchOptions { MaxAttempts = 1_000 },
            "MaxParallelism" => new RowHandlerDispatchOptions { MaxParallelism = 256 },
            "ClaimTimeout" => new RowHandlerDispatchOptions { ClaimTimeout = TimeSpan.FromDays(1) },
            _ => throw new ArgumentOutOfRangeException(nameof(option)),
        };

        new CohortOptionsValidator()
            .Validate(null, new CohortOptions { RowHandlerDispatch = dispatch })
            .Succeeded.Should()
            .BeTrue();
    }
}
