using Cohort.Hosting;

namespace Cohort.Tests;

public sealed class CohortOptionsValidatorTests
{
    [Theory]
    [InlineData("Conventions")]
    [InlineData("RowHandlerDispatch")]
    public void Validate_Rejects_Null_Nested_Options(string option)
    {
        var options = option switch
        {
            "Conventions" => new CohortOptions { Conventions = null! },
            "RowHandlerDispatch" => new CohortOptions { RowHandlerDispatch = null! },
            _ => throw new ArgumentOutOfRangeException(nameof(option)),
        };

        var result = new CohortOptionsValidator().Validate(null, options);

        result.Failed.Should().BeTrue();
        result.Failures.Should().ContainSingle(message => message.Contains(option));
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
    [InlineData("RecordIdPropertyName")]
    [InlineData("TenantPropertyName")]
    [InlineData("SoftDeletePropertyName")]
    [InlineData("DeletedAtPropertyName")]
    [InlineData("AnonymisedAtPropertyName")]
    public void Validate_Rejects_Blank_Convention_Names(string option)
    {
        var conventions = option switch
        {
            "RecordIdPropertyName" => new CohortConventions { RecordIdPropertyName = " " },
            "TenantPropertyName" => new CohortConventions { TenantPropertyName = " " },
            "SoftDeletePropertyName" => new CohortConventions
            {
                SoftDeletePropertyName = " ",
            },
            "DeletedAtPropertyName" => new CohortConventions { DeletedAtPropertyName = " " },
            "AnonymisedAtPropertyName" => new CohortConventions
            {
                AnonymisedAtPropertyName = " ",
            },
            _ => throw new ArgumentOutOfRangeException(nameof(option)),
        };

        var result = new CohortOptionsValidator().Validate(
            null,
            new CohortOptions { Conventions = conventions }
        );

        result.Failed.Should().BeTrue();
        result.Failures.Should().ContainSingle(message => message.Contains(option));
    }
}
