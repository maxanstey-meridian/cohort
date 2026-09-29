using Cohort.Infrastructure;

namespace Cohort.Tests;

public sealed class OperationalTimeTests
{
    [Theory]
    [InlineData("0001-01-01T00:00:00+00:00", long.MaxValue)]
    [InlineData("2026-01-01T00:00:00+00:00", long.MaxValue)]
    [InlineData("2026-01-01T00:00:00+00:00", 0)]
    public void SubtractSaturating_Never_Underflows(
        string value,
        long durationTicks
    )
    {
        var timestamp = DateTimeOffset.Parse(value);
        var duration = TimeSpan.FromTicks(durationTicks);

        var result = OperationalTime.SubtractSaturating(timestamp, duration);

        (result >= DateTimeOffset.MinValue).Should().BeTrue();
        if (durationTicks == long.MaxValue)
        {
            result.Should().Be(DateTimeOffset.MinValue);
        }
        else
        {
            result.Should().Be(timestamp);
        }
    }

    [Theory]
    [InlineData(0, 0)]
    [InlineData(1, 1)]
    [InlineData(599_999_999, 599_999_999)]
    [InlineData(600_000_000, 600_000_000)]
    [InlineData(600_000_001, 600_000_000)]
    [InlineData(long.MaxValue, 600_000_000)]
    public void GetDelayChunk_Clamps_At_The_Exact_One_Minute_Boundary(
        long remainingTicks,
        long expectedTicks
    )
    {
        OperationalTime
            .GetDelayChunk(TimeSpan.FromTicks(remainingTicks))
            .Should()
            .Be(TimeSpan.FromTicks(expectedTicks));
    }

    [Fact]
    public void SubtractSaturating_Preserves_Exact_Representable_Boundaries()
    {
        OperationalTime
            .SubtractSaturating(DateTimeOffset.MinValue, TimeSpan.FromTicks(1))
            .Should()
            .Be(DateTimeOffset.MinValue);
        OperationalTime
            .SubtractSaturating(
                DateTimeOffset.MinValue.AddTicks(1),
                TimeSpan.FromTicks(1)
            )
            .Should()
            .Be(DateTimeOffset.MinValue);
        OperationalTime
            .SubtractSaturating(DateTimeOffset.MaxValue, TimeSpan.FromTicks(1))
            .Should()
            .Be(DateTimeOffset.MaxValue.AddTicks(-1));
        OperationalTime
            .SubtractSaturating(DateTimeOffset.MaxValue, TimeSpan.MaxValue)
            .Should()
            .Be(DateTimeOffset.MinValue);
    }
}
