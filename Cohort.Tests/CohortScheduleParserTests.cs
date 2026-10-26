using System.Globalization;

using Cohort.Hosting;

namespace Cohort.Tests;

// Pattern: pure unit test, copied from CutoffCalculatorTests. The previous occurrence is what
// the worker catches up on start and after a failed pass.

public sealed class CohortScheduleParserTests
{
    [Theory]
    // An occurrence instant counts as already due.
    [InlineData("0 2 * * *", "2026-10-25T02:00:00+00:00", "2026-10-25T02:00:00+00:00")]
    // Daily: just before today's occurrence, yesterday's is the latest.
    [InlineData("0 2 * * *", "2026-10-25T01:59:59+00:00", "2026-10-24T02:00:00+00:00")]
    // Every second, with sub-second precision in now.
    [InlineData("* * * * * *", "2026-10-25T12:00:00.750+00:00", "2026-10-25T12:00:00+00:00")]
    // Six fields: second precision.
    [InlineData("30 15 3 * * *", "2026-10-25T03:15:29+00:00", "2026-10-24T03:15:30+00:00")]
    // Yearly: walks back most of a year.
    [InlineData("0 0 1 1 *", "2026-10-25T12:00:00+00:00", "2026-01-01T00:00:00+00:00")]
    // Leap day: four years back, and eight across 2100, which is not a leap year.
    [InlineData("0 0 29 2 *", "2027-10-25T12:00:00+00:00", "2024-02-29T00:00:00+00:00")]
    [InlineData("0 0 29 2 *", "2104-02-28T12:00:00+00:00", "2096-02-29T00:00:00+00:00")]
    // Dense but seasonal: every second in January, asked in December (millions of occurrences
    // in the window that finds one).
    [InlineData("* * * * 1 *", "2026-12-15T08:00:00+00:00", "2026-01-31T23:59:59+00:00")]
    // Halved down to the minute that holds an occurrence off the minute boundary.
    [InlineData("17 3 * * * *", "2026-10-25T12:02:00+00:00", "2026-10-25T11:03:17+00:00")]
    public void Previous_Occurrence_Is_The_Latest_At_Or_Before_Now(string schedule, string now, string expected)
    {
        CohortScheduleParser
            .GetPreviousOccurrence(schedule, Parse(now))
            .Should()
            .Be(Parse(expected));
    }

    [Theory]
    // Nothing between the start of time and now.
    [InlineData("0 12 * * *", "0001-01-01T00:00:30+00:00")]
    // The first occurrence is still to come.
    [InlineData("0 0 2 1 *", "0001-01-01T12:00:00+00:00")]
    public void Previous_Occurrence_Is_Null_When_None_Is_In_The_Lookback(string schedule, string now)
    {
        CohortScheduleParser.GetPreviousOccurrence(schedule, Parse(now)).Should().BeNull();
    }

    private static DateTimeOffset Parse(string value) =>
        DateTimeOffset.Parse(value, CultureInfo.InvariantCulture, DateTimeStyles.AssumeUniversal);
}
