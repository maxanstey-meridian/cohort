using Cohort.Domain;

namespace Cohort.Tests;

// ─── EXEMPLAR #1 — pure unit test ───────────────────────────────────────────
//
// Pattern: pure unit test. Use ONLY when the code under test is a static
// function with no I/O, no DbContext, no time source beyond parameters, no
// randomness. `[Theory]` + `[InlineData]` rows.
//
// If you find yourself writing `Substitute.For<...>` you are in the wrong file.
// Move to `Cohort.Sample.Tests` and write an end-to-end test instead. NSubstitute
// is intentionally absent from this project — see CLAUDE.md.
//
// No async. No fixtures. No DI. No `IClock` abstraction — never invent an
// abstraction to test a pure function. Keep it boring.
// ────────────────────────────────────────────────────────────────────────────

public sealed class CutoffCalculatorTests
{
    private static readonly TimeZoneInfo London = TimeZoneInfo.FindSystemTimeZoneById("Europe/London");

    [Theory]
    // 30 days back from 1 Jan → start of 2 Dec. A row anchored on 2 Dec is kept through 1 Jan.
    [InlineData("2026-01-01T12:00:00+00:00", 0, 0, 30, "2025-12-02T00:00:00+00:00")]
    // Seven calendar years from 1 Mar 2034 reach back to 1 Mar 2027, across two leap days.
    // As 2,555 days the cutoff was 4 Mar 2027 (that is, rows went from 27 Feb 2034).
    [InlineData("2034-03-01T12:00:00+00:00", 7, 0, 0, "2027-03-01T00:00:00+00:00")]
    // From 28 Feb 2025, one year back is 28 Feb 2024, not the leap day after it.
    [InlineData("2025-02-28T12:00:00+00:00", 1, 0, 0, "2024-02-28T00:00:00+00:00")]
    // From 29 Feb 2028 one year back is 28 Feb 2027, which has no 29th.
    [InlineData("2028-02-29T12:00:00+00:00", 1, 0, 0, "2027-02-28T00:00:00+00:00")]
    // Month ends: one month back from 31 Mar is the last day of February, and from 30 Apr
    // it is 30 Mar (not 31 Mar).
    [InlineData("2027-03-31T12:00:00+00:00", 0, 1, 0, "2027-02-28T00:00:00+00:00")]
    [InlineData("2028-03-31T12:00:00+00:00", 0, 1, 0, "2028-02-29T00:00:00+00:00")]
    [InlineData("2027-04-30T12:00:00+00:00", 0, 1, 0, "2027-03-30T00:00:00+00:00")]
    // Days before months: 1 month 1 day back from 1 Mar is 28 Feb, then 28 Jan. A row anchored
    // on 28 Jan reaches its anniversary (28 Feb plus a day) on 1 Mar, so it is still kept.
    [InlineData("2027-03-01T12:00:00+00:00", 0, 1, 1, "2027-01-28T00:00:00+00:00")]
    // A row anchored on 3 Jan with 1 month 28 days reaches its anniversary on 3 Mar (3 Feb plus
    // 28 days): kept through 3 Mar, reached on 4 Mar. Months first would reach it on 1 Mar.
    [InlineData("2027-03-01T12:00:00+00:00", 0, 1, 28, "2027-01-01T00:00:00+00:00")]
    [InlineData("2027-03-03T12:00:00+00:00", 0, 1, 28, "2027-01-03T00:00:00+00:00")]
    [InlineData("2027-03-04T12:00:00+00:00", 0, 1, 28, "2027-01-04T00:00:00+00:00")]
    // 18 months is one year and six months.
    [InlineData("2027-08-31T12:00:00+00:00", 0, 18, 0, "2026-02-28T00:00:00+00:00")]
    // Zero keeps a row for the rest of its own day.
    [InlineData("2026-01-01T12:00:00+00:00", 0, 0, 0, "2026-01-01T00:00:00+00:00")]
    public void Cutoff_Counts_Calendar_Years_Months_And_Days_Back_To_The_Start_Of_That_Day(
        string nowIso,
        int years,
        int months,
        int days,
        string expectedIso
    )
    {
        var cutoff = CutoffCalculator.Cutoff(
            DateTimeOffset.Parse(nowIso),
            new RetentionPeriod(years, months, days),
            TimeZoneInfo.Utc
        );

        cutoff.Should().Be(DateTimeOffset.Parse(expectedIso));
    }

    [Theory]
    // 00:30 BST on 2 Jul is still 1 Jul in UTC; in London it is 2 Jul, so one year back is
    // 2 Jul 2025, which starts at 23:00 UTC on 1 Jul.
    [InlineData("2026-07-01T23:30:00+00:00", 1, 0, 0, "2025-07-01T23:00:00+00:00")]
    // Counting from GMT (2 Nov) back into BST (2 Oct): the day starts at 23:00 UTC.
    [InlineData("2026-11-02T12:00:00+00:00", 0, 1, 0, "2026-10-01T23:00:00+00:00")]
    // Counting from BST back into GMT: the day starts at midnight UTC.
    [InlineData("2027-04-02T12:00:00+00:00", 0, 2, 0, "2027-02-02T00:00:00+00:00")]
    // Both clock-change days: 25 Oct 2026 (25 hours) and 28 Mar 2027 (23 hours) start at
    // midnight local time, BST and GMT respectively.
    [InlineData("2026-10-26T00:30:00+00:00", 0, 0, 1, "2026-10-24T23:00:00+00:00")]
    [InlineData("2027-03-29T12:00:00+00:00", 0, 0, 1, "2027-03-28T00:00:00+00:00")]
    public void Cutoff_Counts_Days_In_The_Host_Calendar_Across_Clock_Changes(
        string nowIso,
        int years,
        int months,
        int days,
        string expectedIso
    )
    {
        var cutoff = CutoffCalculator.Cutoff(
            DateTimeOffset.Parse(nowIso),
            new RetentionPeriod(years, months, days),
            London
        );

        cutoff.Should().Be(DateTimeOffset.Parse(expectedIso));
    }

    [Theory]
    // Santiago's clocks go forward at midnight on 6 Sep 2026: the day starts at 01:00 -03.
    [InlineData("America/Santiago", "2026-09-07T12:00:00+00:00", "2026-09-06T04:00:00+00:00")]
    // Havana's go back from 01:00 to 00:00 on 1 Nov 2026: the day starts at the first
    // midnight, 00:00 -04.
    [InlineData("America/Havana", "2026-11-02T12:00:00+00:00", "2026-11-01T04:00:00+00:00")]
    public void Cutoff_Starts_A_Day_Whose_Midnight_Is_Skipped_Or_Repeated_At_Its_First_Instant(
        string timeZoneId,
        string nowIso,
        string expectedIso
    )
    {
        var cutoff = CutoffCalculator.Cutoff(
            DateTimeOffset.Parse(nowIso),
            new RetentionPeriod(Days: 1),
            TimeZoneInfo.FindSystemTimeZoneById(timeZoneId)
        );

        cutoff.Should().Be(DateTimeOffset.Parse(expectedIso));
    }

    [Theory]
    // 30-day period, no legal min → the period sets the cutoff.
    [InlineData("2026-01-01T12:00:00+00:00", 30, null, "2025-12-02T00:00:00+00:00", 30)]
    // 30-day period, 90-day legal min → the legal min reaches further back.
    [InlineData("2026-01-01T12:00:00+00:00", 30, 90, "2025-10-03T00:00:00+00:00", 90)]
    // 90-day period, 30-day legal min → the period reaches further back.
    [InlineData("2026-01-01T12:00:00+00:00", 90, 30, "2025-10-03T00:00:00+00:00", 90)]
    // Equal → the period.
    [InlineData("2026-01-01T12:00:00+00:00", 30, 30, "2025-12-02T00:00:00+00:00", 30)]
    public void ResolveSweep_Uses_Whichever_Of_Period_And_LegalMin_Reaches_Further_Back(
        string nowIso,
        int periodDays,
        int? legalMinDays,
        string expectedIso,
        int expectedResolvedDays
    )
    {
        var (cutoff, resolved) = CutoffCalculator.ResolveSweep(
            DateTimeOffset.Parse(nowIso),
            new RetentionPeriod(Days: periodDays),
            legalMinDays is { } d ? new RetentionPeriod(Days: d) : null,
            TimeZoneInfo.Utc
        );

        cutoff.Should().Be(DateTimeOffset.Parse(expectedIso));
        resolved.Should().Be(new RetentionPeriod(Days: expectedResolvedDays));
    }

    [Theory]
    // On 1 Mar one month reaches back 28 days, so a 30-day legal minimum is longer…
    [InlineData("2027-03-01T12:00:00+00:00", "2027-01-30T00:00:00+00:00", false)]
    // …and on 31 Mar one month reaches back 31 days, so the month is longer.
    [InlineData("2027-03-31T12:00:00+00:00", "2027-02-28T00:00:00+00:00", true)]
    public void ResolveSweep_Compares_Months_And_Days_On_The_Day_Of_The_Run(
        string nowIso,
        string expectedIso,
        bool periodWins
    )
    {
        var period = new RetentionPeriod(Months: 1);
        var legalMin = new RetentionPeriod(Days: 30);

        var (cutoff, resolved) = CutoffCalculator.ResolveSweep(
            DateTimeOffset.Parse(nowIso),
            period,
            legalMin,
            TimeZoneInfo.Utc
        );

        cutoff.Should().Be(DateTimeOffset.Parse(expectedIso));
        resolved.Should().Be(periodWins ? period : legalMin);
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public void ResolveSweep_Saturates_When_A_Period_Reaches_Back_Past_The_Minimum_Date(
        bool useLegalMinimum
    )
    {
        var huge = new RetentionPeriod(Years: 10_000);

        var (cutoff, _) = CutoffCalculator.ResolveSweep(
            DateTimeOffset.Parse("2026-01-01T00:00:00+00:00"),
            useLegalMinimum ? new RetentionPeriod() : huge,
            useLegalMinimum ? huge : null,
            London
        );

        cutoff.Should().Be(DateTimeOffset.MinValue);
    }

    [Theory]
    [InlineData(null, null)]
    [InlineData(0, null)]
    [InlineData(90, "2025-10-03T00:00:00+00:00")]
    public void ResolveErasure_Returns_A_Cutoff_Only_For_A_NonZero_LegalMin(
        int? legalMinDays,
        string? expectedIso
    )
    {
        var (cutoff, minimumAge) = CutoffCalculator.ResolveErasure(
            DateTimeOffset.Parse("2026-01-01T12:00:00+00:00"),
            legalMinDays is { } days ? new RetentionPeriod(Days: days) : null,
            TimeZoneInfo.Utc
        );

        cutoff.Should().Be(expectedIso is null ? null : DateTimeOffset.Parse(expectedIso));
        minimumAge.Should().Be(new RetentionPeriod(Days: expectedIso is null ? 0 : legalMinDays!.Value));
    }

    [Fact]
    public void ResolveErasure_Saturates_When_LegalMin_Reaches_Back_Past_The_Minimum_Date()
    {
        var (cutoff, _) = CutoffCalculator.ResolveErasure(
            DateTimeOffset.Parse("2026-01-01T00:00:00+00:00"),
            new RetentionPeriod(Days: int.MaxValue),
            TimeZoneInfo.Utc
        );

        cutoff.Should().Be(DateTimeOffset.MinValue);
    }
}
