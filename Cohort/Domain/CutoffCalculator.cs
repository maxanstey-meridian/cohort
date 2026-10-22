namespace Cohort.Domain;

/// <summary>
/// Works out how old a row must be before retention acts on it. A period is counted in the
/// host's calendar: back from the date it is today in <c>timeZone</c>, and the cutoff is the
/// start of the date that lands on. A row anchored on that date or later is kept.
/// </summary>
internal static class CutoffCalculator
{
    /// <summary>
    /// The sweep cutoff: the longer of <paramref name="period"/> and
    /// <paramref name="legalMin"/>, which is whichever reaches the earlier date from today
    /// (one month and 30 days compare differently in different months). A tie resolves to
    /// the period.
    /// </summary>
    public static (DateTimeOffset Cutoff, RetentionPeriod Period) ResolveSweep(
        DateTimeOffset now,
        RetentionPeriod period,
        RetentionPeriod? legalMin,
        TimeZoneInfo timeZone
    )
    {
        var cutoff = Cutoff(now, period, timeZone);
        if (legalMin is not null)
        {
            var legalCutoff = Cutoff(now, legalMin, timeZone);
            if (legalCutoff < cutoff)
            {
                return (legalCutoff, legalMin);
            }
        }

        return (cutoff, period);
    }

    /// <summary>
    /// An erasure ignores the period. Only a non-zero legal minimum holds rows back; without
    /// one there is no cutoff at all.
    /// </summary>
    public static (DateTimeOffset? Cutoff, RetentionPeriod MinimumAge) ResolveErasure(
        DateTimeOffset now,
        RetentionPeriod? legalMin,
        TimeZoneInfo timeZone
    ) =>
        legalMin is { IsZero: false } minimum
            ? (Cutoff(now, minimum, timeZone), minimum)
            : (null, new RetentionPeriod());

    public static DateTimeOffset Cutoff(DateTimeOffset now, RetentionPeriod period, TimeZoneInfo timeZone)
    {
        var today = DateOnly.FromDateTime(TimeZoneInfo.ConvertTime(now, timeZone).DateTime);

        // Days come off before months: that exactly undoes counting forward months first, then
        // days (as PostgreSQL and NodaTime do), and month-end clamping only moves the cutoff
        // earlier, so no row is reached before its anniversary.
        // A period that reaches back past 1 January of year 1 keeps every row.
        if (today.DayNumber - (long)period.Days <= DateOnly.MinValue.DayNumber)
        {
            return DateTimeOffset.MinValue;
        }

        var afterDays = today.AddDays(-period.Days);
        var monthIndex = (afterDays.Year - 1) * 12L + (afterDays.Month - 1) - period.TotalMonths;
        if (monthIndex < 0)
        {
            return DateTimeOffset.MinValue;
        }

        return StartOfDay(afterDays.AddMonths(-period.TotalMonths), timeZone);
    }

    /// <summary>
    /// The first instant of <paramref name="date"/> in <paramref name="timeZone"/>. Where the
    /// clocks go forward at midnight the day starts when they do; where they go back over
    /// midnight it starts at the first of the two midnights.
    /// </summary>
    private static DateTimeOffset StartOfDay(DateOnly date, TimeZoneInfo timeZone)
    {
        var midnight = date.ToDateTime(TimeOnly.MinValue, DateTimeKind.Unspecified);
        if (timeZone.IsInvalidTime(midnight))
        {
            var offsetBefore = timeZone.GetUtcOffset(midnight.AddTicks(-1));
            return new DateTimeOffset(midnight - offsetBefore, TimeSpan.Zero);
        }

        var offset = timeZone.IsAmbiguousTime(midnight)
            ? timeZone.GetAmbiguousTimeOffsets(midnight).Max()
            : timeZone.GetUtcOffset(midnight);
        return new DateTimeOffset(midnight - offset, TimeSpan.Zero);
    }
}
