namespace Cohort.Domain;

/// <summary>
/// A retention period in calendar units, as retention schedules write them: "7 years",
/// "18 months", "30 days". A year is twelve months, so months of twelve or more are carried
/// into years and <c>new RetentionPeriod(Months: 18)</c> equals
/// <c>new RetentionPeriod(Years: 1, Months: 6)</c>. Days are never carried, because a month
/// has no fixed number of days.
/// </summary>
/// <remarks>
/// Cohort counts a period back from the date it is today in the host's
/// <c>CohortOptions.TimeZone</c>: days first, then years and months (the 29th, 30th or 31st
/// becomes the last day of a shorter month). That is the reverse of counting forward from a
/// row's anchor, so a row is kept at least through the whole of its anniversary day.
/// </remarks>
public sealed record RetentionPeriod
{
    public RetentionPeriod(int Years = 0, int Months = 0, int Days = 0)
    {
        RequireNonNegative(Years, nameof(Years));
        RequireNonNegative(Months, nameof(Months));
        RequireNonNegative(Days, nameof(Days));

        // Stored as PostgreSQL stores an interval: a 32-bit month count and a 32-bit day count.
        var totalMonths = (long)Years * 12 + Months;
        if (totalMonths > int.MaxValue)
        {
            throw new ArgumentOutOfRangeException(
                nameof(Years),
                Years,
                "A retention period cannot exceed int.MaxValue months."
            );
        }

        this.Years = (int)(totalMonths / 12);
        this.Months = (int)(totalMonths % 12);
        this.Days = Days;
    }

    public int Years { get; }

    /// <summary>Months beyond whole years, 0 to 11.</summary>
    public int Months { get; }

    public int Days { get; }

    internal int TotalMonths => Years * 12 + Months;

    internal bool IsZero => TotalMonths == 0 && Days == 0;

    private static void RequireNonNegative(int value, string paramName)
    {
        if (value < 0)
        {
            throw new ArgumentOutOfRangeException(
                paramName,
                value,
                $"{paramName} must be non-negative. A negative retention period computes a cutoff in the future, which would treat every row — including rows that are not yet expired — as eligible for the sweep."
            );
        }
    }
}
