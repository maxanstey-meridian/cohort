using System.Globalization;

using Cohort.Hosting;

namespace Cohort.Tests;

// Pattern: pure unit test, copied from CohortScheduleParserTests. The occurrence comes from the
// database clock, the one that stamps each run's StartedAt; only the wake time is converted to
// the replica's clock.

public sealed class RetentionWorkerOccurrenceTests
{
    [Theory]
    // Clocks agree.
    [InlineData("2026-10-25T02:30:00+00:00", "2026-10-25T02:30:00+00:00", "2026-10-25T02:00:00+00:00", "2026-10-26T02:00:00+00:00")]
    // Replica an hour ahead: it would already see 03:00 as due; the database hasn't reached it.
    [InlineData("2026-10-25T02:30:00+00:00", "2026-10-25T03:30:00+00:00", "2026-10-25T02:00:00+00:00", "2026-10-26T03:00:00+00:00")]
    // Replica an hour behind: it would still see 01:00; the database has reached 02:00.
    [InlineData("2026-10-25T02:30:00+00:00", "2026-10-25T01:30:00+00:00", "2026-10-25T02:00:00+00:00", "2026-10-26T01:00:00+00:00")]
    public void The_Occurrence_Is_Read_From_The_Database_Clock_And_The_Wake_Converted_To_The_Replicas(
        string databaseNow,
        string replicaNow,
        string expectedOccurrence,
        string expectedWake
    )
    {
        var (due, occurrence, nextWake) = RetentionWorker.PlanOccurrence(
            "0 2 * * *",
            dryRun: false,
            settled: null,
            Parse(databaseNow),
            Parse(replicaNow)
        );

        occurrence.Should().Be(Parse(expectedOccurrence));
        due.Should().Be(Parse(expectedOccurrence));
        nextWake.Should().Be(Parse(expectedWake));
    }

    [Theory]
    // Settled for the same kind of run: nothing is owed.
    [InlineData(false, false, false)]
    // A dry run settled it; the real sweep still owes it.
    [InlineData(true, false, true)]
    public void A_Settled_Occurrence_Is_Owed_Only_To_The_Other_Kind_Of_Run(bool settledDryRun, bool dryRun, bool owed)
    {
        var occurrence = Parse("2026-10-25T02:00:00+00:00");

        var (due, _, _) = RetentionWorker.PlanOccurrence(
            "0 2 * * *",
            dryRun,
            new RetentionWorker.SettledOccurrence(occurrence, settledDryRun),
            Parse("2026-10-25T02:30:00+00:00"),
            Parse("2026-10-25T02:30:00+00:00")
        );

        due.Should().Be(owed ? occurrence : null);
    }

    private static DateTimeOffset Parse(string value) =>
        DateTimeOffset.Parse(value, CultureInfo.InvariantCulture, DateTimeStyles.RoundtripKind);
}
