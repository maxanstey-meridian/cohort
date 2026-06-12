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
    [Theory]
    // 30-day period, no legal min → cutoff is now - 30d
    [InlineData("2026-01-01T00:00:00+00:00", 30, null, "2025-12-02T00:00:00+00:00")]
    // 30-day period, 90-day legal min → legal min dominates
    [InlineData("2026-01-01T00:00:00+00:00", 30, 90, "2025-10-03T00:00:00+00:00")]
    // 90-day period, 30-day legal min → period dominates
    [InlineData("2026-01-01T00:00:00+00:00", 90, 30, "2025-10-03T00:00:00+00:00")]
    public void Compute_Returns_Now_Minus_Greater_Of_Period_And_LegalMin(
        string nowIso,
        int periodDays,
        int? legalMinDays,
        string expectedIso
    )
    {
        var now = DateTimeOffset.Parse(nowIso);
        var period = TimeSpan.FromDays(periodDays);
        TimeSpan? legalMin = legalMinDays is { } d ? TimeSpan.FromDays(d) : null;
        var expected = DateTimeOffset.Parse(expectedIso);

        var cutoff = CutoffCalculator.Compute(now, period, legalMin);

        cutoff.Should().Be(expected);
    }

    [Fact]
    public void Compute_Allows_A_Zero_Period_As_Sweep_Immediately()
    {
        var now = DateTimeOffset.Parse("2026-01-01T00:00:00+00:00");

        var cutoff = CutoffCalculator.Compute(now, TimeSpan.Zero, null);

        cutoff.Should().Be(now);
    }

    [Theory]
    // negative period, no legal min → refuse a future cutoff
    [InlineData(-30, null)]
    // negative period and negative legal min → refuse a future cutoff
    [InlineData(-30, -90)]
    public void Compute_Rejects_Negative_Effective_Periods(int periodDays, int? legalMinDays)
    {
        var now = DateTimeOffset.Parse("2026-01-01T00:00:00+00:00");
        var period = TimeSpan.FromDays(periodDays);
        TimeSpan? legalMin = legalMinDays is { } d ? TimeSpan.FromDays(d) : null;

        var act = () => CutoffCalculator.Compute(now, period, legalMin);

        act.Should().Throw<ArgumentOutOfRangeException>();
    }

    [Fact]
    public void Compute_Uses_A_NonNegative_Legal_Min_To_Rescue_A_Negative_Period()
    {
        // A negative period with a dominating non-negative legal min still yields a
        // valid (past or present) cutoff; the guard only rejects future cutoffs.
        var now = DateTimeOffset.Parse("2026-01-01T00:00:00+00:00");

        var cutoff = CutoffCalculator.Compute(now, TimeSpan.FromDays(-30), TimeSpan.FromDays(90));

        cutoff.Should().Be(now - TimeSpan.FromDays(90));
    }
}
