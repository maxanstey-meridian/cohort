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
}
