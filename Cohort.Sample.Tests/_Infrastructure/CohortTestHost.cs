using Microsoft.EntityFrameworkCore;

namespace Cohort.Sample.Tests;

/// <summary>
/// One-line spin-up of the Cohort sample stack against a real Postgres instance.
/// Mirrors the role of <c>CompilationHelper</c> in rivet — every end-to-end test
/// calls <see cref="CreateDbContext"/> and gets a fresh <see cref="SampleDbContext"/>
/// pointed at the fixture connection string.
///
/// When Milestone A's sweep engine lands, register it here once and every future
/// e2e test gets it for free.
/// </summary>
public sealed class CohortTestHost(string connectionString) : IDisposable
{
    private readonly DbContextOptions<SampleDbContext> _options = new DbContextOptionsBuilder<SampleDbContext>()
        .UseNpgsql(connectionString)
        .Options;

    public SampleDbContext CreateDbContext() => new(_options);

    public void Dispose() { }
}
