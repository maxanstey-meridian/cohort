using Cohort.Application;
using Cohort.Sample.Entities;

using Microsoft.EntityFrameworkCore;

namespace Cohort.Sample.Tests;

// ─── EXEMPLAR #3 — end-to-end test ──────────────────────────────────────────
//
// Pattern: end-to-end test. THIS IS THE PATTERN.
//
// Feed real data in the front. Run the real code path. Assert what comes out
// the back. Use this whenever the code under test touches a port (DbContext,
// IOptions with real config binding, IHostedService, file/HTTP I/O).
//
// Copy this file. Rename it. Edit the seed and assertions.
//
// Do NOT abstract.
// Do NOT share a base class beyond IntegrationTestBase.
// Do NOT add mocks — NSubstitute is intentionally absent from this project.
//
// When you add a new port `IFoo`, the same PR adds an end-to-end test here that
// exercises the REAL implementation against PostgresFixture. Non-negotiable.
// See CLAUDE.md.
// ────────────────────────────────────────────────────────────────────────────

public sealed class StartupValidationEndToEndTests(PostgresFixture fixture) : IntegrationTestBase(fixture)
{
    [Fact]
    public async Task Startup_Path_Runs_Validation_Before_Returning_Registry_Entries()
    {
        var entries = await Host.RunStartupAsync();

        entries
            .Should()
            .Contain(kvp =>
                kvp.Key == typeof(Note)
                && kvp.Value.Category == "short-lived"
                && kvp.Value.AnchorMember == nameof(Note.CreatedAt)
            );
        entries
            .Should()
            .Contain(kvp =>
                kvp.Key == typeof(SoftDeleteRecord)
                && kvp.Value.Category == "soft-delete"
                && kvp.Value.AnchorMember == nameof(SoftDeleteRecord.CreatedAt)
            );
    }

    [Fact]
    public async Task Startup_Path_Fails_When_Category_Resolution_Is_Misconfigured()
    {
        await using var db = Host.CreateDbContext();
        var connectionString = db.Database.GetConnectionString()!;
        using var host = new CohortTestHost(connectionString, new EmptyCategoryRepository());

        var act = async () => await host.RunStartupAsync();

        var exception = await act.Should().ThrowAsync<RetentionConfigurationException>();
        exception.Which.Errors.Should().HaveCount(2);
        exception
            .Which.Errors.Should()
            .Contain(
                $"Retention category 'short-lived' for entity {typeof(Note).FullName} could not be resolved."
            );
        exception
            .Which.Errors.Should()
            .Contain(
                $"Retention category 'soft-delete' for entity {typeof(SoftDeleteRecord).FullName} could not be resolved."
            );
    }

    private sealed class EmptyCategoryRepository : IRetentionCategoryRepository
    {
        public Task<IRetentionRuleResolver?> GetAsync(string category, CancellationToken ct) =>
            Task.FromResult<IRetentionRuleResolver?>(null);
    }
}
