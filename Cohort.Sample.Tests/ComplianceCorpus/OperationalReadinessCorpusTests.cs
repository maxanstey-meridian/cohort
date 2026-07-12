using Cohort.Application;
using Cohort.Domain;
using Cohort.Hosting;
using Cohort.Sample.Entities;
using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.DependencyInjection;
using Npgsql;

namespace Cohort.Sample.Tests.ComplianceCorpus;

public sealed class OperationalReadinessCorpusTests(PostgresFixture fixture)
    : IntegrationTestBase(fixture)
{
    [Fact]
    public async Task Public_operation_validates_and_runs_against_the_installed_postgresql_schema()
    {
        var tenantId = Guid.NewGuid();
        var now = new DateTimeOffset(2026, 7, 12, 12, 0, 0, TimeSpan.Zero);
        var noteId = Guid.NewGuid();

        await using (var db = Host.CreateDbContext())
        {
            db.Notes.Add(new Note { Id = noteId, TenantId = tenantId, CreatedAt = now.AddDays(-60), Body = "ready" });
            await db.SaveChangesAsync();
        }

        var result = await Host.RunWithServicesAsync(async services =>
        {
            return await services.GetRequiredService<IRetentionSweep>().SweepAsync(
                new TenantContext(tenantId, "uk", new Dictionary<string, string>()),
                now
            );
        });

        result.EntityFailures.Should().BeEmpty();
        await using var connection = new NpgsqlConnection(ConnectionString);
        await connection.OpenAsync();
        await using var command = connection.CreateCommand();
        command.CommandText = "SELECT COUNT(*) FROM \"sweep_run\" WHERE \"SweepId\" = @sweepId AND \"Status\" = 1";
        command.Parameters.AddWithValue("sweepId", result.SweepId);
        ((long)(await command.ExecuteScalarAsync())!).Should().Be(1);
        await using var verify = Host.CreateDbContext();
        (await verify.Notes.AnyAsync(note => note.Id == noteId)).Should().BeFalse();
    }

    [Fact]
    public async Task Missing_provider_fails_hosted_startup_before_mutation()
    {
        var builder = Microsoft.Extensions.Hosting.Host.CreateApplicationBuilder();
        builder.Services.AddDbContext<UnsupportedProviderDbContext>();
        builder.Services.AddSingleton<IRetentionRuleProvider>(
            new UnsupportedProviderCategoryRepository()
        );
        builder.Services.AddCohort<UnsupportedProviderDbContext>();
        using var host = builder.Build();

        var act = () => host.StartAsync();

        await act.Should()
            .ThrowAsync<RetentionConfigurationException>()
            .WithMessage("*requires the Npgsql Entity Framework Core provider*<unknown>*");
    }

    private sealed class UnsupportedProviderDbContext(
        DbContextOptions<UnsupportedProviderDbContext> options
    ) : DbContext(options);

    private sealed class UnsupportedProviderCategoryRepository : ITestRetentionRuleProvider
    {
        public Task<ITestRetentionRule?> GetAsync(string category, CancellationToken ct)
        {
            return Task.FromResult<ITestRetentionRule?>(null);
        }
    }
}
