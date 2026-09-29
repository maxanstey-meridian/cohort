using Cohort.Application;
using Cohort.Domain;
using Cohort.Infrastructure;
using Cohort.Sample.Entities;
using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.DependencyInjection;
using Npgsql;

namespace Cohort.Sample.Tests;

public sealed class RetentionPreviewEndToEndTests(PostgresFixture fixture)
    : IntegrationTestBase(fixture)
{

    [Fact]
    public async Task Preview_Path_Counts_Anonymise_Candidates_Without_Modifying_Rows()
    {
        var tenantA = Guid.NewGuid();
        var tenantB = Guid.NewGuid();
        var asOf = new DateTimeOffset(2026, 4, 12, 12, 0, 0, TimeSpan.Zero);

        await using (var db = Host.CreateDbContext())
        {
            db.AnonymisedContacts.AddRange(
                new AnonymisedContact
                {
                    Id = Guid.NewGuid(),
                    TenantId = tenantA,
                    CreatedAt = asOf.AddDays(-45),
                    EmailAddress = "preview-expired@example.com",
                    GivenName = "Expired",
                    Surname = "Candidate",
                    Notes = "preview-count-target-tenant",
                },
                new AnonymisedContact
                {
                    Id = Guid.NewGuid(),
                    TenantId = tenantA,
                    CreatedAt = asOf.AddDays(-5),
                    EmailAddress = "preview-current@example.com",
                    GivenName = "Current",
                    Surname = "Candidate",
                    Notes = "preview-ignore-current-row",
                },
                new AnonymisedContact
                {
                    Id = Guid.NewGuid(),
                    TenantId = tenantB,
                    CreatedAt = asOf.AddDays(-45),
                    EmailAddress = "preview-other-tenant@example.com",
                    GivenName = "Other",
                    Surname = "Tenant",
                    Notes = "preview-ignore-other-tenant",
                }
            );
            await db.SaveChangesAsync();
        }

        var result = await Host.RunPreviewAsync(
            new TenantContext(tenantA, "uk", new Dictionary<string, string>()),
            asOf
        );

        result
            .Counts.Should()
            .Contain(new EntitySweepCount(typeof(Note), "short-lived", tenantA, Strategy.Purge, 0));
        result
            .Counts.Should()
            .Contain(
                new EntitySweepCount(
                    typeof(SoftDeleteRecord),
                    "soft-delete",
                    tenantA,
                    Strategy.SoftDelete,
                    0
                )
            );
        result
            .Counts.Should()
            .Contain(
                new EntitySweepCount(
                    typeof(AnonymisedContact),
                    "anonymise",
                    tenantA,
                    Strategy.Anonymise,
                    1
                )
            );

        await using var verify = Host.CreateDbContext();
        var contacts = await verify
            .AnonymisedContacts.OrderBy(contact => contact.Notes)
            .Select(contact => new
            {
                contact.EmailAddress,
                contact.GivenName,
                contact.Surname,
                contact.Notes,
            })
            .ToListAsync();

        contacts
            .Should()
            .Equal(
                new
                {
                    EmailAddress = (string?)"preview-expired@example.com",
                    GivenName = "Expired",
                    Surname = "Candidate",
                    Notes = "preview-count-target-tenant",
                },
                new
                {
                    EmailAddress = (string?)"preview-current@example.com",
                    GivenName = "Current",
                    Surname = "Candidate",
                    Notes = "preview-ignore-current-row",
                },
                new
                {
                    EmailAddress = (string?)"preview-other-tenant@example.com",
                    GivenName = "Other",
                    Surname = "Tenant",
                    Notes = "preview-ignore-other-tenant",
                }
            );
    }

    [Fact]
    public async Task Preview_Reports_The_Same_Measured_Held_And_Null_Anchor_Counts_As_Dry_Run()
    {
        var tenantId = Guid.NewGuid();
        var asOf = new DateTimeOffset(2026, 4, 12, 12, 0, 0, TimeSpan.Zero);
        var heldId = Guid.NewGuid();

        await using (var db = Host.CreateDbContext())
        {
            db.NullableAnchorEvents.AddRange(
                new NullableAnchorEvent
                {
                    Id = Guid.NewGuid(),
                    TenantId = tenantId,
                    OccurredAt = asOf.AddDays(-120),
                    Payload = "preview-measured-eligible",
                },
                new NullableAnchorEvent
                {
                    Id = heldId,
                    TenantId = tenantId,
                    OccurredAt = asOf.AddDays(-120),
                    Payload = "preview-measured-held",
                },
                new NullableAnchorEvent
                {
                    Id = Guid.NewGuid(),
                    TenantId = tenantId,
                    OccurredAt = null,
                    Payload = "preview-measured-null-anchor",
                }
            );
            await db.SaveChangesAsync();
        }

        await CreateHoldAsync("nullable_anchor_events", heldId, tenantId, asOf);
        var tenant = new TenantContext(tenantId, "uk", new Dictionary<string, string>());

        var preview = await Host.RunPreviewAsync(tenant, asOf);
        var previewCount = preview.Counts.Single(count =>
            count.EntityType == typeof(NullableAnchorEvent)
        );

        previewCount.Affected.Should().Be(1);
        previewCount.HeldCount.Should().Be(1);
        previewCount.NullAnchorCount.Should().Be(1);
        (await SweepRunExistsAsync(preview.SweepId)).Should().BeFalse();

        RetentionSweepResult? dryRun = null;
        await Host.RunWithServicesAsync(async services =>
        {
            dryRun = await services
                .GetRequiredService<RetentionSweepEngine>()
                .DryRunAsync(
                    tenant,
                    asOf,
                    SweepTriggerKind.Manual,
                    SweepEntityScope.TenantedOnly
                );
        });

        dryRun.Should().NotBeNull();
        var dryRunCount = dryRun!.Counts.Single(count =>
            count.EntityType == typeof(NullableAnchorEvent)
        );
        previewCount.Should().Be(dryRunCount);

        await using var verify = Host.CreateDbContext();
        (
            await verify
                .NullableAnchorEvents.Where(record => record.TenantId == tenantId)
                .OrderBy(record => record.Payload)
                .Select(record => record.Payload)
                .ToListAsync()
        )
            .Should()
            .Equal(
                "preview-measured-eligible",
                "preview-measured-held",
                "preview-measured-null-anchor"
            );
    }

    private async Task CreateHoldAsync(
        string tableName,
        Guid recordId,
        Guid tenantId,
        DateTimeOffset asOf
    )
    {
        await Host.RunWithServicesAsync(async services =>
        {
            var repository = services.GetRequiredService<IRetentionHoldsRepository>();
            await repository.CreateAsync(
                new RetentionHoldRequest(
                    Guid.NewGuid(),
                    RetentionEntityIdentity.ForTable(tableName),
                    recordId.ToString(),
                    tenantId,
                    "preview-hold",
                    asOf.AddDays(-1)
                ),
                CancellationToken.None
            );
        });
    }

    private async Task<bool> SweepRunExistsAsync(Guid sweepId)
    {
        await using var connection = new NpgsqlConnection(GetConnectionString());
        await connection.OpenAsync();
        await using var command = connection.CreateCommand();
        command.CommandText =
            "SELECT EXISTS (SELECT 1 FROM \"sweep_run\" WHERE \"SweepId\" = @sweepId)";
        command.Parameters.AddWithValue("sweepId", sweepId);
        return (bool)(await command.ExecuteScalarAsync())!;
    }

    private sealed class StaticCategoryRepository(
        IReadOnlyDictionary<string, ITestRetentionRule> resolvers
    ) : ITestRetentionRuleProvider
    {
        private static readonly ITestRetentionRule ExemptFallback =
            new StaticTestRetentionRule(
                new RetentionRule(TimeSpan.FromDays(30), Strategy.Exempt)
            );

        public Task<ITestRetentionRule?> GetAsync(string category, CancellationToken ct)
        {
            return resolvers.TryGetValue(category, out var resolver)
                ? Task.FromResult<ITestRetentionRule?>(resolver)
                : Task.FromResult<ITestRetentionRule?>(ExemptFallback);
        }
    }

    private string GetConnectionString()
    {
        using var db = Host.CreateDbContext();
        return db.Database.GetConnectionString()!;
    }
}
