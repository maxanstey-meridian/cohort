using System.Data;
using Cohort.Application;
using Cohort.Domain;
using Cohort.Hosting;
using Cohort.Infrastructure;
using Cohort.Infrastructure.Migrations;
using Cohort.Sample.Entities;
using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Npgsql;

namespace Cohort.Sample.Tests;

public sealed class AnonymiseSweepEndToEndTests(PostgresFixture fixture)
    : IntegrationTestBase(fixture)
{
    [Fact]
    public async Task Erasure_Waits_For_A_Matching_Row_Locked_By_Another_Transaction()
    {
        var tenantId = Guid.NewGuid();
        var subjectId = Guid.NewGuid();
        var contactId = Guid.NewGuid();
        var asOf = new DateTimeOffset(2026, 7, 11, 12, 0, 0, TimeSpan.Zero);

        await using (var db = Host.CreateDbContext())
        {
            db.AnonymisedContacts.Add(
                new AnonymisedContact
                {
                    Id = contactId,
                    TenantId = tenantId,
                    SubjectId = subjectId,
                    CreatedAt = asOf.AddDays(-120),
                    EmailAddress = "locked-erasure@example.com",
                    GivenName = "Locked",
                    Surname = "Erasure",
                    Notes = "locked-erasure",
                }
            );
            await db.SaveChangesAsync();
        }

        await using var blocker = new NpgsqlConnection(GetConnectionString());
        await blocker.OpenAsync();
        await using var blockerTransaction = await blocker.BeginTransactionAsync();
        await using (var lockCommand = blocker.CreateCommand())
        {
            lockCommand.Transaction = blockerTransaction;
            lockCommand.CommandText = """
                SELECT "Id" FROM "anonymised_contacts" WHERE "Id" = @id FOR UPDATE
                """;
            lockCommand.Parameters.AddWithValue("id", contactId);
            await lockCommand.ExecuteNonQueryAsync();
        }

        var erasure = Host.RunErasureAsync(
            new TenantContext(tenantId, "uk", new Dictionary<string, string>()),
            new ErasureScope(subjectId, allowSoftDeleteAsErasure: true),
            asOf
        );
        await WaitForBlockedRowMutationAsync(blocker.ProcessID);

        await blockerTransaction.CommitAsync();
        var result = await erasure.WaitAsync(TimeSpan.FromSeconds(10));

        result.EntityFailures.Should().BeEmpty();
        result
            .Counts.Should()
            .Contain(count => count.EntityType == typeof(AnonymisedContact) && count.Affected == 1);
        await using var verify = Host.CreateDbContext();
        var contact = await verify.AnonymisedContacts.SingleAsync(candidate => candidate.Id == contactId);
        contact.AnonymisedAt.Should().Be(asOf);
    }

    [Fact]
    public async Task Handler_Aware_Erasure_Rechecks_Subject_After_Waiting_For_The_Row()
    {
        var tenantId = Guid.NewGuid();
        var originalSubjectId = Guid.NewGuid();
        var replacementSubjectId = Guid.NewGuid();
        var contactId = Guid.NewGuid();
        var asOf = new DateTimeOffset(2026, 7, 11, 12, 0, 0, TimeSpan.Zero);

        await using (var db = Host.CreateDbContext())
        {
            db.AnonymisedContacts.Add(
                new AnonymisedContact
                {
                    Id = contactId,
                    TenantId = tenantId,
                    SubjectId = originalSubjectId,
                    CreatedAt = asOf.AddDays(-120),
                    EmailAddress = "subject-race@example.com",
                    GivenName = "Subject",
                    Surname = "Race",
                    Notes = "must survive",
                }
            );
            await db.SaveChangesAsync();
        }

        using var handlerHost = new CohortTestHost(
            GetConnectionString(),
            configureServices: services =>
                services.AddRowHandler<AnonymisedContact, SubjectRaceContactHandler>()
        );
        await using var blocker = new NpgsqlConnection(GetConnectionString());
        await blocker.OpenAsync();
        await using var blockerTransaction = await blocker.BeginTransactionAsync();
        await using (var lockCommand = blocker.CreateCommand())
        {
            lockCommand.Transaction = blockerTransaction;
            lockCommand.CommandText =
                "SELECT \"Id\" FROM \"anonymised_contacts\" WHERE \"Id\" = @id FOR UPDATE";
            lockCommand.Parameters.AddWithValue("id", contactId);
            await lockCommand.ExecuteNonQueryAsync();
        }

        var erasure = handlerHost.RunErasureAsync(
            new TenantContext(tenantId, "uk", new Dictionary<string, string>()),
            new ErasureScope(originalSubjectId, allowSoftDeleteAsErasure: true),
            asOf
        );
        await WaitForBlockedRowMutationAsync(blocker.ProcessID);

        await using (var changeSubject = blocker.CreateCommand())
        {
            changeSubject.Transaction = blockerTransaction;
            changeSubject.CommandText =
                "UPDATE \"anonymised_contacts\" SET \"SubjectId\" = @subjectId WHERE \"Id\" = @id";
            changeSubject.Parameters.AddWithValue("subjectId", replacementSubjectId);
            changeSubject.Parameters.AddWithValue("id", contactId);
            await changeSubject.ExecuteNonQueryAsync();
        }
        await blockerTransaction.CommitAsync();

        var result = await erasure.WaitAsync(TimeSpan.FromSeconds(10));

        result.EntityFailures.Should().BeEmpty();
        result
            .Counts.Should()
            .Contain(count => count.EntityType == typeof(AnonymisedContact) && count.Affected == 0);
        await using var verify = Host.CreateDbContext();
        var contact = await verify.AnonymisedContacts.SingleAsync(candidate => candidate.Id == contactId);
        contact.SubjectId.Should().Be(replacementSubjectId);
        contact.AnonymisedAt.Should().BeNull();
        contact.EmailAddress.Should().Be("subject-race@example.com");
    }

    private async Task WaitForBlockedRowMutationAsync(int blockerBackendId)
    {
        using var timeout = new CancellationTokenSource(TimeSpan.FromSeconds(10));
        await using var connection = new NpgsqlConnection(GetConnectionString());
        await connection.OpenAsync(timeout.Token);

        while (true)
        {
            await using var command = connection.CreateCommand();
            command.CommandText = """
                SELECT EXISTS (
                    SELECT 1
                    FROM pg_stat_activity waiter
                    WHERE @blockerBackendId = ANY(pg_blocking_pids(waiter.pid))
                      AND waiter.query LIKE '%anonymised_contacts%'
                      AND waiter.query LIKE '%UPDATE%'
                )
                """;
            command.Parameters.AddWithValue("blockerBackendId", blockerBackendId);
            if ((bool)(await command.ExecuteScalarAsync(timeout.Token))!)
            {
                return;
            }

            await Task.Delay(TimeSpan.FromMilliseconds(20), timeout.Token);
        }
    }

    [Fact]
    public async Task Sweep_Path_Fills_A_Batch_Past_Oldest_Rows_Locked_By_Another_Transaction()
    {
        var tenantId = Guid.NewGuid();
        var asOf = new DateTimeOffset(2026, 7, 11, 12, 0, 0, TimeSpan.Zero);
        var ids = Enumerable.Range(0, 4).Select(_ => Guid.NewGuid()).ToArray();

        await using (var db = Host.CreateDbContext())
        {
            db.AnonymisedContacts.AddRange(
                ids.Select((id, index) => new AnonymisedContact
                {
                    Id = id,
                    TenantId = tenantId,
                    CreatedAt = asOf.AddDays(-120).AddMinutes(index),
                    EmailAddress = $"locked-{index}@example.com",
                    GivenName = $"Locked {index}",
                    Surname = "Prefix",
                    Notes = $"locked-anonymise-{index}",
                })
            );
            await db.SaveChangesAsync();
        }

        await using var blocker = new NpgsqlConnection(GetConnectionString());
        await blocker.OpenAsync();
        await using var blockerTransaction = await blocker.BeginTransactionAsync();
        await using (var lockCommand = blocker.CreateCommand())
        {
            lockCommand.Transaction = blockerTransaction;
            lockCommand.CommandText = """
                SELECT "Id"
                FROM "anonymised_contacts"
                WHERE "Id" = ANY(@ids)
                FOR UPDATE
                """;
            lockCommand.Parameters.AddWithValue("ids", ids[..2]);
            await lockCommand.ExecuteNonQueryAsync();
        }

        using var host = new CohortTestHost(
            GetConnectionString(),
            configurationOverrides: new Dictionary<string, string?>
            {
                [$"{CohortOptions.SectionName}:SweepBatchSize"] = "2",
            }
        );
        using var timeout = new CancellationTokenSource(TimeSpan.FromSeconds(10));
        var result = await host.RunSweepAsync(
            new TenantContext(tenantId, "uk", new Dictionary<string, string>()),
            asOf,
            timeout.Token
        );

        result.EntityFailures.Should().BeEmpty();
        result
            .Counts.Should()
            .Contain(count => count.EntityType == typeof(AnonymisedContact) && count.Affected == 2);
        await blockerTransaction.CommitAsync();

        await using var verify = Host.CreateDbContext();
        var contacts = await verify
            .AnonymisedContacts.Where(contact => contact.TenantId == tenantId)
            .OrderBy(contact => contact.CreatedAt)
            .ToListAsync();
        contacts[..2].Should().OnlyContain(contact => contact.AnonymisedAt == null);
        contacts[2..].Should().OnlyContain(contact => contact.AnonymisedAt == asOf);
    }

    [Fact]
    public async Task Sweep_Path_Anonymises_Only_Expired_Rows_For_The_Target_Tenant_And_Leaves_Unmarked_Columns_Unchanged()
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
                    CreatedAt = asOf.AddDays(-120),
                    EmailAddress = "expired@example.com",
                    GivenName = "Alice",
                    Surname = "Smith",
                    Notes = "keep-expired-notes",
                },
                new AnonymisedContact
                {
                    Id = Guid.NewGuid(),
                    TenantId = tenantA,
                    CreatedAt = asOf.AddDays(-5),
                    EmailAddress = "newer@example.com",
                    GivenName = "Bob",
                    Surname = "Jones",
                    Notes = "keep-newer-notes",
                },
                new AnonymisedContact
                {
                    Id = Guid.NewGuid(),
                    TenantId = tenantB,
                    CreatedAt = asOf.AddDays(-120),
                    EmailAddress = "other-tenant@example.com",
                    GivenName = "Cara",
                    Surname = "Mills",
                    Notes = "keep-other-tenant-notes",
                }
            );
            await db.SaveChangesAsync();
        }

        var result = await Host.RunSweepAsync(
            new TenantContext(tenantA, "uk", new Dictionary<string, string>()),
            asOf
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
                    EmailAddress = (string?)null,
                    GivenName = string.Empty,
                    Surname = "[redacted]",
                    Notes = "keep-expired-notes",
                },
                new
                {
                    EmailAddress = (string?)"newer@example.com",
                    GivenName = "Bob",
                    Surname = "Jones",
                    Notes = "keep-newer-notes",
                },
                new
                {
                    EmailAddress = (string?)"other-tenant@example.com",
                    GivenName = "Cara",
                    Surname = "Mills",
                    Notes = "keep-other-tenant-notes",
                }
            );
    }

    [Fact]
    public async Task Validation_Fails_When_An_Anonymise_Category_Has_No_Annotated_Fields()
    {
        await using var db = Host.CreateDbContext();
        var connectionString = db.Database.GetConnectionString()!;

        using var host = new CohortTestHost(
            connectionString,
            new StaticCategoryRepository(
                new Dictionary<string, IRetentionRuleResolver>
                {
                    ["short-lived"] = new StaticRetentionRuleResolver(
                        new RetentionRule(TimeSpan.FromDays(30), Strategy.Anonymise)
                    ),
                    ["soft-delete"] = new StaticRetentionRuleResolver(
                        new RetentionRule(TimeSpan.FromDays(30), Strategy.SoftDelete)
                    ),
                    ["anonymise"] = new StaticRetentionRuleResolver(
                        new RetentionRule(TimeSpan.FromDays(30), Strategy.Anonymise)
                    ),
                }
            )
        );

        var act = () => host.ValidateAndScanAsync();

        var exception = await act.Should().ThrowAsync<RetentionConfigurationException>();
        exception.Which.Errors.Should().HaveCount(2);
        exception
            .Which.Errors.Should()
            .Contain(
                $"Anonymise convention on {typeof(Note).FullName}: retained Anonymise categories require at least one [Anonymise]-annotated property mapped by EF."
            );
        exception
            .Which.Errors.Should()
            .Contain(
                $"Anonymise convention on {typeof(Note).FullName}: retained Anonymise categories require a nullable DateTimeOffset marker property (named AnonymisedAt by convention, or marked with [RetentionAnonymisedAt]). NULL marks rows not yet anonymised; without it anonymisation re-scrubs every expired row on every sweep."
            );
    }

    [Fact]
    public async Task Sweep_Path_Can_Run_Twice_Without_Reintroducing_Scrubbed_Data()
    {
        var tenantId = Guid.NewGuid();
        var asOf = new DateTimeOffset(2026, 4, 12, 12, 0, 0, TimeSpan.Zero);

        await using (var db = Host.CreateDbContext())
        {
            db.AnonymisedContacts.Add(
                new AnonymisedContact
                {
                    Id = Guid.NewGuid(),
                    TenantId = tenantId,
                    CreatedAt = asOf.AddDays(-120),
                    EmailAddress = "repeat@example.com",
                    GivenName = "Repeat",
                    Surname = "Target",
                    Notes = "repeat-notes",
                }
            );
            await db.SaveChangesAsync();
        }

        var tenant = new TenantContext(tenantId, "uk", new Dictionary<string, string>());

        var first = await Host.RunSweepAsync(tenant, asOf);
        var second = await Host.RunSweepAsync(tenant, asOf);

        first
            .Counts.Should()
            .Contain(
                new EntitySweepCount(
                    typeof(AnonymisedContact),
                    "anonymise",
                    tenantId,
                    Strategy.Anonymise,
                    1
                )
            );
        // AnonymisedAt makes anonymisation idempotent: rows scrubbed by the first
        // sweep are stamped and fall out of the second sweep's candidate filter.
        second
            .Counts.Should()
            .Contain(count =>
                count.EntityType == typeof(AnonymisedContact)
                && count.Category == "anonymise"
                && count.TenantId == tenantId
                && count.Strategy == Strategy.Anonymise
                && count.Affected == 0
            );

        await using var verify = Host.CreateDbContext();
        var contact = await verify.AnonymisedContacts.SingleAsync();

        contact.EmailAddress.Should().BeNull();
        contact.GivenName.Should().BeEmpty();
        contact.Surname.Should().Be("[redacted]");
        contact.Notes.Should().Be("repeat-notes");
        contact.AnonymisedAt.Should().Be(asOf);
    }

    [Fact]
    public async Task Sweep_Path_Does_Not_Anonymise_Rows_Exactly_On_The_Cutoff()
    {
        var tenantId = Guid.NewGuid();
        var asOf = new DateTimeOffset(2026, 4, 12, 12, 0, 0, TimeSpan.Zero);

        await using (var db = Host.CreateDbContext())
        {
            db.AnonymisedContacts.AddRange(
                new AnonymisedContact
                {
                    Id = Guid.NewGuid(),
                    TenantId = tenantId,
                    CreatedAt = asOf.AddDays(-31),
                    EmailAddress = "expired-boundary@example.com",
                    GivenName = "Expired",
                    Surname = "Contact",
                    Notes = "expired-before-cutoff",
                },
                new AnonymisedContact
                {
                    Id = Guid.NewGuid(),
                    TenantId = tenantId,
                    CreatedAt = asOf.AddDays(-30),
                    EmailAddress = "boundary@example.com",
                    GivenName = "Boundary",
                    Surname = "Contact",
                    Notes = "exact-cutoff-boundary",
                }
            );
            await db.SaveChangesAsync();
        }

        var result = await Host.RunSweepAsync(
            new TenantContext(tenantId, "uk", new Dictionary<string, string>()),
            asOf
        );

        result
            .Counts.Should()
            .Contain(
                new EntitySweepCount(
                    typeof(AnonymisedContact),
                    "anonymise",
                    tenantId,
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
                    EmailAddress = (string?)"boundary@example.com",
                    GivenName = "Boundary",
                    Surname = "Contact",
                    Notes = "exact-cutoff-boundary",
                },
                new
                {
                    EmailAddress = (string?)null,
                    GivenName = string.Empty,
                    Surname = "[redacted]",
                    Notes = "expired-before-cutoff",
                }
            );
    }

    [Fact]
    public async Task Sweep_Path_Uses_A_SetBased_Update_For_Factories_That_Do_Not_Require_Original_Values()
    {
        await using var database = await TemporaryDatabase.CreateAsync(GetConnectionString());
        await using var services = BuildFactoryBackedSweepServiceProvider(
            database.ConnectionString
        );
        var tenantId = Guid.NewGuid();
        var otherTenantId = Guid.NewGuid();
        var asOf = new DateTimeOffset(2026, 4, 12, 12, 0, 0, TimeSpan.Zero);

        await using (var scope = services.CreateAsyncScope())
        {
            var db = scope.ServiceProvider.GetRequiredService<FactoryBackedSweepDbContext>();
            await db.Database.EnsureCreatedAsync();
            db.SetBasedFactorySweepRecords.AddRange(
                new SetBasedFactorySweepRecord
                {
                    Id = Guid.NewGuid(),
                    TenantId = tenantId,
                    CreatedAt = asOf.AddDays(-120),
                    ExternalId = Guid.NewGuid(),
                    DisplayName = "expired-a",
                },
                new SetBasedFactorySweepRecord
                {
                    Id = Guid.NewGuid(),
                    TenantId = tenantId,
                    CreatedAt = asOf.AddDays(-90),
                    ExternalId = Guid.NewGuid(),
                    DisplayName = "expired-b",
                },
                new SetBasedFactorySweepRecord
                {
                    Id = Guid.NewGuid(),
                    TenantId = tenantId,
                    CreatedAt = asOf.AddDays(-5),
                    ExternalId = Guid.NewGuid(),
                    DisplayName = "fresh",
                },
                new SetBasedFactorySweepRecord
                {
                    Id = Guid.NewGuid(),
                    TenantId = otherTenantId,
                    CreatedAt = asOf.AddDays(-120),
                    ExternalId = Guid.NewGuid(),
                    DisplayName = "other-tenant",
                }
            );
            await db.SaveChangesAsync();
        }

        RetentionSweepResult result;
        await using (var scope = services.CreateAsyncScope())
        {
            var engine = scope.ServiceProvider.GetRequiredService<RetentionSweepEngine>();
            result = await engine.SweepAsync(
                new TenantContext(tenantId, "uk", new Dictionary<string, string>()),
                asOf,
                SweepTriggerKind.Manual,
                SweepEntityScope.TenantedOnly
            );
        }

        result
            .Counts.Should()
            .Contain(
                new EntitySweepCount(
                    typeof(SetBasedFactorySweepRecord),
                    "factory-backed-set-based",
                    tenantId,
                    Strategy.Anonymise,
                    2
                )
            );

        await using (var scope = services.CreateAsyncScope())
        {
            var db = scope.ServiceProvider.GetRequiredService<FactoryBackedSweepDbContext>();
            var records = await db
                .SetBasedFactorySweepRecords.OrderBy(record => record.DisplayName)
                .ToListAsync();
            var factory = scope.ServiceProvider.GetRequiredService<SetBasedGuidFactory>();

            records
                .Single(record => record.DisplayName == "expired-a")
                .ExternalId.Should()
                .Be(SetBasedGuidFactory.ScrubbedValue);
            records
                .Single(record => record.DisplayName == "expired-b")
                .ExternalId.Should()
                .Be(SetBasedGuidFactory.ScrubbedValue);
            records
                .Single(record => record.DisplayName == "fresh")
                .ExternalId.Should()
                .NotBe(SetBasedGuidFactory.ScrubbedValue);
            records
                .Single(record => record.DisplayName == "other-tenant")
                .ExternalId.Should()
                .NotBe(SetBasedGuidFactory.ScrubbedValue);

            factory.Contexts.Should().ContainSingle();
            factory.Contexts[0].OriginalValue.Should().BeNull();
            factory.Contexts[0].TenantId.Should().Be(tenantId);
            factory
                .Contexts[0]
                .MemberName.Should()
                .Be(nameof(SetBasedFactorySweepRecord.ExternalId));
        }
    }

    [Fact]
    public async Task Sweep_Path_Uses_PerRow_Execution_For_Factories_That_Require_Original_Values_And_Respects_Holds()
    {
        await using var database = await TemporaryDatabase.CreateAsync(GetConnectionString());
        await using var services = BuildFactoryBackedSweepServiceProvider(
            database.ConnectionString
        );
        var tenantId = Guid.NewGuid();
        var asOf = new DateTimeOffset(2026, 4, 12, 12, 0, 0, TimeSpan.Zero);
        var firstId = Guid.NewGuid();
        var secondId = Guid.NewGuid();
        var heldId = Guid.NewGuid();

        await using (var scope = services.CreateAsyncScope())
        {
            var db = scope.ServiceProvider.GetRequiredService<FactoryBackedSweepDbContext>();
            await db.Database.EnsureCreatedAsync();
            db.PerRowFactorySweepRecords.AddRange(
                new PerRowFactorySweepRecord
                {
                    Id = firstId,
                    TenantId = tenantId,
                    CreatedAt = asOf.AddDays(-120),
                    ExternalId = "alpha",
                    DisplayName = "first",
                    Notes = "keep-first",
                },
                new PerRowFactorySweepRecord
                {
                    Id = secondId,
                    TenantId = tenantId,
                    CreatedAt = asOf.AddDays(-90),
                    ExternalId = "beta",
                    DisplayName = "second",
                    Notes = "keep-second",
                },
                new PerRowFactorySweepRecord
                {
                    Id = heldId,
                    TenantId = tenantId,
                    CreatedAt = asOf.AddDays(-90),
                    ExternalId = "held",
                    DisplayName = "held",
                    Notes = "keep-held",
                }
            );
            await db.SaveChangesAsync();
        }

        await using (var scope = services.CreateAsyncScope())
        {
            var repository = scope.ServiceProvider.GetRequiredService<IRetentionHoldsRepository>();
            await repository.CreateAsync(
                new RetentionHoldRequest(
                    Guid.NewGuid(),
                    RetentionEntityIdentity.For<PerRowFactorySweepRecord>(),
                    heldId.ToString(),
                    tenantId,
                    "per-row-hold",
                    asOf.AddDays(-1)
                ),
                CancellationToken.None
            );
        }

        RetentionSweepResult result;
        await using (var scope = services.CreateAsyncScope())
        {
            var engine = scope.ServiceProvider.GetRequiredService<RetentionSweepEngine>();
            result = await engine.SweepAsync(
                new TenantContext(tenantId, "uk", new Dictionary<string, string>()),
                asOf,
                SweepTriggerKind.Manual,
                SweepEntityScope.TenantedOnly
            );
        }

        result
            .Counts.Should()
            .Contain(
                new EntitySweepCount(
                    typeof(PerRowFactorySweepRecord),
                    "factory-backed-per-row",
                    tenantId,
                    Strategy.Anonymise,
                    2,
                    HeldCount: 1
                )
            );

        await using (var scope = services.CreateAsyncScope())
        {
            var db = scope.ServiceProvider.GetRequiredService<FactoryBackedSweepDbContext>();
            var records = await db
                .PerRowFactorySweepRecords.OrderBy(record => record.Notes)
                .ToListAsync();
            var factory = scope.ServiceProvider.GetRequiredService<OriginalValueEchoFactory>();
            var perRowFactory = scope.ServiceProvider.GetRequiredService<PerRowSequenceFactory>();

            records
                .Where(record =>
                    record.ExternalId == "alpha-scrubbed" || record.ExternalId == "beta-scrubbed"
                )
                .Select(record => record.DisplayName)
                .Should()
                .BeEquivalentTo(["per-row-1", "per-row-2"]);
            records.Single(record => record.Notes == "keep-held").ExternalId.Should().Be("held");
            records.Single(record => record.Notes == "keep-held").DisplayName.Should().Be("held");

            factory.Contexts.Should().HaveCount(2);
            factory
                .Contexts.Select(context => context.OriginalValue)
                .Should()
                .BeEquivalentTo(new object?[] { "alpha", "beta" });
            factory.Contexts.Should().OnlyContain(context => context.TenantId == tenantId);
            factory
                .Contexts.Should()
                .OnlyContain(context =>
                    context.MemberName == nameof(PerRowFactorySweepRecord.ExternalId)
                );

            perRowFactory.Contexts.Should().HaveCount(2);
            perRowFactory.Contexts.Should().OnlyContain(context => context.OriginalValue == null);
            perRowFactory
                .Contexts.Should()
                .OnlyContain(context =>
                    context.MemberName == nameof(PerRowFactorySweepRecord.DisplayName)
                );
        }
    }

    [Fact]
    public async Task Sweep_Path_Converts_Provider_Values_Back_To_Clr_Values_Before_Building_OriginalValue_Context()
    {
        await using var database = await TemporaryDatabase.CreateAsync(GetConnectionString());
        await using var services = BuildConvertedOriginalValueServiceProvider(
            database.ConnectionString
        );
        var tenantId = Guid.NewGuid();
        var asOf = new DateTimeOffset(2026, 4, 12, 12, 0, 0, TimeSpan.Zero);

        await using (var scope = services.CreateAsyncScope())
        {
            var db = scope.ServiceProvider.GetRequiredService<ConvertedOriginalValueDbContext>();
            await db.Database.EnsureCreatedAsync();
            db.ConvertedOriginalValueRecords.Add(
                new ConvertedOriginalValueRecord
                {
                    Id = Guid.NewGuid(),
                    TenantId = tenantId,
                    CreatedAt = asOf.AddDays(-120),
                    ExternalId = "alpha",
                    Notes = "converted-original",
                }
            );
            await db.SaveChangesAsync();
        }

        await using (var scope = services.CreateAsyncScope())
        {
            var engine = scope.ServiceProvider.GetRequiredService<RetentionSweepEngine>();
            await engine.SweepAsync(
                new TenantContext(tenantId, "uk", new Dictionary<string, string>()),
                asOf,
                SweepTriggerKind.Manual,
                SweepEntityScope.TenantedOnly
            );
        }

        await using (var scope = services.CreateAsyncScope())
        {
            var db = scope.ServiceProvider.GetRequiredService<ConvertedOriginalValueDbContext>();
            var factory = scope.ServiceProvider.GetRequiredService<ConvertedOriginalValueFactory>();
            var record = await db.ConvertedOriginalValueRecords.SingleAsync();
            var providerValue = await ReadProviderStringAsync(
                db,
                """
                SELECT external_id
                FROM converted_original_value_records
                """
            );

            record.ExternalId.Should().Be("alpha-scrubbed");
            providerValue.Should().Be("ALPHA-SCRUBBED");
            factory.Contexts.Should().ContainSingle();
            factory.Contexts[0].OriginalValue.Should().Be("alpha");
            factory.Contexts[0].TenantId.Should().Be(tenantId);
            factory
                .Contexts[0]
                .MemberName.Should()
                .Be(nameof(ConvertedOriginalValueRecord.ExternalId));
        }
    }

    [Fact]
    public async Task Sweep_Path_Converts_SetBased_Factory_Output_To_Provider_Values_Before_Writing()
    {
        await using var database = await TemporaryDatabase.CreateAsync(GetConnectionString());
        await using var services = BuildConvertedOriginalValueServiceProvider(
            database.ConnectionString
        );
        var tenantId = Guid.NewGuid();
        var asOf = new DateTimeOffset(2026, 4, 12, 12, 0, 0, TimeSpan.Zero);

        await using (var scope = services.CreateAsyncScope())
        {
            var db = scope.ServiceProvider.GetRequiredService<ConvertedOriginalValueDbContext>();
            await db.Database.EnsureCreatedAsync();
            db.ConvertedSetBasedValueRecords.Add(
                new ConvertedSetBasedValueRecord
                {
                    Id = Guid.NewGuid(),
                    TenantId = tenantId,
                    CreatedAt = asOf.AddDays(-120),
                    ExternalId = "seed-value",
                    Notes = "converted-set-based",
                }
            );
            await db.SaveChangesAsync();
        }

        await using (var scope = services.CreateAsyncScope())
        {
            var engine = scope.ServiceProvider.GetRequiredService<RetentionSweepEngine>();
            await engine.SweepAsync(
                new TenantContext(tenantId, "uk", new Dictionary<string, string>()),
                asOf,
                SweepTriggerKind.Manual,
                SweepEntityScope.TenantedOnly
            );
        }

        await using (var scope = services.CreateAsyncScope())
        {
            var db = scope.ServiceProvider.GetRequiredService<ConvertedOriginalValueDbContext>();
            var factory = scope.ServiceProvider.GetRequiredService<ConvertedSetBasedValueFactory>();
            var record = await db.ConvertedSetBasedValueRecords.SingleAsync();
            var providerValue = await ReadProviderStringAsync(
                db,
                """
                SELECT external_id
                FROM converted_set_based_value_records
                """
            );

            record.ExternalId.Should().Be("set-based-scrubbed");
            providerValue.Should().Be("SET-BASED-SCRUBBED");
            factory.Contexts.Should().ContainSingle();
            factory.Contexts[0].OriginalValue.Should().BeNull();
            factory.Contexts[0].TenantId.Should().Be(tenantId);
            factory
                .Contexts[0]
                .MemberName.Should()
                .Be(nameof(ConvertedSetBasedValueRecord.ExternalId));
        }
    }

    private string GetConnectionString()
    {
        using var db = Host.CreateDbContext();
        return db.Database.GetConnectionString()!;
    }

    private static ServiceProvider BuildFactoryBackedSweepServiceProvider(string connectionString)
    {
        var services = new ServiceCollection();
        var configuration = new ConfigurationBuilder().AddInMemoryCollection().Build();

        services.AddSingleton<IConfiguration>(configuration);
        services.AddLogging();
        services.AddDbContext<FactoryBackedSweepDbContext>(options =>
            options.UseNpgsql(connectionString)
        );
        services.AddSingleton<IRetentionCategoryRepository>(
            new StaticCategoryRepository(
                new Dictionary<string, IRetentionRuleResolver>
                {
                    ["factory-backed-set-based"] = new StaticRetentionRuleResolver(
                        new RetentionRule(TimeSpan.FromDays(30), Strategy.Anonymise)
                    ),
                    ["factory-backed-per-row"] = new StaticRetentionRuleResolver(
                        new RetentionRule(TimeSpan.FromDays(30), Strategy.Anonymise)
                    ),
                }
            )
        );
        services.AddSingleton<SetBasedGuidFactory>();
        services.AddSingleton<PerRowSequenceFactory>();
        services.AddSingleton<OriginalValueEchoFactory>();
        services.AddSingleton<IAnonymiseValueFactory>(sp =>
            sp.GetRequiredService<SetBasedGuidFactory>()
        );
        services.AddSingleton<IAnonymiseValueFactory>(sp =>
            sp.GetRequiredService<PerRowSequenceFactory>()
        );
        services.AddSingleton<IAnonymiseValueFactory>(sp =>
            sp.GetRequiredService<OriginalValueEchoFactory>()
        );
        services.AddCohort<FactoryBackedSweepDbContext>();

        return services.BuildServiceProvider(validateScopes: true);
    }

    private static ServiceProvider BuildConvertedOriginalValueServiceProvider(
        string connectionString
    )
    {
        var services = new ServiceCollection();
        var configuration = new ConfigurationBuilder().AddInMemoryCollection().Build();

        services.AddSingleton<IConfiguration>(configuration);
        services.AddLogging();
        services.AddDbContext<ConvertedOriginalValueDbContext>(options =>
            options.UseNpgsql(connectionString)
        );
        services.AddSingleton<IRetentionCategoryRepository>(
            new StaticCategoryRepository(
                new Dictionary<string, IRetentionRuleResolver>
                {
                    ["converted-original-value"] = new StaticRetentionRuleResolver(
                        new RetentionRule(TimeSpan.FromDays(30), Strategy.Anonymise)
                    ),
                    ["converted-set-based-value"] = new StaticRetentionRuleResolver(
                        new RetentionRule(TimeSpan.FromDays(30), Strategy.Anonymise)
                    ),
                }
            )
        );
        services.AddSingleton<ConvertedSetBasedValueFactory>();
        services.AddSingleton<ConvertedOriginalValueFactory>();
        services.AddSingleton<IAnonymiseValueFactory>(sp =>
            sp.GetRequiredService<ConvertedSetBasedValueFactory>()
        );
        services.AddSingleton<IAnonymiseValueFactory>(sp =>
            sp.GetRequiredService<ConvertedOriginalValueFactory>()
        );
        services.AddCohort<ConvertedOriginalValueDbContext>();

        return services.BuildServiceProvider(validateScopes: true);
    }

    private sealed class FactoryBackedSweepDbContext(
        DbContextOptions<FactoryBackedSweepDbContext> options
    ) : DbContext(options)
    {
        public DbSet<SetBasedFactorySweepRecord> SetBasedFactorySweepRecords =>
            Set<SetBasedFactorySweepRecord>();
        public DbSet<PerRowFactorySweepRecord> PerRowFactorySweepRecords =>
            Set<PerRowFactorySweepRecord>();

        protected override void OnModelCreating(ModelBuilder modelBuilder)
        {
            modelBuilder.Entity<SetBasedFactorySweepRecord>(entity =>
            {
                entity.ToTable("set_based_factory_sweep_records");
                entity.HasKey(record => record.Id);
                entity.Property(record => record.TenantId).HasColumnName("tenant_id");
                entity.Property(record => record.CreatedAt).HasColumnName("created_at_utc");
                entity.Property(record => record.ExternalId).HasColumnName("external_id");
                entity.Property(record => record.DisplayName).HasColumnName("display_name");
            });

            modelBuilder.Entity<PerRowFactorySweepRecord>(entity =>
            {
                entity.ToTable("per_row_factory_sweep_records");
                entity.HasKey(record => record.Id);
                entity.Property(record => record.TenantId).HasColumnName("tenant_id");
                entity.Property(record => record.CreatedAt).HasColumnName("created_at_utc");
                entity.Property(record => record.ExternalId).HasColumnName("external_id");
                entity.Property(record => record.DisplayName).HasColumnName("display_name");
                entity.Property(record => record.Notes).HasColumnName("notes");
            });

            modelBuilder.ConfigureCohortTables();
        }
    }

    private sealed class ConvertedOriginalValueDbContext(
        DbContextOptions<ConvertedOriginalValueDbContext> options
    ) : DbContext(options)
    {
        public DbSet<ConvertedOriginalValueRecord> ConvertedOriginalValueRecords =>
            Set<ConvertedOriginalValueRecord>();
        public DbSet<ConvertedSetBasedValueRecord> ConvertedSetBasedValueRecords =>
            Set<ConvertedSetBasedValueRecord>();

        protected override void OnModelCreating(ModelBuilder modelBuilder)
        {
            modelBuilder.Entity<ConvertedOriginalValueRecord>(entity =>
            {
                entity.ToTable("converted_original_value_records");
                entity.HasKey(record => record.Id);
                entity.Property(record => record.TenantId).HasColumnName("tenant_id");
                entity.Property(record => record.CreatedAt).HasColumnName("created_at_utc");
                entity
                    .Property(record => record.ExternalId)
                    .HasColumnName("external_id")
                    .HasConversion(
                        value => value.ToUpperInvariant(),
                        value => value.ToLowerInvariant()
                    );
                entity.Property(record => record.Notes).HasColumnName("notes");
            });

            modelBuilder.Entity<ConvertedSetBasedValueRecord>(entity =>
            {
                entity.ToTable("converted_set_based_value_records");
                entity.HasKey(record => record.Id);
                entity.Property(record => record.TenantId).HasColumnName("tenant_id");
                entity.Property(record => record.CreatedAt).HasColumnName("created_at_utc");
                entity
                    .Property(record => record.ExternalId)
                    .HasColumnName("external_id")
                    .HasConversion(
                        value => value.ToUpperInvariant(),
                        value => value.ToLowerInvariant()
                    );
                entity.Property(record => record.Notes).HasColumnName("notes");
            });

            modelBuilder.ConfigureCohortTables();
        }
    }

    [Retain("factory-backed-set-based", nameof(SetBasedFactorySweepRecord.CreatedAt))]
    [RetentionEntityId("00000000-0000-0000-0001-000000000001")]
    private sealed class SetBasedFactorySweepRecord
    {
        public Guid Id { get; set; }
        public Guid TenantId { get; set; }
        public DateTimeOffset CreatedAt { get; set; }

        [AnonymiseWith(typeof(SetBasedGuidFactory))]
        public Guid ExternalId { get; set; }

        public string DisplayName { get; set; } = "";

        public DateTimeOffset? AnonymisedAt { get; set; }
    }

    [Retain("factory-backed-per-row", nameof(PerRowFactorySweepRecord.CreatedAt))]
    [RetentionEntityId("00000000-0000-0000-0001-000000000002")]
    private sealed class PerRowFactorySweepRecord
    {
        public Guid Id { get; set; }
        public Guid TenantId { get; set; }
        public DateTimeOffset CreatedAt { get; set; }

        [AnonymiseWith(typeof(OriginalValueEchoFactory))]
        public string ExternalId { get; set; } = "";

        [AnonymiseWith(typeof(PerRowSequenceFactory))]
        public string DisplayName { get; set; } = "";

        public string Notes { get; set; } = "";

        public DateTimeOffset? AnonymisedAt { get; set; }
    }

    [Retain("converted-original-value", nameof(ConvertedOriginalValueRecord.CreatedAt))]
    [RetentionEntityId("00000000-0000-0000-0001-000000000003")]
    private sealed class ConvertedOriginalValueRecord
    {
        public Guid Id { get; set; }
        public Guid TenantId { get; set; }
        public DateTimeOffset CreatedAt { get; set; }

        [AnonymiseWith(typeof(ConvertedOriginalValueFactory))]
        public string ExternalId { get; set; } = "";

        public string Notes { get; set; } = "";

        public DateTimeOffset? AnonymisedAt { get; set; }
    }

    [Retain("converted-set-based-value", nameof(ConvertedSetBasedValueRecord.CreatedAt))]
    [RetentionEntityId("00000000-0000-0000-0001-000000000004")]
    private sealed class ConvertedSetBasedValueRecord
    {
        public Guid Id { get; set; }
        public Guid TenantId { get; set; }
        public DateTimeOffset CreatedAt { get; set; }

        [AnonymiseWith(typeof(ConvertedSetBasedValueFactory))]
        public string ExternalId { get; set; } = "";

        public string Notes { get; set; } = "";

        public DateTimeOffset? AnonymisedAt { get; set; }
    }

    private sealed class SetBasedGuidFactory : IAnonymiseValueFactory
    {
        public static readonly Guid ScrubbedValue = Guid.Parse(
            "11111111-1111-1111-1111-111111111111"
        );
        public List<AnonymiseValueContext> Contexts { get; } = [];

        public object? Create(AnonymiseValueContext context)
        {
            Contexts.Add(context);
            return ScrubbedValue;
        }
    }

    private sealed class PerRowSequenceFactory : IAnonymiseValueFactory
    {
        public AnonymiseFactoryExecutionMode ExecutionMode =>
            AnonymiseFactoryExecutionMode.PerRow;
        public List<AnonymiseValueContext> Contexts { get; } = [];
        private int sequence = 0;

        public object? Create(AnonymiseValueContext context)
        {
            Contexts.Add(context);
            sequence++;
            return $"per-row-{sequence}";
        }
    }

    private sealed class OriginalValueEchoFactory : IAnonymiseValueFactory
    {
        public AnonymiseFactoryExecutionMode ExecutionMode =>
            AnonymiseFactoryExecutionMode.PerRowWithOriginalValue;
        public List<AnonymiseValueContext> Contexts { get; } = [];

        public object? Create(AnonymiseValueContext context)
        {
            Contexts.Add(context);
            return $"{context.OriginalValue}-scrubbed";
        }
    }

    private sealed class ConvertedOriginalValueFactory : IAnonymiseValueFactory
    {
        public AnonymiseFactoryExecutionMode ExecutionMode =>
            AnonymiseFactoryExecutionMode.PerRowWithOriginalValue;
        public List<AnonymiseValueContext> Contexts { get; } = [];

        public object? Create(AnonymiseValueContext context)
        {
            Contexts.Add(context);
            return $"{context.OriginalValue}-scrubbed";
        }
    }

    private sealed class ConvertedSetBasedValueFactory : IAnonymiseValueFactory
    {
        public List<AnonymiseValueContext> Contexts { get; } = [];

        public object? Create(AnonymiseValueContext context)
        {
            Contexts.Add(context);
            return "set-based-scrubbed";
        }
    }

    private static async Task<string> ReadProviderStringAsync(DbContext db, string sql)
    {
        var connection = db.Database.GetDbConnection();
        if (connection.State != ConnectionState.Open)
        {
            await connection.OpenAsync();
        }

        await using var command = connection.CreateCommand();
        command.CommandText = sql;
        return (string)(await command.ExecuteScalarAsync())!;
    }

    private sealed class StaticCategoryRepository(
        IReadOnlyDictionary<string, IRetentionRuleResolver> resolvers
    ) : IRetentionCategoryRepository
    {
        // Falls through to Exempt for categories the test does not care about
        // (e.g. categories owned by other sample entities sharing SampleDbContext).
        private static readonly IRetentionRuleResolver ExemptFallback =
            new StaticRetentionRuleResolver(
                new RetentionRule(TimeSpan.FromDays(30), Strategy.Exempt)
            );

        public Task<IRetentionRuleResolver?> GetAsync(string category, CancellationToken ct)
        {
            return resolvers.TryGetValue(category, out var resolver)
                ? Task.FromResult<IRetentionRuleResolver?>(resolver)
                : Task.FromResult<IRetentionRuleResolver?>(ExemptFallback);
        }
    }
}

file sealed class SubjectRaceContactHandler : IRetentionHandler<AnonymisedContact>
{
    public Task OnBeforeAsync(
        AnonymisedContact row,
        RetentionBeforeContext ctx,
        CancellationToken ct
    ) => Task.CompletedTask;
}
