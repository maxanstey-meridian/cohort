using Cohort.Application;
using Cohort.Domain;
using Cohort.Hosting;
using Cohort.Infrastructure.Migrations;
using Cohort.Infrastructure.Sweep;
using Cohort.Sample.Entities;

using System.Collections;
using System.Data;
using System.Data.Common;
using System.Diagnostics.CodeAnalysis;

using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;

namespace Cohort.Sample.Tests;

public sealed class AnonymiseSweepEndToEndTests(PostgresFixture fixture)
    : IntegrationTestBase(fixture)
{
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

        result.Counts.Should().Contain(
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

        contacts.Should().Equal(
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
    public async Task Startup_Path_Fails_When_An_Anonymise_Category_Has_No_Annotated_Fields()
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

        var act = () => host.RunStartupAsync();

        var exception = await act.Should().ThrowAsync<RetentionConfigurationException>();
        exception.Which.Errors.Should().ContainSingle();
        exception
            .Which.Errors[0]
            .Should()
            .Be(
                $"Anonymise convention on {typeof(Note).FullName}: retained Anonymise categories require at least one [Anonymise]-annotated property mapped by EF."
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

        first.Counts.Should().Contain(
            new EntitySweepCount(
                typeof(AnonymisedContact),
                "anonymise",
                tenantId,
                Strategy.Anonymise,
                1
            )
        );
        second.Counts.Should().Contain(
            count =>
                count.EntityType == typeof(AnonymisedContact)
                && count.Category == "anonymise"
                && count.TenantId == tenantId
                && count.Strategy == Strategy.Anonymise
        );

        await using var verify = Host.CreateDbContext();
        var contact = await verify.AnonymisedContacts.SingleAsync();

        contact.EmailAddress.Should().BeNull();
        contact.GivenName.Should().BeEmpty();
        contact.Surname.Should().Be("[redacted]");
        contact.Notes.Should().Be("repeat-notes");
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

        result.Counts.Should().Contain(
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

        contacts.Should().Equal(
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
        await using var services = BuildFactoryBackedSweepServiceProvider(database.ConnectionString);
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
                asOf
            );
        }

        result.Counts.Should().Contain(
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
            var records = await db.SetBasedFactorySweepRecords.OrderBy(record => record.DisplayName).ToListAsync();
            var factory = scope.ServiceProvider.GetRequiredService<SetBasedGuidFactory>();

            records.Single(record => record.DisplayName == "expired-a").ExternalId.Should().Be(SetBasedGuidFactory.ScrubbedValue);
            records.Single(record => record.DisplayName == "expired-b").ExternalId.Should().Be(SetBasedGuidFactory.ScrubbedValue);
            records.Single(record => record.DisplayName == "fresh").ExternalId.Should().NotBe(SetBasedGuidFactory.ScrubbedValue);
            records.Single(record => record.DisplayName == "other-tenant").ExternalId.Should().NotBe(SetBasedGuidFactory.ScrubbedValue);

            factory.Contexts.Should().ContainSingle();
            factory.Contexts[0].OriginalValue.Should().BeNull();
            factory.Contexts[0].TenantId.Should().Be(tenantId);
            factory.Contexts[0].MemberName.Should().Be(nameof(SetBasedFactorySweepRecord.ExternalId));
        }
    }

    [Fact]
    public async Task Sweep_Path_Uses_PerRow_Execution_For_Factories_That_Require_Original_Values_And_Respects_Holds()
    {
        await using var database = await TemporaryDatabase.CreateAsync(GetConnectionString());
        await using var services = BuildFactoryBackedSweepServiceProvider(database.ConnectionString);
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
                    "per_row_factory_sweep_records",
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
                asOf
            );
        }

        result.Counts.Should().Contain(
            new EntitySweepCount(
                typeof(PerRowFactorySweepRecord),
                "factory-backed-per-row",
                tenantId,
                Strategy.Anonymise,
                2
            )
        );

        await using (var scope = services.CreateAsyncScope())
        {
            var db = scope.ServiceProvider.GetRequiredService<FactoryBackedSweepDbContext>();
            var records = await db.PerRowFactorySweepRecords.OrderBy(record => record.Notes).ToListAsync();
            var factory = scope.ServiceProvider.GetRequiredService<OriginalValueEchoFactory>();
            var perRowFactory = scope.ServiceProvider.GetRequiredService<PerRowSequenceFactory>();

            records.Where(record =>
                    record.ExternalId == "alpha-scrubbed"
                    || record.ExternalId == "beta-scrubbed"
                )
                .Select(record => record.DisplayName)
                .Should()
                .BeEquivalentTo(["per-row-1", "per-row-2"]);
            records.Single(record => record.Notes == "keep-held").ExternalId.Should().Be("held");
            records.Single(record => record.Notes == "keep-held").DisplayName.Should().Be("held");

            factory.Contexts.Should().HaveCount(2);
            factory.Contexts.Select(context => context.OriginalValue).Should().BeEquivalentTo(new object?[] { "alpha", "beta" });
            factory.Contexts.Should().OnlyContain(context => context.TenantId == tenantId);
            factory.Contexts.Should().OnlyContain(context => context.MemberName == nameof(PerRowFactorySweepRecord.ExternalId));

            perRowFactory.Contexts.Should().HaveCount(2);
            perRowFactory.Contexts.Should().OnlyContain(context => context.OriginalValue == null);
            perRowFactory.Contexts.Should().OnlyContain(context => context.MemberName == nameof(PerRowFactorySweepRecord.DisplayName));
        }
    }

    [Fact]
    public async Task Sweep_Path_Converts_Provider_Values_Back_To_Clr_Values_Before_Building_OriginalValue_Context()
    {
        await using var database = await TemporaryDatabase.CreateAsync(GetConnectionString());
        await using var services = BuildConvertedOriginalValueServiceProvider(database.ConnectionString);
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
                asOf
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
            factory.Contexts[0].MemberName.Should().Be(nameof(ConvertedOriginalValueRecord.ExternalId));
        }
    }

    [Fact]
    public async Task Sweep_Path_Converts_SetBased_Factory_Output_To_Provider_Values_Before_Writing()
    {
        await using var database = await TemporaryDatabase.CreateAsync(GetConnectionString());
        await using var services = BuildConvertedOriginalValueServiceProvider(database.ConnectionString);
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
                asOf
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
            factory.Contexts[0].MemberName.Should().Be(nameof(ConvertedSetBasedValueRecord.ExternalId));
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
        services.AddDbContext<FactoryBackedSweepDbContext>(options => options.UseNpgsql(connectionString));
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
        services.AddSingleton<IAnonymiseValueFactory>(sp => sp.GetRequiredService<SetBasedGuidFactory>());
        services.AddSingleton<IAnonymiseValueFactory>(sp => sp.GetRequiredService<PerRowSequenceFactory>());
        services.AddSingleton<IAnonymiseValueFactory>(sp => sp.GetRequiredService<OriginalValueEchoFactory>());
        services.AddCohort<FactoryBackedSweepDbContext>();

        return services.BuildServiceProvider(validateScopes: true);
    }

    private static ServiceProvider BuildConvertedOriginalValueServiceProvider(string connectionString)
    {
        var services = new ServiceCollection();
        var configuration = new ConfigurationBuilder().AddInMemoryCollection().Build();

        services.AddSingleton<IConfiguration>(configuration);
        services.AddLogging();
        services.AddDbContext<ConvertedOriginalValueDbContext>(options => options.UseNpgsql(connectionString));
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
        services.AddSingleton<IAnonymiseValueFactory>(
            sp => sp.GetRequiredService<ConvertedSetBasedValueFactory>()
        );
        services.AddSingleton<IAnonymiseValueFactory>(
            sp => sp.GetRequiredService<ConvertedOriginalValueFactory>()
        );
        services.AddCohort<ConvertedOriginalValueDbContext>();

        return services.BuildServiceProvider(validateScopes: true);
    }

    private sealed class FactoryBackedSweepDbContext(
        DbContextOptions<FactoryBackedSweepDbContext> options
    ) : DbContext(options)
    {
        public DbSet<SetBasedFactorySweepRecord> SetBasedFactorySweepRecords => Set<SetBasedFactorySweepRecord>();
        public DbSet<PerRowFactorySweepRecord> PerRowFactorySweepRecords => Set<PerRowFactorySweepRecord>();

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
    private sealed class SetBasedFactorySweepRecord
    {
        public Guid Id { get; set; }
        public Guid TenantId { get; set; }
        public DateTimeOffset CreatedAt { get; set; }

        [AnonymiseWith(typeof(SetBasedGuidFactory))]
        public Guid ExternalId { get; set; }

        public string DisplayName { get; set; } = "";
    }

    [Retain("factory-backed-per-row", nameof(PerRowFactorySweepRecord.CreatedAt))]
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
    }

    [Retain("converted-original-value", nameof(ConvertedOriginalValueRecord.CreatedAt))]
    private sealed class ConvertedOriginalValueRecord
    {
        public Guid Id { get; set; }
        public Guid TenantId { get; set; }
        public DateTimeOffset CreatedAt { get; set; }

        [AnonymiseWith(typeof(ConvertedOriginalValueFactory))]
        public string ExternalId { get; set; } = "";

        public string Notes { get; set; } = "";
    }

    [Retain("converted-set-based-value", nameof(ConvertedSetBasedValueRecord.CreatedAt))]
    private sealed class ConvertedSetBasedValueRecord
    {
        public Guid Id { get; set; }
        public Guid TenantId { get; set; }
        public DateTimeOffset CreatedAt { get; set; }

        [AnonymiseWith(typeof(ConvertedSetBasedValueFactory))]
        public string ExternalId { get; set; } = "";

        public string Notes { get; set; } = "";
    }

    private sealed class SetBasedGuidFactory : IAnonymiseValueFactory
    {
        public static readonly Guid ScrubbedValue = Guid.Parse("11111111-1111-1111-1111-111111111111");
        public List<AnonymiseValueContext> Contexts { get; } = [];

        public object? Create(AnonymiseValueContext context)
        {
            Contexts.Add(context);
            return ScrubbedValue;
        }
    }

    private sealed class PerRowSequenceFactory : IAnonymiseValueFactory
    {
        public bool RequiresPerRowExecution => true;
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
        public bool RequiresOriginalValue => true;
        public List<AnonymiseValueContext> Contexts { get; } = [];

        public object? Create(AnonymiseValueContext context)
        {
            Contexts.Add(context);
            return $"{context.OriginalValue}-scrubbed";
        }
    }

    private sealed class ConvertedOriginalValueFactory : IAnonymiseValueFactory
    {
        public bool RequiresOriginalValue => true;
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
        private static readonly IRetentionRuleResolver ExemptFallback = new StaticRetentionRuleResolver(
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

[Collection("Integration")]
public sealed class AnonymiseSweepStrategyCommandTests
{
    [Fact]
    public void Constructor_Requires_A_DbContext()
    {
        var act = () => new AnonymiseSweepStrategy(null!);

        act.Should().Throw<ArgumentNullException>().Which.ParamName.Should().Be("db");
    }

    [Fact]
    public async Task PreviewAsync_Uses_A_Hold_Aware_Count_Query()
    {
        using var db = CreateCommandStrategyDbContext();
        var strategy = new AnonymiseSweepStrategy(db);
        var connection = new RecordingDbConnection();
        var tenantId = Guid.NewGuid();
        var now = new DateTimeOffset(2026, 4, 12, 12, 0, 0, TimeSpan.Zero);
        var entry = new RetentionEntry(
            typeof(AnonymisedContact),
            "anonymised_contacts",
            "anonymise",
            nameof(AnonymisedContact.CreatedAt),
            "CreatedAt",
            new RecordIdConvention(nameof(AnonymisedContact.Id), "Id", typeof(Guid)),
            [
                new AnonymiseLiteralField(nameof(AnonymisedContact.EmailAddress), "EmailAddress", AnonymiseMethod.Null),
                new AnonymiseLiteralField(nameof(AnonymisedContact.GivenName), "GivenName", AnonymiseMethod.EmptyString),
                new AnonymiseLiteralField(
                    nameof(AnonymisedContact.Surname),
                    "Surname",
                    AnonymiseMethod.FixedLiteral,
                    "[redacted]"
                ),
            ],
            new TenantConvention(nameof(AnonymisedContact.TenantId), "TenantId"),
            null
        );
        var rule = new RetentionRule(TimeSpan.FromDays(30), Strategy.Anonymise);
        var context = new RetentionResolutionContext(
            "anonymise",
            new TenantContext(tenantId, "uk", new Dictionary<string, string>()),
            now,
            []
        );

        var affected = await strategy.PreviewAsync(
            entry,
            rule,
            context,
            connection,
            CancellationToken.None
        );

        affected.Should().Be(1);
        connection.Commands.Should().ContainSingle();
        connection.LastCommand.Should().NotBeNull();
        connection.LastCommand!.AssignedTransaction.Should().BeNull();
        connection.LastCommand.CommandText.Should().Contain("SELECT COUNT(*)");
        connection.LastCommand.CommandText.Should().Contain("@cutoff");
        connection.LastCommand.CommandText.Should().Contain("@tenantId");
        connection.LastCommand.CommandText.Should().Contain("@holdTableName");
        connection.LastCommand.CommandText.Should().Contain("@holdAsOf");
        connection.LastCommand.CommandText.Should().Contain("NOT EXISTS");
        connection.LastCommand.Parameters.Count.Should().Be(4);
        GetParameterNames(connection.LastCommand).Should().Equal(
            "cutoff",
            "tenantId",
            "holdTableName",
            "holdAsOf"
        );
        connection.LastCommand.Parameters["cutoff"].Value.Should().Be(now.AddDays(-30));
        connection.LastCommand.Parameters["tenantId"].Value.Should().Be(tenantId);
        connection.LastCommand.Parameters["holdTableName"].Value.Should().Be("anonymised_contacts");
        connection.LastCommand.Parameters["holdAsOf"].Value.Should().Be(now);
    }

    [Fact]
    public async Task SweepAsync_Uses_Parameterized_Assignments_For_All_Anonymise_Methods()
    {
        using var db = CreateCommandStrategyDbContext();
        var strategy = new AnonymiseSweepStrategy(db);
        var connection = new RecordingDbConnection();
        connection.EnqueueResultSet(Guid.NewGuid());
        connection.EnqueueResultSet(Guid.NewGuid());
        var transaction = connection.BeginTransaction();
        var tenantId = Guid.NewGuid();
        var now = new DateTimeOffset(2026, 4, 12, 12, 0, 0, TimeSpan.Zero);
        var entry = new RetentionEntry(
            typeof(AnonymisedContact),
            "anonymised_contacts",
            "anonymise",
            nameof(AnonymisedContact.CreatedAt),
            "CreatedAt",
            new RecordIdConvention(nameof(AnonymisedContact.Id), "Id", typeof(Guid)),
            [
                new AnonymiseLiteralField(nameof(AnonymisedContact.EmailAddress), "EmailAddress", AnonymiseMethod.Null),
                new AnonymiseLiteralField(nameof(AnonymisedContact.GivenName), "GivenName", AnonymiseMethod.EmptyString),
                new AnonymiseLiteralField(
                    nameof(AnonymisedContact.Surname),
                    "Surname",
                    AnonymiseMethod.FixedLiteral,
                    "[redacted]"
                ),
            ],
            new TenantConvention(nameof(AnonymisedContact.TenantId), "TenantId"),
            null
        );
        var rule = new RetentionRule(TimeSpan.FromDays(30), Strategy.Anonymise);
        var context = new RetentionResolutionContext(
            "anonymise",
            new TenantContext(tenantId, "uk", new Dictionary<string, string>()),
            now,
            []
        );

        var affected = await strategy.SweepAsync(
            entry,
            rule,
            context,
            connection,
            transaction,
            CancellationToken.None
        );

        affected.AffectedRecordIds.Should().ContainSingle();
        affected.HeldCount.Should().Be(0);
        connection.LastCommand.Should().NotBeNull();
        connection.LastCommand!.AssignedTransaction.Should().BeSameAs(transaction);
        connection.LastCommand.CommandText.Should().Contain("\"EmailAddress\" = @value0");
        connection.LastCommand.CommandText.Should().Contain("\"GivenName\" = @value1");
        connection.LastCommand.CommandText.Should().Contain("\"Surname\" = @value2");
        connection.LastCommand.CommandText.Should().Contain("@cutoff");
        connection.LastCommand.CommandText.Should().Contain("@tenantId");
        connection.LastCommand.CommandText.Should().Contain("@candidateIds");
        connection.LastCommand.CommandText.Should().Contain("@holdTableName");
        connection.LastCommand.CommandText.Should().Contain("@holdAsOf");
        connection.LastCommand.CommandText.Should().Contain("NOT EXISTS");
        connection.LastCommand.Parameters.Count.Should().Be(8);
        GetParameterNames(connection.LastCommand).Should().Equal(
            "value0",
            "value1",
            "value2",
            "cutoff",
            "tenantId",
            "candidateIds",
            "holdTableName",
            "holdAsOf"
        );
        connection.LastCommand.Parameters.Contains("value0").Should().BeTrue();
        connection.LastCommand.Parameters.Contains("value1").Should().BeTrue();
        connection.LastCommand.Parameters.Contains("value2").Should().BeTrue();
        connection.LastCommand.Parameters.Contains("cutoff").Should().BeTrue();
        connection.LastCommand.Parameters.Contains("tenantId").Should().BeTrue();
        connection.LastCommand.Parameters.Contains("candidateIds").Should().BeTrue();
        connection.LastCommand.Parameters.Contains("holdTableName").Should().BeTrue();
        connection.LastCommand.Parameters.Contains("holdAsOf").Should().BeTrue();
        connection.LastCommand.Parameters["value0"].Value.Should().Be(DBNull.Value);
        connection.LastCommand.Parameters["value1"].Value.Should().Be(string.Empty);
        connection.LastCommand.Parameters["value2"].Value.Should().Be("[redacted]");
        connection.LastCommand.Parameters["cutoff"].Value.Should().Be(now.AddDays(-30));
        connection.LastCommand.Parameters["tenantId"].Value.Should().Be(tenantId);
        connection.LastCommand.Parameters["candidateIds"].Value.Should().BeOfType<string[]>();
        connection.LastCommand.Parameters["holdTableName"].Value.Should().Be("anonymised_contacts");
        connection.LastCommand.Parameters["holdAsOf"].Value.Should().Be(now);
    }

    [Fact]
    public async Task SweepAsync_Computes_HeldCount_From_Selected_Candidates_And_Targets_Only_Those_Ids()
    {
        var selectedId = Guid.NewGuid();
        var heldId = Guid.NewGuid();
        using var db = CreateCommandStrategyDbContext();
        var strategy = new AnonymiseSweepStrategy(db);
        var connection = new RecordingDbConnection();
        connection.EnqueueResultSet(selectedId, heldId);
        connection.EnqueueResultSet(selectedId);
        var transaction = connection.BeginTransaction();
        var tenantId = Guid.NewGuid();
        var now = new DateTimeOffset(2026, 4, 12, 12, 0, 0, TimeSpan.Zero);
        var entry = new RetentionEntry(
            typeof(AnonymisedContact),
            "anonymised_contacts",
            "anonymise",
            nameof(AnonymisedContact.CreatedAt),
            "CreatedAt",
            new RecordIdConvention(nameof(AnonymisedContact.Id), "Id", typeof(Guid)),
            [
                new AnonymiseLiteralField(nameof(AnonymisedContact.EmailAddress), "EmailAddress", AnonymiseMethod.Null),
                new AnonymiseLiteralField(nameof(AnonymisedContact.GivenName), "GivenName", AnonymiseMethod.EmptyString),
                new AnonymiseLiteralField(
                    nameof(AnonymisedContact.Surname),
                    "Surname",
                    AnonymiseMethod.FixedLiteral,
                    "[redacted]"
                ),
            ],
            new TenantConvention(nameof(AnonymisedContact.TenantId), "TenantId"),
            null
        );
        var rule = new RetentionRule(TimeSpan.FromDays(30), Strategy.Anonymise);
        var context = new RetentionResolutionContext(
            "anonymise",
            new TenantContext(tenantId, "uk", new Dictionary<string, string>()),
            now,
            []
        );

        var affected = await strategy.SweepAsync(
            entry,
            rule,
            context,
            connection,
            transaction,
            CancellationToken.None
        );

        affected.AffectedRecordIds.Should().Equal(selectedId.ToString());
        affected.HeldCount.Should().Be(1);
        connection.Commands.Should().HaveCount(2);
        connection.Commands[0].CommandText.Should().Contain("FOR UPDATE");
        GetParameterNames(connection.Commands[0]).Should().Equal("cutoff", "tenantId");
        connection.Commands[1].CommandText.Should().Contain("ANY(@candidateIds)");
        GetParameterNames(connection.Commands[1]).Should().Equal(
            "value0",
            "value1",
            "value2",
            "cutoff",
            "tenantId",
            "candidateIds",
            "holdTableName",
            "holdAsOf"
        );
        connection.Commands[1].Parameters["candidateIds"].Value.Should().BeEquivalentTo(
            new[] { selectedId.ToString(), heldId.ToString() }
        );
    }

    [Fact]
    public async Task SweepAsync_Uses_The_Mapped_Record_Id_Column_In_Hold_Filtering()
    {
        using var db = CreateCommandStrategyDbContext();
        var strategy = new AnonymiseSweepStrategy(db);
        var connection = new RecordingDbConnection();
        connection.EnqueueResultSet(Guid.NewGuid());
        connection.EnqueueResultSet(Guid.NewGuid());
        var transaction = connection.BeginTransaction();
        var tenantId = Guid.NewGuid();
        var now = new DateTimeOffset(2026, 4, 12, 12, 0, 0, TimeSpan.Zero);
        var entry = new RetentionEntry(
            typeof(AnonymisedContact),
            "anonymised_contacts",
            "anonymise",
            nameof(AnonymisedContact.CreatedAt),
            "CreatedAt",
            new RecordIdConvention(nameof(AnonymisedContact.Id), "record_id", typeof(Guid)),
            [
                new AnonymiseLiteralField(nameof(AnonymisedContact.EmailAddress), "EmailAddress", AnonymiseMethod.Null),
                new AnonymiseLiteralField(nameof(AnonymisedContact.GivenName), "GivenName", AnonymiseMethod.EmptyString),
                new AnonymiseLiteralField(
                    nameof(AnonymisedContact.Surname),
                    "Surname",
                    AnonymiseMethod.FixedLiteral,
                    "[redacted]"
                ),
            ],
            new TenantConvention(nameof(AnonymisedContact.TenantId), "TenantId"),
            null
        );
        var rule = new RetentionRule(TimeSpan.FromDays(30), Strategy.Anonymise);
        var context = new RetentionResolutionContext(
            "anonymise",
            new TenantContext(tenantId, "uk", new Dictionary<string, string>()),
            now,
            []
        );

        var affected = await strategy.SweepAsync(
            entry,
            rule,
            context,
            connection,
            transaction,
            CancellationToken.None
        );

        affected.AffectedRecordIds.Should().ContainSingle();
        affected.HeldCount.Should().Be(0);
        connection.LastCommand.Should().NotBeNull();
        connection.LastCommand!.CommandText.Should().Contain("hold.\"RecordId\" = CAST(target.\"record_id\" AS text)");
    }

    [Fact]
    public async Task PreviewEraseAsync_Uses_A_NonMutating_HoldAware_Count_Query_For_The_Selected_Subject()
    {
        var tenantId = Guid.NewGuid();
        var subjectId = Guid.NewGuid();
        var now = new DateTimeOffset(2026, 4, 12, 12, 0, 0, TimeSpan.Zero);
        using var db = CreateCommandStrategyDbContext();
        var strategy = new AnonymiseSweepStrategy(db);
        var connection = new RecordingDbConnection();
        var entry = new RetentionEntry(
            typeof(AnonymisedContact),
            "anonymised_contacts",
            "anonymise",
            nameof(AnonymisedContact.CreatedAt),
            "CreatedAt",
            new RecordIdConvention(nameof(AnonymisedContact.Id), "Id", typeof(Guid)),
            [
                new AnonymiseLiteralField(nameof(AnonymisedContact.EmailAddress), "EmailAddress", AnonymiseMethod.Null),
                new AnonymiseLiteralField(nameof(AnonymisedContact.GivenName), "GivenName", AnonymiseMethod.EmptyString),
                new AnonymiseLiteralField(
                    nameof(AnonymisedContact.Surname),
                    "Surname",
                    AnonymiseMethod.FixedLiteral,
                    "[redacted]"
                ),
            ],
            new TenantConvention(nameof(AnonymisedContact.TenantId), "TenantId"),
            null
        );
        var rule = new RetentionRule(TimeSpan.FromDays(30), Strategy.Anonymise);

        var affected = await strategy.PreviewEraseAsync(
            entry,
            rule,
            new ErasureSubjectPredicate(
                [new ErasureSubjectMatch(nameof(AnonymisedContact.Id), "SubjectId", subjectId)]
            ),
            new TenantContext(tenantId, "uk", new Dictionary<string, string>()),
            now,
            connection,
            CancellationToken.None
        );

        affected.Should().Be(1);
        connection.Commands.Should().ContainSingle();
        connection.Commands[0].AssignedTransaction.Should().BeNull();
        connection.Commands[0].CommandText.Should().Contain("SELECT COUNT(*)");
        connection.Commands[0].CommandText.Should().Contain("\"SubjectId\" = @subjectValue0");
        connection.Commands[0].CommandText.Should().Contain("\"CreatedAt\" < @cutoff");
        connection.Commands[0].CommandText.Should().Contain("NOT EXISTS");
        connection.Commands[0].CommandText.Should().NotContain("ANY(@candidateIds)");
        connection.Commands[0].CommandText.Should().NotContain("DELETE FROM");
        connection.Commands[0].CommandText.Should().NotContain("UPDATE ");
        connection.Commands[0].CommandText.Should().NotContain("FOR UPDATE");
        connection.Commands[0].Parameters.Count.Should().Be(5);
        GetParameterNames(connection.Commands[0]).Should().Equal(
            "subjectValue0",
            "cutoff",
            "tenantId",
            "holdTableName",
            "holdAsOf"
        );
        connection.Commands[0].Parameters["tenantId"].Value.Should().Be(tenantId);
        connection.Commands[0].Parameters["subjectValue0"].Value.Should().Be(subjectId);
        connection.Commands[0].Parameters["cutoff"].Value.Should().Be(now.AddDays(-30));
        connection.Commands[0].Parameters["holdTableName"].Value.Should().Be("anonymised_contacts");
        connection.Commands[0].Parameters["holdAsOf"].Value.Should().Be(now);
    }

    [Fact]
    public async Task EraseAsync_Uses_A_SetBased_Update_For_Literal_Fields_And_Subject_Filtered_Candidates()
    {
        var selectedId = Guid.NewGuid();
        var heldId = Guid.NewGuid();
        var tenantId = Guid.NewGuid();
        var subjectId = Guid.NewGuid();
        var now = new DateTimeOffset(2026, 4, 12, 12, 0, 0, TimeSpan.Zero);
        using var db = CreateCommandStrategyDbContext();
        var strategy = new AnonymiseSweepStrategy(db);
        var connection = new RecordingDbConnection();
        connection.EnqueueResultSet(selectedId, heldId);
        connection.EnqueueResultSet(selectedId);
        var transaction = connection.BeginTransaction();
        var entry = new RetentionEntry(
            typeof(AnonymisedContact),
            "anonymised_contacts",
            "anonymise",
            nameof(AnonymisedContact.CreatedAt),
            "CreatedAt",
            new RecordIdConvention(nameof(AnonymisedContact.Id), "Id", typeof(Guid)),
            [
                new AnonymiseLiteralField(nameof(AnonymisedContact.EmailAddress), "EmailAddress", AnonymiseMethod.Null),
                new AnonymiseLiteralField(nameof(AnonymisedContact.GivenName), "GivenName", AnonymiseMethod.EmptyString),
                new AnonymiseLiteralField(
                    nameof(AnonymisedContact.Surname),
                    "Surname",
                    AnonymiseMethod.FixedLiteral,
                    "[redacted]"
                ),
            ],
            new TenantConvention(nameof(AnonymisedContact.TenantId), "TenantId"),
            null
        );
        var rule = new RetentionRule(TimeSpan.FromDays(30), Strategy.Anonymise);

        var affected = await strategy.EraseAsync(
            entry,
            rule,
            new ErasureSubjectPredicate(
                [new ErasureSubjectMatch(nameof(AnonymisedContact.Id), "SubjectId", subjectId)]
            ),
            new TenantContext(tenantId, "uk", new Dictionary<string, string>()),
            now,
            connection,
            transaction,
            CancellationToken.None
        );

        affected.AffectedRecordIds.Should().Equal(selectedId.ToString());
        affected.HeldCount.Should().Be(1);
        connection.Commands.Should().HaveCount(2);
        connection.Commands[0].AssignedTransaction.Should().BeSameAs(transaction);
        connection.Commands[0].CommandText.Should().Contain("\"SubjectId\" = @subjectValue0");
        connection.Commands[0].CommandText.Should().Contain("\"CreatedAt\" < @cutoff");
        connection.Commands[0].CommandText.Should().Contain("FOR UPDATE");
        GetParameterNames(connection.Commands[0]).Should().Equal(
            "subjectValue0",
            "cutoff",
            "tenantId"
        );
        connection.Commands[0].Parameters["cutoff"].Value.Should().Be(now.AddDays(-30));
        connection.Commands[1].AssignedTransaction.Should().BeSameAs(transaction);
        connection.Commands[1].CommandText.Should().Contain("\"SubjectId\" = @subjectValue0");
        connection.Commands[1].CommandText.Should().Contain("\"CreatedAt\" < @cutoff");
        connection.Commands[1].CommandText.Should().Contain("ANY(@candidateIds)");
        connection.Commands[1].CommandText.Should().NotContain("WHERE CAST(target.\"Id\" AS text) = @recordId");
        GetParameterNames(connection.Commands[1]).Should().Equal(
            "value0",
            "value1",
            "value2",
            "subjectValue0",
            "cutoff",
            "tenantId",
            "candidateIds",
            "holdTableName",
            "holdAsOf"
        );
        connection.Commands[1].Parameters["subjectValue0"].Value.Should().Be(subjectId);
        connection.Commands[1].Parameters["cutoff"].Value.Should().Be(now.AddDays(-30));
        connection.Commands[1].Parameters["tenantId"].Value.Should().Be(tenantId);
        connection.Commands[1].Parameters["candidateIds"].Value.Should().BeEquivalentTo(
            new[] { selectedId.ToString(), heldId.ToString() }
        );
    }

    [Fact]
    public async Task SweepAsync_Uses_A_SetBased_Update_For_FactoryBacked_Fields_That_Do_Not_Require_Original_Values()
    {
        var selectedId = Guid.NewGuid();
        var otherSelectedId = Guid.NewGuid();
        using var db = CreateCommandStrategyDbContext();
        var strategy = new AnonymiseSweepStrategy(db, [new RecordingSetBasedFactory()]);
        var connection = new RecordingDbConnection();
        connection.EnqueueResultSet(selectedId, otherSelectedId);
        connection.EnqueueResultSet(selectedId, otherSelectedId);
        var transaction = connection.BeginTransaction();
        var tenantId = Guid.NewGuid();
        var now = new DateTimeOffset(2026, 4, 12, 12, 0, 0, TimeSpan.Zero);
        var entry = new RetentionEntry(
            typeof(CommandSetBasedFactoryRecord),
            "set_based_factory_sweep_records",
            "factory-backed-set-based",
            nameof(CommandSetBasedFactoryRecord.CreatedAt),
            "created_at_utc",
            new RecordIdConvention(nameof(CommandSetBasedFactoryRecord.Id), "Id", typeof(Guid)),
            [new AnonymiseFactoryField(nameof(CommandSetBasedFactoryRecord.ExternalId), "external_id", typeof(RecordingSetBasedFactory))],
            new TenantConvention(nameof(CommandSetBasedFactoryRecord.TenantId), "tenant_id"),
            null
        );
        var rule = new RetentionRule(TimeSpan.FromDays(30), Strategy.Anonymise);
        var context = new RetentionResolutionContext(
            "factory-backed-set-based",
            new TenantContext(tenantId, "uk", new Dictionary<string, string>()),
            now,
            []
        );

        var affected = await strategy.SweepAsync(
            entry,
            rule,
            context,
            connection,
            transaction,
            CancellationToken.None
        );

        affected.AffectedRecordIds.Should().HaveCount(2);
        connection.Commands.Should().HaveCount(2);
        connection.Commands[0].CommandText.Should().Contain("FOR UPDATE");
        GetParameterNames(connection.Commands[0]).Should().Equal("cutoff", "tenantId");
        connection.Commands[1].CommandText.Should().Contain("ANY(@candidateIds)");
        connection.Commands[1].CommandText.Should().NotContain("WHERE CAST(target.\"Id\" AS text) = @recordId");
        GetParameterNames(connection.Commands[1]).Should().Equal(
            "value0",
            "cutoff",
            "tenantId",
            "candidateIds",
            "holdTableName",
            "holdAsOf"
        );
        connection.Commands[1].Parameters["value0"].Value.Should().Be(RecordingSetBasedFactory.ScrubbedValue);
    }

    [Fact]
    public async Task EraseAsync_Uses_PerRow_Updates_For_Factories_That_Request_PerRow_Execution_Without_Original_Values()
    {
        var firstSelectedId = Guid.NewGuid();
        var secondSelectedId = Guid.NewGuid();
        using var db = CreateCommandStrategyDbContext();
        var strategy = new AnonymiseSweepStrategy(
            db,
            [new RecordingOriginalValueFactory(), new RecordingPerRowStringFactory()]
        );
        var connection = new RecordingDbConnection();
        connection.EnqueueResultSet(firstSelectedId, secondSelectedId);
        connection.EnqueueRowSet(
            ["Id", "external_id"],
            [firstSelectedId.ToString(), "alpha"],
            [secondSelectedId.ToString(), "beta"]
        );
        connection.EnqueueResultSet(firstSelectedId);
        connection.EnqueueResultSet(secondSelectedId);
        var transaction = connection.BeginTransaction();
        var tenantId = Guid.NewGuid();
        var subjectId = Guid.NewGuid();
        var now = new DateTimeOffset(2026, 4, 12, 12, 0, 0, TimeSpan.Zero);
        var entry = new RetentionEntry(
            typeof(CommandPerRowFactoryRecord),
            "per_row_factory_sweep_records",
            "factory-backed-per-row",
            nameof(CommandPerRowFactoryRecord.CreatedAt),
            "created_at_utc",
            new RecordIdConvention(nameof(CommandPerRowFactoryRecord.Id), "Id", typeof(Guid)),
            [
                new AnonymiseFactoryField(nameof(CommandPerRowFactoryRecord.ExternalId), "external_id", typeof(RecordingOriginalValueFactory)),
                new AnonymiseFactoryField(nameof(CommandPerRowFactoryRecord.DisplayName), "display_name", typeof(RecordingPerRowStringFactory)),
            ],
            new TenantConvention(nameof(CommandPerRowFactoryRecord.TenantId), "tenant_id"),
            null
        );
        var rule = new RetentionRule(TimeSpan.FromDays(30), Strategy.Anonymise);

        var affected = await strategy.EraseAsync(
            entry,
            rule,
            new ErasureSubjectPredicate(
                [new ErasureSubjectMatch("SubjectId", "subject_id", subjectId)]
            ),
            new TenantContext(tenantId, "uk", new Dictionary<string, string>()),
            now,
            connection,
            transaction,
            CancellationToken.None
        );

        affected.AffectedRecordIds.Should().Equal(firstSelectedId.ToString(), secondSelectedId.ToString());
        affected.HeldCount.Should().Be(0);
        connection.Commands.Should().HaveCount(4);
        connection.Commands[0].CommandText.Should().Contain("\"subject_id\" = @subjectValue0");
        connection.Commands[0].CommandText.Should().Contain("\"created_at_utc\" < @cutoff");
        GetParameterNames(connection.Commands[0]).Should().Equal(
            "subjectValue0",
            "cutoff",
            "tenantId"
        );
        connection.Commands[0].Parameters["cutoff"].Value.Should().Be(now.AddDays(-30));
        connection.Commands[1].CommandText.Should().Contain("\"external_id\"");
        connection.Commands[1].CommandText.Should().Contain("ANY(@candidateIds)");
        GetParameterNames(connection.Commands[1]).Should().Equal(
            "candidateIds",
            "tenantId",
            "holdTableName",
            "holdAsOf"
        );
        connection.Commands[2].CommandText.Should().Contain("WHERE CAST(target.\"Id\" AS text) = @recordId");
        connection.Commands[2].CommandText.Should().Contain("\"subject_id\" = @subjectValue0");
        connection.Commands[2].CommandText.Should().Contain("\"created_at_utc\" < @cutoff");
        GetParameterNames(connection.Commands[2]).Should().Equal(
            "value0",
            "value1",
            "recordId",
            "subjectValue0",
            "cutoff",
            "tenantId",
            "holdTableName",
            "holdAsOf"
        );
        connection.Commands[2].Parameters["cutoff"].Value.Should().Be(now.AddDays(-30));
        connection.Commands[2].Parameters["value0"].Value.Should().Be("alpha-scrubbed");
        connection.Commands[2].Parameters["value1"].Value.Should().Be("command-per-row-1");
        connection.Commands[3].CommandText.Should().Contain("\"subject_id\" = @subjectValue0");
        connection.Commands[3].CommandText.Should().Contain("\"created_at_utc\" < @cutoff");
        GetParameterNames(connection.Commands[3]).Should().Equal(
            "value0",
            "value1",
            "recordId",
            "subjectValue0",
            "cutoff",
            "tenantId",
            "holdTableName",
            "holdAsOf"
        );
        connection.Commands[3].Parameters["cutoff"].Value.Should().Be(now.AddDays(-30));
        connection.Commands[3].Parameters["value0"].Value.Should().Be("beta-scrubbed");
        connection.Commands[3].Parameters["value1"].Value.Should().Be("command-per-row-2");
    }

    [Fact]
    public async Task PreviewEraseAsync_Uses_An_Or_Subject_Predicate_When_Multiple_Subject_Matches_Are_Provided()
    {
        var tenantId = Guid.NewGuid();
        var firstSubjectId = Guid.NewGuid();
        var secondSubjectId = Guid.NewGuid();
        var now = new DateTimeOffset(2026, 4, 12, 12, 0, 0, TimeSpan.Zero);
        using var db = CreateCommandStrategyDbContext();
        var strategy = new AnonymiseSweepStrategy(db);
        var connection = new RecordingDbConnection();
        var entry = new RetentionEntry(
            typeof(AnonymisedContact),
            "anonymised_contacts",
            "anonymise",
            nameof(AnonymisedContact.CreatedAt),
            "CreatedAt",
            new RecordIdConvention(nameof(AnonymisedContact.Id), "Id", typeof(Guid)),
            [
                new AnonymiseLiteralField(nameof(AnonymisedContact.EmailAddress), "EmailAddress", AnonymiseMethod.Null),
                new AnonymiseLiteralField(nameof(AnonymisedContact.GivenName), "GivenName", AnonymiseMethod.EmptyString),
                new AnonymiseLiteralField(
                    nameof(AnonymisedContact.Surname),
                    "Surname",
                    AnonymiseMethod.FixedLiteral,
                    "[redacted]"
                ),
            ],
            new TenantConvention(nameof(AnonymisedContact.TenantId), "TenantId"),
            null
        );
        var rule = new RetentionRule(TimeSpan.FromDays(30), Strategy.Anonymise);

        var affected = await strategy.PreviewEraseAsync(
            entry,
            rule,
            new ErasureSubjectPredicate(
                [
                    new ErasureSubjectMatch(nameof(AnonymisedContact.Id), "SubjectId", firstSubjectId),
                    new ErasureSubjectMatch(nameof(AnonymisedContact.Id), "DelegateSubjectId", secondSubjectId),
                ]
            ),
            new TenantContext(tenantId, "uk", new Dictionary<string, string>()),
            now,
            connection,
            CancellationToken.None
        );

        affected.Should().Be(1);
        connection.Commands.Should().ContainSingle();
        connection.Commands[0].AssignedTransaction.Should().BeNull();
        connection.Commands[0].CommandText.Should().Contain("SELECT COUNT(*)");
        connection.Commands[0].CommandText.Should().Contain(
            "((target.\"SubjectId\" = @subjectValue0 OR target.\"DelegateSubjectId\" = @subjectValue1))"
        );
        connection.Commands[0].CommandText.Should().Contain("\"CreatedAt\" < @cutoff");
        connection.Commands[0].CommandText.Should().Contain("NOT EXISTS");
        connection.Commands[0].CommandText.Should().NotContain("ANY(@candidateIds)");
        connection.Commands[0].CommandText.Should().NotContain("DELETE FROM");
        connection.Commands[0].CommandText.Should().NotContain("UPDATE ");
        connection.Commands[0].CommandText.Should().NotContain("FOR UPDATE");
        GetParameterNames(connection.Commands[0]).Should().Equal(
            "subjectValue0",
            "subjectValue1",
            "cutoff",
            "tenantId",
            "holdTableName",
            "holdAsOf"
        );
        connection.Commands[0].Parameters["tenantId"].Value.Should().Be(tenantId);
        connection.Commands[0].Parameters["subjectValue0"].Value.Should().Be(firstSubjectId);
        connection.Commands[0].Parameters["subjectValue1"].Value.Should().Be(secondSubjectId);
        connection.Commands[0].Parameters["cutoff"].Value.Should().Be(now.AddDays(-30));
        connection.Commands[0].Parameters["holdTableName"].Value.Should().Be("anonymised_contacts");
        connection.Commands[0].Parameters["holdAsOf"].Value.Should().Be(now);
    }

    [Fact]
    public async Task EraseAsync_Uses_An_Or_Subject_Predicate_When_Multiple_Subject_Matches_Are_Provided()
    {
        var selectedId = Guid.NewGuid();
        var heldId = Guid.NewGuid();
        var tenantId = Guid.NewGuid();
        var firstSubjectId = Guid.NewGuid();
        var secondSubjectId = Guid.NewGuid();
        var now = new DateTimeOffset(2026, 4, 12, 12, 0, 0, TimeSpan.Zero);
        using var db = CreateCommandStrategyDbContext();
        var strategy = new AnonymiseSweepStrategy(db);
        var connection = new RecordingDbConnection();
        connection.EnqueueResultSet(selectedId, heldId);
        connection.EnqueueResultSet(selectedId);
        var transaction = connection.BeginTransaction();
        var entry = new RetentionEntry(
            typeof(AnonymisedContact),
            "anonymised_contacts",
            "anonymise",
            nameof(AnonymisedContact.CreatedAt),
            "CreatedAt",
            new RecordIdConvention(nameof(AnonymisedContact.Id), "Id", typeof(Guid)),
            [
                new AnonymiseLiteralField(nameof(AnonymisedContact.EmailAddress), "EmailAddress", AnonymiseMethod.Null),
                new AnonymiseLiteralField(nameof(AnonymisedContact.GivenName), "GivenName", AnonymiseMethod.EmptyString),
                new AnonymiseLiteralField(
                    nameof(AnonymisedContact.Surname),
                    "Surname",
                    AnonymiseMethod.FixedLiteral,
                    "[redacted]"
                ),
            ],
            new TenantConvention(nameof(AnonymisedContact.TenantId), "TenantId"),
            null
        );
        var rule = new RetentionRule(TimeSpan.FromDays(30), Strategy.Anonymise);

        var affected = await strategy.EraseAsync(
            entry,
            rule,
            new ErasureSubjectPredicate(
                [
                    new ErasureSubjectMatch(nameof(AnonymisedContact.Id), "SubjectId", firstSubjectId),
                    new ErasureSubjectMatch(nameof(AnonymisedContact.Id), "DelegateSubjectId", secondSubjectId),
                ]
            ),
            new TenantContext(tenantId, "uk", new Dictionary<string, string>()),
            now,
            connection,
            transaction,
            CancellationToken.None
        );

        affected.AffectedRecordIds.Should().Equal(selectedId.ToString());
        connection.Commands.Should().HaveCount(2);
        connection.Commands[0].AssignedTransaction.Should().BeSameAs(transaction);
        connection.Commands[0].CommandText.Should().Contain(
            "((target.\"SubjectId\" = @subjectValue0 OR target.\"DelegateSubjectId\" = @subjectValue1))"
        );
        connection.Commands[0].CommandText.Should().Contain("\"CreatedAt\" < @cutoff");
        connection.Commands[0].CommandText.Should().Contain("FOR UPDATE");
        GetParameterNames(connection.Commands[0]).Should().Equal(
            "subjectValue0",
            "subjectValue1",
            "cutoff",
            "tenantId"
        );
        connection.Commands[0].Parameters["tenantId"].Value.Should().Be(tenantId);
        connection.Commands[0].Parameters["cutoff"].Value.Should().Be(now.AddDays(-30));
        connection.Commands[1].CommandText.Should().Contain(
            "((target.\"SubjectId\" = @subjectValue0 OR target.\"DelegateSubjectId\" = @subjectValue1))"
        );
        connection.Commands[1].AssignedTransaction.Should().BeSameAs(transaction);
        connection.Commands[1].CommandText.Should().Contain("\"CreatedAt\" < @cutoff");
        connection.Commands[1].CommandText.Should().Contain("ANY(@candidateIds)");
        connection.Commands[1].CommandText.Should().Contain("NOT EXISTS");
        GetParameterNames(connection.Commands[1]).Should().Equal(
            "value0",
            "value1",
            "value2",
            "subjectValue0",
            "subjectValue1",
            "cutoff",
            "tenantId",
            "candidateIds",
            "holdTableName",
            "holdAsOf"
        );
        connection.Commands[1].Parameters["tenantId"].Value.Should().Be(tenantId);
        connection.Commands[0].Parameters["subjectValue0"].Value.Should().Be(firstSubjectId);
        connection.Commands[0].Parameters["subjectValue1"].Value.Should().Be(secondSubjectId);
        connection.Commands[1].Parameters["subjectValue0"].Value.Should().Be(firstSubjectId);
        connection.Commands[1].Parameters["subjectValue1"].Value.Should().Be(secondSubjectId);
        connection.Commands[1].Parameters["cutoff"].Value.Should().Be(now.AddDays(-30));
        connection.Commands[1].Parameters["candidateIds"].Value.Should().BeEquivalentTo(
            new[] { selectedId.ToString(), heldId.ToString() }
        );
        connection.Commands[1].Parameters["holdTableName"].Value.Should().Be("anonymised_contacts");
        connection.Commands[1].Parameters["holdAsOf"].Value.Should().Be(now);
    }

    private static CommandStrategyDbContext CreateCommandStrategyDbContext()
    {
        var options = new DbContextOptionsBuilder<CommandStrategyDbContext>()
            .UseInMemoryDatabase(Guid.NewGuid().ToString("N"))
            .Options;

        return new CommandStrategyDbContext(options);
    }

    private static string[] GetParameterNames(RecordingDbCommand command)
    {
        return command.Parameters.Cast<DbParameter>().Select(parameter => parameter.ParameterName).ToArray();
    }

    private sealed class CommandStrategyDbContext(DbContextOptions<CommandStrategyDbContext> options)
        : DbContext(options)
    {
        protected override void OnModelCreating(ModelBuilder modelBuilder)
        {
            modelBuilder.Entity<AnonymisedContact>(entity =>
            {
                entity.ToTable("anonymised_contacts");
                entity.HasKey(contact => contact.Id);
                entity.Property(contact => contact.TenantId);
                entity.Property(contact => contact.CreatedAt);
                entity.Property(contact => contact.EmailAddress);
                entity.Property(contact => contact.GivenName);
                entity.Property(contact => contact.Surname);
                entity.Property(contact => contact.Notes);
            });

            modelBuilder.Entity<CommandSetBasedFactoryRecord>(entity =>
            {
                entity.ToTable("set_based_factory_sweep_records");
                entity.HasKey(record => record.Id);
                entity.Property(record => record.TenantId);
                entity.Property(record => record.CreatedAt);
                entity.Property(record => record.ExternalId);
            });

            modelBuilder.Entity<CommandPerRowFactoryRecord>(entity =>
            {
                entity.ToTable("per_row_factory_sweep_records");
                entity.HasKey(record => record.Id);
                entity.Property(record => record.TenantId);
                entity.Property(record => record.CreatedAt);
                entity.Property(record => record.ExternalId);
                entity.Property(record => record.DisplayName);
            });
        }
    }

    private sealed class CommandSetBasedFactoryRecord
    {
        public Guid Id { get; set; }
        public Guid TenantId { get; set; }
        public DateTimeOffset CreatedAt { get; set; }
        public Guid ExternalId { get; set; }
    }

    private sealed class CommandPerRowFactoryRecord
    {
        public Guid Id { get; set; }
        public Guid TenantId { get; set; }
        public DateTimeOffset CreatedAt { get; set; }
        public string ExternalId { get; set; } = "";
        public string DisplayName { get; set; } = "";
    }

    private sealed class RecordingDbConnection : DbConnection
    {
        private ConnectionState state = ConnectionState.Closed;
        private string connectionString = "Host=recording";
        private readonly Queue<RecordingResultSet> queuedResultSets = new();

        public RecordingDbCommand? LastCommand { get; private set; }
        public List<RecordingDbCommand> Commands { get; } = [];

        public void EnqueueResultSet(params Guid[] values)
        {
            queuedResultSets.Enqueue(
                new RecordingResultSet(
                    ["Id"],
                    values.Select(value => new object?[] { value }).ToArray()
                )
            );
        }

        public void EnqueueRowSet(string[] columnNames, params object?[][] rows)
        {
            queuedResultSets.Enqueue(new RecordingResultSet(columnNames, rows));
        }

        [AllowNull]
        public override string ConnectionString
        {
            get => connectionString;
            set => connectionString = value ?? "";
        }

        public override string Database => "recording";

        public override string DataSource => "recording";

        public override string ServerVersion => "1.0";

        public override ConnectionState State => state;

        public override void ChangeDatabase(string databaseName) { }

        public override void Close()
        {
            state = ConnectionState.Closed;
        }

        public override void Open()
        {
            state = ConnectionState.Open;
        }

        protected override DbTransaction BeginDbTransaction(IsolationLevel isolationLevel)
        {
            state = ConnectionState.Open;
            return new RecordingDbTransaction(this);
        }

        protected override DbCommand CreateDbCommand()
        {
            LastCommand = new RecordingDbCommand(this);
            Commands.Add(LastCommand);
            return LastCommand;
        }

        public RecordingResultSet DequeueResultSet()
        {
            return queuedResultSets.Count > 0
                ? queuedResultSets.Dequeue()
                : new RecordingResultSet(["Id"], [new object?[] { Guid.NewGuid() }]);
        }
    }

    private sealed class RecordingDbTransaction(RecordingDbConnection connection) : DbTransaction
    {
        public override IsolationLevel IsolationLevel => IsolationLevel.ReadCommitted;

        protected override DbConnection? DbConnection => connection;

        public override void Commit() { }

        public override void Rollback() { }
    }

    private sealed class RecordingDbCommand(RecordingDbConnection connection) : DbCommand
    {
        private readonly RecordingDbParameterCollection parameters = new();
        private string commandText = "";

        public DbTransaction? AssignedTransaction { get; private set; }

        [AllowNull]
        public override string CommandText
        {
            get => commandText;
            set => commandText = value ?? "";
        }

        public override int CommandTimeout { get; set; }

        public override CommandType CommandType { get; set; } = CommandType.Text;

        protected override DbConnection? DbConnection { get; set; } = connection;

        protected override DbParameterCollection DbParameterCollection => parameters;

        protected override DbTransaction? DbTransaction
        {
            get => AssignedTransaction;
            set => AssignedTransaction = value;
        }

        public override bool DesignTimeVisible { get; set; }

        public override UpdateRowSource UpdatedRowSource { get; set; }

        public override void Cancel() { }

        public override int ExecuteNonQuery()
        {
            return 1;
        }

        public override object? ExecuteScalar()
        {
            return 1;
        }

        public override void Prepare() { }

        protected override DbParameter CreateDbParameter()
        {
            return new RecordingDbParameter();
        }

        protected override DbDataReader ExecuteDbDataReader(CommandBehavior behavior)
        {
            return new RecordingDbDataReader(connection.DequeueResultSet());
        }

        public override Task<int> ExecuteNonQueryAsync(CancellationToken cancellationToken)
        {
            return Task.FromResult(1);
        }
    }

    private sealed class RecordingDbDataReader(RecordingResultSet resultSet) : DbDataReader
    {
        private int index = -1;

        public override int FieldCount => resultSet.ColumnNames.Length;
        public override bool HasRows => resultSet.Rows.Length > 0;
        public override bool IsClosed => false;
        public override int RecordsAffected => 1;
        public override int Depth => 0;
        public override object this[int ordinal] => GetValue(ordinal);
        public override object this[string name] => GetValue(GetOrdinal(name));

        public override bool Read()
        {
            if (index + 1 >= resultSet.Rows.Length)
            {
                return false;
            }

            index++;
            return true;
        }

        public override Task<bool> ReadAsync(CancellationToken cancellationToken) => Task.FromResult(Read());
        public override bool NextResult() => false;
        public override Task<bool> NextResultAsync(CancellationToken cancellationToken) => Task.FromResult(false);
        public override Guid GetGuid(int ordinal) => (Guid)GetValue(ordinal);
        public override object GetValue(int ordinal) => resultSet.Rows[index][ordinal]!;
        public override int GetValues(object[] items)
        {
            Array.Copy(resultSet.Rows[index], items, resultSet.Rows[index].Length);
            return resultSet.Rows[index].Length;
        }

        public override string GetName(int ordinal) => resultSet.ColumnNames[ordinal];
        public override string GetDataTypeName(int ordinal) => GetFieldType(ordinal).Name;
        public override Type GetFieldType(int ordinal) => resultSet.Rows[index][ordinal]?.GetType() ?? typeof(DBNull);
        public override int GetOrdinal(string name) => Array.IndexOf(resultSet.ColumnNames, name);
        public override bool IsDBNull(int ordinal) => resultSet.Rows[index][ordinal] is null or DBNull;
        public override IEnumerator GetEnumerator() => resultSet.Rows.GetEnumerator();

        public override bool GetBoolean(int ordinal) => throw new NotSupportedException();
        public override byte GetByte(int ordinal) => throw new NotSupportedException();
        public override long GetBytes(int ordinal, long dataOffset, byte[]? buffer, int bufferOffset, int length) => throw new NotSupportedException();
        public override char GetChar(int ordinal) => throw new NotSupportedException();
        public override long GetChars(int ordinal, long dataOffset, char[]? buffer, int bufferOffset, int length) => throw new NotSupportedException();
        public override string GetString(int ordinal) => (string)GetValue(ordinal);
        public override short GetInt16(int ordinal) => throw new NotSupportedException();
        public override int GetInt32(int ordinal) => throw new NotSupportedException();
        public override long GetInt64(int ordinal) => throw new NotSupportedException();
        public override float GetFloat(int ordinal) => throw new NotSupportedException();
        public override double GetDouble(int ordinal) => throw new NotSupportedException();
        public override decimal GetDecimal(int ordinal) => throw new NotSupportedException();
        public override DateTime GetDateTime(int ordinal) => throw new NotSupportedException();
    }

    private sealed record RecordingResultSet(string[] ColumnNames, object?[][] Rows);

    private sealed class RecordingSetBasedFactory : IAnonymiseValueFactory
    {
        public static readonly Guid ScrubbedValue = Guid.Parse("22222222-2222-2222-2222-222222222222");

        public object? Create(AnonymiseValueContext context) => ScrubbedValue;
    }

    private sealed class RecordingPerRowStringFactory : IAnonymiseValueFactory
    {
        public bool RequiresPerRowExecution => true;
        private int sequence = 0;

        public object? Create(AnonymiseValueContext context)
        {
            sequence++;
            return $"command-per-row-{sequence}";
        }
    }

    private sealed class RecordingOriginalValueFactory : IAnonymiseValueFactory
    {
        public bool RequiresOriginalValue => true;

        public object? Create(AnonymiseValueContext context) => $"{context.OriginalValue}-scrubbed";
    }

    private sealed class RecordingDbParameter : DbParameter
    {
        private string parameterName = "";
        private string sourceColumn = "";

        public override DbType DbType { get; set; }

        public override ParameterDirection Direction { get; set; } = ParameterDirection.Input;

        public override bool IsNullable { get; set; }

        [AllowNull]
        public override string ParameterName
        {
            get => parameterName;
            set => parameterName = value ?? "";
        }

        [AllowNull]
        public override string SourceColumn
        {
            get => sourceColumn;
            set => sourceColumn = value ?? "";
        }

        public override object? Value { get; set; }

        public override bool SourceColumnNullMapping { get; set; }

        public override int Size { get; set; }

        public override void ResetDbType() { }
    }

    private sealed class RecordingDbParameterCollection : DbParameterCollection
    {
        private readonly List<DbParameter> items = [];

        public override int Count => items.Count;

        public override object SyncRoot => this;

        public override int Add(object value)
        {
            items.Add((DbParameter)value);
            return items.Count - 1;
        }

        public override void AddRange(Array values)
        {
            foreach (var value in values)
            {
                Add(value!);
            }
        }

        public override void Clear()
        {
            items.Clear();
        }

        public override bool Contains(object value)
        {
            return items.Contains((DbParameter)value);
        }

        public override bool Contains(string value)
        {
            return items.Any(parameter => parameter.ParameterName == value);
        }

        public override void CopyTo(Array array, int index)
        {
            items.ToArray().CopyTo(array, index);
        }

        public override IEnumerator GetEnumerator()
        {
            return items.GetEnumerator();
        }

        public override int IndexOf(object value)
        {
            return items.IndexOf((DbParameter)value);
        }

        public override int IndexOf(string parameterName)
        {
            return items.FindIndex(parameter => parameter.ParameterName == parameterName);
        }

        public override void Insert(int index, object value)
        {
            items.Insert(index, (DbParameter)value);
        }

        public override void Remove(object value)
        {
            items.Remove((DbParameter)value);
        }

        public override void RemoveAt(int index)
        {
            items.RemoveAt(index);
        }

        public override void RemoveAt(string parameterName)
        {
            var index = IndexOf(parameterName);
            if (index >= 0)
            {
                items.RemoveAt(index);
            }
        }

        protected override DbParameter GetParameter(int index)
        {
            return items[index];
        }

        protected override DbParameter GetParameter(string parameterName)
        {
            return items[IndexOf(parameterName)];
        }

        protected override void SetParameter(int index, DbParameter value)
        {
            items[index] = value;
        }

        protected override void SetParameter(string parameterName, DbParameter value)
        {
            var index = IndexOf(parameterName);
            if (index >= 0)
            {
                items[index] = value;
                return;
            }

            items.Add(value);
        }
    }
}
