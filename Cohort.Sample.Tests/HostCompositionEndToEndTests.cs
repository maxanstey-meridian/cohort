using Cohort.Application;
using Cohort.Domain;
using Cohort.Hosting;
using Cohort.Infrastructure;
using Cohort.Infrastructure.Handlers;
using Cohort.Infrastructure.Migrations;

using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;

using Npgsql;

namespace Cohort.Sample.Tests;

[Collection("Integration")]
public sealed class HostCompositionEndToEndTests(PostgresFixture fixture)
{
    [Fact]
    public void AddCohort_Registers_Scope_Owned_Public_Facades_And_Scoped_Executors()
    {
        var services = new ServiceCollection();

        services.AddCohort<ValidRetentionDbContext>();

        services
            .Should()
            .ContainSingle(descriptor =>
                descriptor.ServiceType == typeof(IRetentionSweep)
                && descriptor.ImplementationType == typeof(ScopeOwnedRetentionSweep)
                && descriptor.Lifetime == ServiceLifetime.Singleton
            );
        services
            .Should()
            .ContainSingle(descriptor =>
                descriptor.ServiceType == typeof(IRetentionPreview)
                && descriptor.ImplementationType == typeof(ScopeOwnedRetentionPreview)
                && descriptor.Lifetime == ServiceLifetime.Singleton
            );
        services
            .Should()
            .ContainSingle(descriptor =>
                descriptor.ServiceType == typeof(IRetentionErasureService)
                && descriptor.ImplementationType == typeof(ScopeOwnedRetentionErasureService)
                && descriptor.Lifetime == ServiceLifetime.Singleton
            );
        services
            .Should()
            .ContainSingle(descriptor =>
                descriptor.ServiceType == typeof(RetentionSweepEngine)
                && descriptor.Lifetime == ServiceLifetime.Scoped
            );
        typeof(RetentionPreviewService).GetInterfaces().Should().NotContain(typeof(IRetentionPreview));
        typeof(RetentionErasureService)
            .GetInterfaces()
            .Should()
            .NotContain(typeof(IRetentionErasureService));
    }

    [Fact]
    public async Task Public_Request_Facades_Reject_Null_Before_Delegation()
    {
        var services = new ServiceCollection();
        services.AddCohort<ValidRetentionDbContext>();
        await using var provider = services.BuildServiceProvider();

        var sweep = provider.GetRequiredService<IRetentionSweep>();
        var preview = provider.GetRequiredService<IRetentionPreview>();

        var sweepAct = async () => await sweep.ExecuteAsync(null!);
        var previewAct = async () => await preview.ExecuteAsync(null!);

        await sweepAct.Should().ThrowAsync<ArgumentNullException>().WithParameterName("request");
        await previewAct.Should().ThrowAsync<ArgumentNullException>().WithParameterName("request");
    }

    [Fact]
    public async Task Public_Erasure_Facade_Rejects_Null_Before_Delegation()
    {
        var services = new ServiceCollection();
        services.AddCohort<ValidRetentionDbContext>();
        await using var provider = services.BuildServiceProvider();
        var erasure = provider.GetRequiredService<IRetentionErasureService>();
        var now = DateTimeOffset.UtcNow;
        var tenant = new TenantContext(Guid.NewGuid(), "uk", new Dictionary<string, string>());
        var scope = new ErasureScope(Guid.NewGuid());

        var tenantAct = async () => await erasure.EraseAsync(null!, scope, now);
        var scopeAct = async () => await erasure.EraseAsync(tenant, null!, now);

        await tenantAct.Should().ThrowAsync<ArgumentNullException>().WithParameterName("tenant");
        await scopeAct.Should().ThrowAsync<ArgumentNullException>().WithParameterName("scope");
    }

    [Fact]
    public async Task StartAsync_Rejects_Legacy_Cohort_Schema_When_Host_Migrations_Are_Not_Applied()
    {
        await using var database = await TemporaryDatabase.CreateAsync(fixture.ConnectionString);
        await LegacyCohortSchema.BootstrapPreRowDispatchAsync(database.ConnectionString);
        using var host = BuildHost<ValidRetentionDbContext>(
            options => options.UseNpgsql(database.ConnectionString),
            new SingleCategoryRepository(
                "valid",
                new RetentionRule(
                    TimeSpan.FromDays(30),
                    Strategy.Purge,
                    AuditRowDetail: AuditRowDetail.PerRow
                )
            )
        );

        var act = async () => await host.StartAsync();

        var exception = await act.Should().ThrowAsync<RetentionConfigurationException>();
        exception
            .Which.Errors.Should()
            .ContainSingle(error =>
                error.Contains("sweep_run.\"Status\"", StringComparison.Ordinal)
                && error.Contains(
                    "sweep_run_row_detail.\"RetentionEntityId\"",
                    StringComparison.Ordinal
                )
                && error.Contains("sweep_row_handler_status", StringComparison.Ordinal)
                && error.Contains("pending EF Core migrations", StringComparison.Ordinal)
            );
    }

    [Fact]
    public async Task StartAsync_Accepts_Current_Cohort_Schema()
    {
        using var host = BuildHost<ValidRetentionDbContext>(
            options => options.UseNpgsql(fixture.ConnectionString),
            new SingleCategoryRepository(
                "valid",
                new RetentionRule(TimeSpan.FromDays(30), Strategy.Purge)
            )
        );

        await host.StartAsync();
        await host.StopAsync();
    }

    [Fact]
    public async Task Sample_Migrates_A_Fresh_Database_Before_Host_Schema_Validation()
    {
        await using var database = await TemporaryDatabase.CreateAsync(fixture.ConnectionString);
        var builder = Host.CreateApplicationBuilder();
        builder.Configuration.AddInMemoryCollection(new Dictionary<string, string?>
        {
            [$"{SampleOptions.SectionName}:{nameof(SampleOptions.ConnectionString)}"] =
                database.ConnectionString,
        });
        builder.Services.AddSampleRetentionServices();
        using var host = builder.Build();

        await using (var scope = host.Services.CreateAsyncScope())
        {
            var db = scope.ServiceProvider.GetRequiredService<SampleDbContext>();
            await db.Database.MigrateAsync();
        }

        await host.StartAsync();
        await host.StopAsync();
    }

    [Fact]
    public async Task StartAsync_Accepts_Unconstrained_Varchar_For_Runtime_String_Columns()
    {
        await using var database = await TemporaryDatabase.CreateAsync(fixture.ConnectionString);
        await BootstrapCurrentSchemaAsync(database.ConnectionString);
        await ExecuteSqlAsync(database.ConnectionString, """
            ALTER TABLE public.retention_holds
                ALTER COLUMN "RecordId" TYPE varchar;
            ALTER TABLE public.sweep_run_entity_summary
                ALTER COLUMN "EntityType" TYPE varchar,
                ALTER COLUMN "Category" TYPE varchar;
            ALTER TABLE public.sweep_run_row_detail
                ALTER COLUMN "EntityType" TYPE varchar,
                ALTER COLUMN "EntityId" TYPE varchar,
                ALTER COLUMN "Category" TYPE varchar;
            ALTER TABLE public.sweep_row_handler_status
                ALTER COLUMN "HandlerType" TYPE varchar;
            """);
        using var host = BuildHost<ValidRetentionDbContext>(
            options => options.UseNpgsql(database.ConnectionString),
            new SingleCategoryRepository(
                "valid",
                new RetentionRule(TimeSpan.FromDays(30), Strategy.Purge)
            )
        );

        await host.StartAsync();
        await host.StopAsync();
    }

    [Fact]
    public async Task StartAsync_Accepts_Covering_Indexes_With_Equivalent_Key_Attributes()
    {
        await using var database = await TemporaryDatabase.CreateAsync(fixture.ConnectionString);
        await BootstrapCurrentSchemaAsync(database.ConnectionString);
        await ExecuteSqlAsync(database.ConnectionString, """
            DROP INDEX public."IX_retention_holds_RetentionEntityId_TenantId_RecordId";
            CREATE INDEX "IX_retention_holds_RetentionEntityId_TenantId_RecordId"
                ON public.retention_holds ("RetentionEntityId", "TenantId", "RecordId")
                INCLUDE ("Reason");
            """);
        using var host = BuildHost<ValidRetentionDbContext>(
            options => options.UseNpgsql(database.ConnectionString),
            new SingleCategoryRepository(
                "valid",
                new RetentionRule(TimeSpan.FromDays(30), Strategy.Purge)
            )
        );

        await host.StartAsync();
        await host.StopAsync();
    }

    [Fact]
    public async Task StartAsync_Uses_SearchPath_Resolution_And_Accepts_Equivalent_Index_Names()
    {
        await using var database = await TemporaryDatabase.CreateAsync(fixture.ConnectionString);
        await BootstrapCurrentSchemaAsync(database.ConnectionString);
        await ExecuteSqlAsync(database.ConnectionString, """
            CREATE SCHEMA host_default;
            CREATE SCHEMA cohort_runtime;
            ALTER TABLE public.retention_holds SET SCHEMA cohort_runtime;
            ALTER TABLE public.sweep_run SET SCHEMA cohort_runtime;
            ALTER TABLE public.sweep_run_entity_summary SET SCHEMA cohort_runtime;
            ALTER TABLE public.sweep_run_row_detail SET SCHEMA cohort_runtime;
            ALTER TABLE public.sweep_row_handler_status SET SCHEMA cohort_runtime;
            ALTER INDEX cohort_runtime."IX_sweep_run_row_detail_StableIdentity"
                RENAME TO host_equivalent_stable_identity;
            """);
        var connectionString = new NpgsqlConnectionStringBuilder(database.ConnectionString)
        {
            SearchPath = "host_default, cohort_runtime, public",
        }.ConnectionString;
        using var host = BuildHost<ValidRetentionDbContext>(
            options => options.UseNpgsql(connectionString),
            new SingleCategoryRepository(
                "valid",
                new RetentionRule(TimeSpan.FromDays(30), Strategy.Purge)
            )
        );

        await host.StartAsync();
        await host.StopAsync();
    }

    [Fact]
    public async Task StartAsync_Rejects_A_Named_Index_With_The_Wrong_Structure()
    {
        await using var database = await TemporaryDatabase.CreateAsync(fixture.ConnectionString);
        await BootstrapCurrentSchemaAsync(database.ConnectionString);
        await ExecuteSqlAsync(database.ConnectionString, """
            DROP INDEX public."IX_sweep_run_row_detail_StableIdentity";
            CREATE UNIQUE INDEX "IX_sweep_run_row_detail_StableIdentity"
                ON public.sweep_run_row_detail ("SweepId")
                ;
            """);
        using var host = BuildHost<ValidRetentionDbContext>(
            options => options.UseNpgsql(database.ConnectionString),
            new SingleCategoryRepository(
                "valid",
                new RetentionRule(TimeSpan.FromDays(30), Strategy.Purge)
            )
        );

        var act = async () => await host.StartAsync();

        var exception = await act.Should().ThrowAsync<RetentionConfigurationException>();
        exception.Which.Errors.Should().ContainSingle(error =>
            error.Contains("sweep_run_row_detail(SweepId, RetentionEntityId, EntityId, Category, Strategy, TenantId)", StringComparison.Ordinal)
        );
    }

    [Fact]
    public async Task StartAsync_Rejects_An_Indexed_Column_With_The_Wrong_Type_And_Nullability()
    {
        await using var database = await TemporaryDatabase.CreateAsync(fixture.ConnectionString);
        await BootstrapCurrentSchemaAsync(database.ConnectionString);
        await ExecuteSqlAsync(database.ConnectionString, """
            ALTER TABLE public.sweep_run_entity_summary
                ALTER COLUMN "RetentionEntityId" TYPE text USING "RetentionEntityId"::text;
            """);
        using var host = BuildHost<ValidRetentionDbContext>(
            options => options.UseNpgsql(database.ConnectionString),
            new SingleCategoryRepository(
                "valid",
                new RetentionRule(TimeSpan.FromDays(30), Strategy.Purge)
            )
        );

        var act = async () => await host.StartAsync();

        var exception = await act.Should().ThrowAsync<RetentionConfigurationException>();
        exception.Which.Errors.Should().ContainSingle(error =>
            error.Contains("sweep_run_entity_summary.\"RetentionEntityId\" uuid NOT NULL", StringComparison.Ordinal)
        );
    }

    [Theory]
    [InlineData(
        "ALTER TABLE public.sweep_run DROP CONSTRAINT \"CK_sweep_run_Status_Range\", DROP CONSTRAINT \"CK_sweep_run_Started_Unsettled\", DROP CONSTRAINT \"CK_sweep_run_Terminal_Settled\"; ALTER TABLE public.sweep_run ALTER COLUMN \"Status\" TYPE text USING \"Status\"::text",
        "sweep_run.\"Status\" int4 NOT NULL"
    )]
    [InlineData(
        "ALTER TABLE public.sweep_run ALTER COLUMN \"Status\" DROP NOT NULL",
        "sweep_run.\"Status\" int4 NOT NULL"
    )]
    [InlineData(
        "ALTER TABLE public.sweep_row_handler_status DROP CONSTRAINT \"CK_sweep_row_handler_status_Claim\"; ALTER TABLE public.sweep_row_handler_status ALTER COLUMN \"ClaimToken\" TYPE text USING \"ClaimToken\"::text",
        "sweep_row_handler_status.\"ClaimToken\" uuid NULL"
    )]
    [InlineData(
        "ALTER TABLE public.sweep_row_handler_status ALTER COLUMN \"ClaimToken\" SET NOT NULL",
        "sweep_row_handler_status.\"ClaimToken\" uuid NULL"
    )]
    public async Task StartAsync_Rejects_Wrong_Runtime_Column_Structure(
        string alterColumnSql,
        string expectedCapability
    )
    {
        await using var database = await TemporaryDatabase.CreateAsync(fixture.ConnectionString);
        await BootstrapCurrentSchemaAsync(database.ConnectionString);
        await ExecuteSqlAsync(database.ConnectionString, alterColumnSql);
        using var host = BuildHost<ValidRetentionDbContext>(
            options => options.UseNpgsql(database.ConnectionString),
            new SingleCategoryRepository(
                "valid",
                new RetentionRule(TimeSpan.FromDays(30), Strategy.Purge)
            )
        );

        var act = async () => await host.StartAsync();

        var exception = await act.Should().ThrowAsync<RetentionConfigurationException>();
        exception.Which.Errors.Should().ContainSingle(error =>
            error.Contains(expectedCapability, StringComparison.Ordinal)
        );
    }

    [Fact]
    public async Task StartAsync_Validates_The_Retention_Model_When_Schedule_Is_Absent()
    {
        using var host = BuildHost<InvalidRetentionDbContext>(
            options => options.UseNpgsql(fixture.ConnectionString),
            new SingleCategoryRepository(
                "invalid",
                new RetentionRule(TimeSpan.FromDays(30), Strategy.Purge)
            )
        );

        var act = async () => await host.StartAsync();

        var exception = await act.Should().ThrowAsync<RetentionConfigurationException>();
        exception
            .Which.Errors.Should()
            .ContainSingle(error => error.StartsWith("Tenant convention on "));
    }

    [Fact]
    public async Task StartAsync_Uses_The_Context_Selected_By_AddCohort_When_An_Untyped_Context_Exists()
    {
        using var host = BuildHost<ValidRetentionDbContext>(
            options => options.UseNpgsql(fixture.ConnectionString),
            new SingleCategoryRepository(
                "valid",
                new RetentionRule(TimeSpan.FromDays(30), Strategy.Purge)
            ),
            configureServices: services =>
            {
                services.AddDbContext<InvalidRetentionDbContext>(options =>
                    options.UseNpgsql(fixture.ConnectionString)
                );
                services.AddScoped<DbContext>(sp =>
                    sp.GetRequiredService<InvalidRetentionDbContext>()
                );
            }
        );

        var act = async () => await host.StartAsync();

        await act.Should().NotThrowAsync();
        await host.StopAsync();
    }

    [Fact]
    public void AddCohort_Is_Idempotent_For_The_Same_Context()
    {
        var services = new ServiceCollection();
        services.AddLogging();
        services.AddSingleton<IConfiguration>(new ConfigurationBuilder().Build());

        services.AddCohort<ValidRetentionDbContext>();
        var relevantServiceTypes = new[]
        {
            typeof(IHostedService),
            typeof(IRetentionRowDispatcher),
            typeof(RetentionRowDispatcher),
        };
        var descriptorsBeforeSecondCall = services
            .Where(descriptor => relevantServiceTypes.Contains(descriptor.ServiceType))
            .ToArray();
        var act = () => services.AddCohort<ValidRetentionDbContext>();

        act.Should().NotThrow();
        var descriptorsAfterSecondCall = services
            .Where(descriptor => relevantServiceTypes.Contains(descriptor.ServiceType))
            .ToArray();
        descriptorsAfterSecondCall.Should().Equal(descriptorsBeforeSecondCall);

        var hostedServiceDescriptors = descriptorsAfterSecondCall
            .Where(descriptor => descriptor.ServiceType == typeof(IHostedService))
            .ToArray();
        hostedServiceDescriptors
            .Should()
            .ContainSingle(descriptor =>
                descriptor.ImplementationType == typeof(RetentionValidationHostedService)
            );
        hostedServiceDescriptors
            .Should()
            .ContainSingle(descriptor => descriptor.ImplementationType == typeof(RetentionWorker));
        hostedServiceDescriptors
            .Should()
            .ContainSingle(descriptor => descriptor.ImplementationFactory != null);

        using var provider = services.BuildServiceProvider();
        var dispatcher = provider.GetRequiredService<IRetentionRowDispatcher>();
        provider.GetRequiredService<RetentionRowDispatcher>().Should().BeSameAs(dispatcher);
        provider
            .GetServices<IHostedService>()
            .Single(service => service is IRetentionRowDispatcher)
            .Should()
            .BeSameAs(dispatcher);
    }

    [Fact]
    public void AddCohort_Execution_Settings_Track_Configuration_Reloads()
    {
        var initialValues = new Dictionary<string, string?>
        {
            [$"{CohortOptions.SectionName}:DryRun"] = "false",
            [$"{CohortOptions.SectionName}:SweepBatchSize"] = "100",
            [$"{CohortOptions.SectionName}:RowHandlerDispatch:PollInterval"] = "00:00:01",
            [$"{CohortOptions.SectionName}:RowHandlerDispatch:PayloadRetention"] = "2.00:00:00",
            [$"{CohortOptions.SectionName}:RowHandlerDispatch:BatchSize"] = "20",
            [$"{CohortOptions.SectionName}:RowHandlerDispatch:MaxParallelism"] = "2",
            [$"{CohortOptions.SectionName}:RowHandlerDispatch:MaxAttempts"] = "3",
            [$"{CohortOptions.SectionName}:RowHandlerDispatch:BaseBackoff"] = "00:00:02",
            [$"{CohortOptions.SectionName}:RowHandlerDispatch:ClaimTimeout"] = "00:01:00",
        };
        var configuration = new ConfigurationBuilder()
            .AddInMemoryCollection(initialValues)
            .Build();
        var services = new ServiceCollection();
        services.AddLogging();
        services.AddSingleton<IConfiguration>(configuration);
        services.AddCohort<ValidRetentionDbContext>();
        using var provider = services.BuildServiceProvider();
        var settings = provider.GetRequiredService<IRetentionExecutionSettings>();

        settings.DryRun.Should().BeFalse();
        settings.SweepBatchSize.Should().Be(100);
        settings.RowHandlerDispatch.PollInterval.Should().Be(TimeSpan.FromSeconds(1));
        settings.RowHandlerDispatch.PayloadRetention.Should().Be(TimeSpan.FromDays(2));
        settings.RowHandlerDispatch.BatchSize.Should().Be(20);
        settings.RowHandlerDispatch.MaxParallelism.Should().Be(2);
        settings.RowHandlerDispatch.MaxAttempts.Should().Be(3);
        settings.RowHandlerDispatch.BaseBackoff.Should().Be(TimeSpan.FromSeconds(2));
        settings.RowHandlerDispatch.ClaimTimeout.Should().Be(TimeSpan.FromMinutes(1));

        configuration[$"{CohortOptions.SectionName}:DryRun"] = "true";
        configuration[$"{CohortOptions.SectionName}:SweepBatchSize"] = "250";
        configuration[$"{CohortOptions.SectionName}:RowHandlerDispatch:PollInterval"] = "00:00:03";
        configuration[$"{CohortOptions.SectionName}:RowHandlerDispatch:PayloadRetention"] = "4.00:00:00";
        configuration[$"{CohortOptions.SectionName}:RowHandlerDispatch:BatchSize"] = "40";
        configuration[$"{CohortOptions.SectionName}:RowHandlerDispatch:MaxParallelism"] = "4";
        configuration[$"{CohortOptions.SectionName}:RowHandlerDispatch:MaxAttempts"] = "6";
        configuration[$"{CohortOptions.SectionName}:RowHandlerDispatch:BaseBackoff"] = "00:00:05";
        configuration[$"{CohortOptions.SectionName}:RowHandlerDispatch:ClaimTimeout"] = "00:02:00";
        configuration.Reload();

        settings.DryRun.Should().BeTrue();
        settings.SweepBatchSize.Should().Be(250);
        settings.RowHandlerDispatch.PollInterval.Should().Be(TimeSpan.FromSeconds(3));
        settings.RowHandlerDispatch.PayloadRetention.Should().Be(TimeSpan.FromDays(4));
        settings.RowHandlerDispatch.BatchSize.Should().Be(40);
        settings.RowHandlerDispatch.MaxParallelism.Should().Be(4);
        settings.RowHandlerDispatch.MaxAttempts.Should().Be(6);
        settings.RowHandlerDispatch.BaseBackoff.Should().Be(TimeSpan.FromSeconds(5));
        settings.RowHandlerDispatch.ClaimTimeout.Should().Be(TimeSpan.FromMinutes(2));
    }

    [Fact]
    public void AddCohort_Rejects_A_Different_Context()
    {
        var services = new ServiceCollection();
        services.AddCohort<ValidRetentionDbContext>();

        var act = () => services.AddCohort<SecondRetentionDbContext>();

        act.Should().Throw<InvalidOperationException>().WithMessage("*different DbContext*");
    }

    [Fact]
    public async Task Scheduled_Tenantless_Only_Model_Does_Not_Require_TenantContext()
    {
        await using var database = await TemporaryDatabase.CreateAsync(fixture.ConnectionString);
        var recordId = Guid.NewGuid();
        using var host = BuildHost<TenantlessRetentionDbContext>(
            options => options.UseNpgsql(database.ConnectionString),
            new SingleCategoryRepository(
                "tenantless",
                new RetentionRule(TimeSpan.FromDays(30), Strategy.Purge)
            ),
            new Dictionary<string, string?>
            {
                [$"{CohortOptions.SectionName}:Schedule"] = "*/1 * * * * *",
            }
        );

        await using (var scope = host.Services.CreateAsyncScope())
        {
            var db = scope.ServiceProvider.GetRequiredService<TenantlessRetentionDbContext>();
            await db.Database.EnsureCreatedAsync();
            db.Records.Add(
                new TenantlessRetentionRecord
                {
                    Id = recordId,
                    CreatedAt = new DateTimeOffset(2000, 1, 1, 0, 0, 0, TimeSpan.Zero),
                }
            );
            await db.SaveChangesAsync();
        }

        await host.StartAsync();
        try
        {
            await WaitForSuccessfulSweepAsync(
                database.ConnectionString,
                TimeSpan.FromSeconds(8)
            );

            await using var scope = host.Services.CreateAsyncScope();
            var db = scope.ServiceProvider.GetRequiredService<TenantlessRetentionDbContext>();
            (await db.Records.AnyAsync()).Should().BeFalse();
        }
        finally
        {
            await host.StopAsync();
        }
    }

    private static async Task WaitForSuccessfulSweepAsync(
        string connectionString,
        TimeSpan timeout
    )
    {
        var deadline = DateTimeOffset.UtcNow.Add(timeout);

        while (DateTimeOffset.UtcNow < deadline)
        {
            await using var connection = new NpgsqlConnection(connectionString);
            await connection.OpenAsync();
            await using var command = connection.CreateCommand();
            command.CommandText = """
                SELECT EXISTS (
                    SELECT 1
                    FROM "sweep_run" AS run
                    INNER JOIN "sweep_run_entity_summary" AS summary
                        ON summary."SweepId" = run."SweepId"
                    WHERE run."Status" = 1
                      AND run."SettledAt" IS NOT NULL
                      AND run."TriggerKind" = @triggerKind
                      AND summary."RetentionEntityId" = @retentionEntityId
                      AND summary."Category" = @category
                      AND summary."Affected" = 1
                )
                """;
            command.Parameters.AddWithValue("triggerKind", (int)SweepTriggerKind.Scheduled);
            command.Parameters.AddWithValue(
                "retentionEntityId",
                RetentionEntityIdentity.For<TenantlessRetentionRecord>()
            );
            command.Parameters.AddWithValue("category", "tenantless");

            if ((bool)(await command.ExecuteScalarAsync())!)
            {
                return;
            }

            await Task.Delay(100);
        }

        throw new Xunit.Sdk.XunitException(
            "A successful persisted sweep audit was not observed within the allotted timeout."
        );
    }

    private static IHost BuildHost<TContext>(
        Action<DbContextOptionsBuilder> configureDb,
        IRetentionCategoryRepository categoryRepository,
        IReadOnlyDictionary<string, string?>? settings = null,
        Action<IServiceCollection>? configureServices = null
    )
        where TContext : DbContext
    {
        var builder = Host.CreateApplicationBuilder();
        builder.Configuration.AddInMemoryCollection(settings ?? new Dictionary<string, string?>());
        builder.Services.AddDbContext<TContext>(configureDb);
        builder.Services.AddSingleton(categoryRepository);
        configureServices?.Invoke(builder.Services);
        builder.Services.AddCohort<TContext>();
        return builder.Build();
    }

    private static async Task BootstrapCurrentSchemaAsync(string connectionString)
    {
        var options = new DbContextOptionsBuilder<SampleDbContext>()
            .UseNpgsql(connectionString)
            .Options;
        await using var db = new SampleDbContext(options);
        await db.Database.MigrateAsync();
    }

    private static async Task ExecuteSqlAsync(string connectionString, string sql)
    {
        await using var connection = new NpgsqlConnection(connectionString);
        await connection.OpenAsync();
        await using var command = connection.CreateCommand();
        command.CommandText = sql;
        await command.ExecuteNonQueryAsync();
    }

    private sealed class SingleCategoryRepository(string category, RetentionRule rule)
        : IRetentionCategoryRepository
    {
        public Task<IRetentionRuleResolver?> GetAsync(
            string requestedCategory,
            CancellationToken ct
        ) =>
            Task.FromResult<IRetentionRuleResolver?>(
                requestedCategory == category ? new StaticRetentionRuleResolver(rule) : null
            );
    }

    private sealed class InvalidRetentionDbContext(
        DbContextOptions<InvalidRetentionDbContext> options
    ) : DbContext(options)
    {
        protected override void OnModelCreating(ModelBuilder modelBuilder)
        {
            modelBuilder.Entity<InvalidRetentionRecord>().HasKey(record => record.Id);
        }
    }

    private sealed class ValidRetentionDbContext(DbContextOptions<ValidRetentionDbContext> options)
        : DbContext(options)
    {
        protected override void OnModelCreating(ModelBuilder modelBuilder)
        {
            modelBuilder.Entity<ValidRetentionRecord>().HasKey(record => record.Id);
        }
    }

    private sealed class SecondRetentionDbContext(
        DbContextOptions<SecondRetentionDbContext> options
    ) : DbContext(options);

    private sealed class TenantlessRetentionDbContext(
        DbContextOptions<TenantlessRetentionDbContext> options
    ) : DbContext(options)
    {
        public DbSet<TenantlessRetentionRecord> Records => Set<TenantlessRetentionRecord>();

        protected override void OnModelCreating(ModelBuilder modelBuilder)
        {
            modelBuilder.Entity<TenantlessRetentionRecord>(entity =>
            {
                entity.ToTable("host_composition_tenantless_records");
                entity.HasKey(record => record.Id);
            });
            modelBuilder.ConfigureCohortTables();
        }
    }

    [Retain("invalid", nameof(InvalidRetentionRecord.CreatedAt))]
    [RetentionEntityId("00000000-0000-0000-0001-000000000009")]
    private sealed class InvalidRetentionRecord
    {
        public Guid Id { get; init; }
        public DateTimeOffset CreatedAt { get; init; }
    }

    [Retain("valid", nameof(ValidRetentionRecord.CreatedAt))]
    [RetentionEntityId("00000000-0000-0000-0001-00000000000a")]
    private sealed class ValidRetentionRecord
    {
        public Guid Id { get; init; }
        public Guid TenantId { get; init; }
        public DateTimeOffset CreatedAt { get; init; }
    }

    [RetentionTenantless]
    [Retain("tenantless", nameof(TenantlessRetentionRecord.CreatedAt))]
    [RetentionEntityId("00000000-0000-0000-0001-00000000000b")]
    private sealed class TenantlessRetentionRecord
    {
        public Guid Id { get; init; }
        public DateTimeOffset CreatedAt { get; init; }
    }
}
