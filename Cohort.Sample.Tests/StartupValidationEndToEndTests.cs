using Cohort.Application;
using Cohort.Domain;
using Cohort.Hosting;
using Cohort.Infrastructure;
using Cohort.Infrastructure.Migrations;
using Cohort.Sample.Entities;

using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Options;

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

public sealed class StartupValidationEndToEndTests : IntegrationTestBase
{
    private readonly string connectionString;

    public StartupValidationEndToEndTests(PostgresFixture fixture)
        : base(fixture)
    {
        connectionString = fixture.ConnectionString;
    }

    [Fact]
    public async Task Validation_Returns_Registry_Entries_When_Configuration_Is_Valid()
    {
        var entries = await Host.ValidateAndScanAsync();

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
        entries
            .Should()
            .Contain(kvp =>
                kvp.Key == typeof(AnonymisedContact)
                && kvp.Value.Category == "anonymise"
                && kvp.Value.AnchorMember == nameof(AnonymisedContact.CreatedAt)
            );
        entries
            .Should()
            .Contain(kvp =>
                kvp.Key == typeof(BlobBackedFile)
                && kvp.Value.Category == "blob-cleanup"
                && kvp.Value.AnchorMember == nameof(BlobBackedFile.CreatedAt)
            );
        entries
            .Should()
            .Contain(kvp =>
                kvp.Key == typeof(TenantlessLog)
                && kvp.Value.Category == "tenantless-purge"
                && kvp.Value.AnchorMember == nameof(TenantlessLog.CreatedAt)
                && kvp.Value.Tenant == null
                && kvp.Value.IsExplicitlyTenantless
            );
        entries
            .Should()
            .Contain(kvp =>
                kvp.Key == typeof(TenantlessSoftDelete)
                && kvp.Value.Category == "tenantless-softdelete"
                && kvp.Value.AnchorMember == nameof(TenantlessSoftDelete.CreatedAt)
                && kvp.Value.Tenant == null
                && kvp.Value.IsExplicitlyTenantless
            );
        entries
            .Should()
            .Contain(kvp =>
                kvp.Key == typeof(TombstoneRecord)
                && kvp.Value.Category == "tombstone-anonymise"
                && kvp.Value.AnchorMember == nameof(TombstoneRecord.CreatedAt)
            );
    }

    [Fact]
    public async Task Validation_Checks_Every_Strategy_Declared_For_A_Dynamic_Category()
    {
        var builder = Microsoft.Extensions.Hosting.Host.CreateApplicationBuilder();
        builder.Configuration.AddInMemoryCollection(
            new Dictionary<string, string?>
            {
                [$"{SampleOptions.SectionName}:{nameof(SampleOptions.ConnectionString)}"] =
                    connectionString,
            }
        );
        builder.Services.AddSampleRetentionServices();
        builder.Services.RemoveAll<IRetentionRuleProvider>();
        builder.Services.AddSingleton<IRetentionRuleProvider, MultiStrategyRuleProvider>();
        using var host = builder.Build();

        var act = () => host.StartAsync();

        var exception = await act.Should().ThrowAsync<RetentionConfigurationException>();
        exception
            .Which.Errors.Should()
            .Contain(error =>
                error.Contains(typeof(SoftDeleteRecord).FullName!, StringComparison.Ordinal)
                && error.Contains("Anonymise", StringComparison.Ordinal)
            );
    }

    [Fact]
    public async Task Validation_Fails_When_Retained_Entity_Cannot_Resolve_Tenant_In_TenantScoped_Config()
    {
        var act = async () =>
            await RunTenantScopeStartupAsync<MisconfiguredTenantScopedDbContext>(
                new SingleCategoryRepository(
                    "misconfigured-tenant-scope",
                    new StaticTestRetentionRule(
                        new RetentionRule(TimeSpan.FromDays(30), Strategy.Purge)
                    )
                )
            );

        var exception = await act.Should().ThrowAsync<RetentionConfigurationException>();
        exception.Which.Errors.Should().ContainSingle();
        exception
            .Which.Errors[0]
            .Should()
            .Be(
                $"Tenant convention on {typeof(MisconfiguredTenantScopedRecord).FullName}: retained entities must expose a public non-nullable Guid tenant property named 'TenantId' by convention, or mark the tenant property with [RetentionTenant], unless the entity is explicitly marked with [RetentionTenantless]."
            );
    }

    [Fact]
    public async Task Sweep_Path_Does_Not_Start_When_TenantScoped_Retained_Entity_Is_Misconfigured()
    {
        var asOf = new DateTimeOffset(2026, 4, 13, 12, 0, 0, TimeSpan.Zero);
        var recordId = Guid.NewGuid();

        await RunTenantScopeHostAsync<MisconfiguredTenantScopedDbContext>(
            new SingleCategoryRepository(
                "misconfigured-tenant-scope",
                new StaticTestRetentionRule(
                    new RetentionRule(TimeSpan.FromDays(30), Strategy.Purge)
                )
            ),
            async serviceProvider =>
            {
                await using (var seedScope = serviceProvider.CreateAsyncScope())
                {
                    var db =
                        seedScope.ServiceProvider.GetRequiredService<MisconfiguredTenantScopedDbContext>();
                    await db.Database.ExecuteSqlRawAsync(
                        """
                        CREATE TABLE IF NOT EXISTS "misconfigured_tenant_scoped_records" (
                            "Id" uuid PRIMARY KEY,
                            "created_at_utc" timestamp with time zone NOT NULL,
                            "payload" text NOT NULL
                        )
                        """
                    );
                    db.MisconfiguredTenantScopedRecords.Add(
                        new MisconfiguredTenantScopedRecord
                        {
                            Id = recordId,
                            CreatedAt = asOf.AddDays(-120),
                            Payload = "still-here-because-validation-failed",
                        }
                    );
                    await db.SaveChangesAsync();
                }

                var act = async () =>
                {
                    await using var startupScope = serviceProvider.CreateAsyncScope();
                    var sweep = startupScope.ServiceProvider.GetRequiredService<IRetentionSweep>();
                    await sweep.SweepAsync(
                        new TenantContext(Guid.NewGuid(), "uk", new Dictionary<string, string>()),
                        asOf
                    );
                };

                var exception = await act.Should().ThrowAsync<RetentionConfigurationException>();
                exception.Which.Errors.Should().ContainSingle();
                exception
                    .Which.Errors[0]
                    .Should()
                    .Be(
                        $"Tenant convention on {typeof(MisconfiguredTenantScopedRecord).FullName}: retained entities must expose a public non-nullable Guid tenant property named 'TenantId' by convention, or mark the tenant property with [RetentionTenant], unless the entity is explicitly marked with [RetentionTenantless]."
                    );

                await using var verifyScope = serviceProvider.CreateAsyncScope();
                var verifyDb =
                    verifyScope.ServiceProvider.GetRequiredService<MisconfiguredTenantScopedDbContext>();
                var remaining = await verifyDb.MisconfiguredTenantScopedRecords.SingleAsync(
                    record => record.Id == recordId
                );

                remaining.Payload.Should().Be("still-here-because-validation-failed");
            }
        );
    }

    [Fact]
    public async Task Validation_Fails_When_Category_Resolution_Is_Misconfigured()
    {
        await using var db = Host.CreateDbContext();
        var connectionString = db.Database.GetConnectionString()!;
        using var host = new CohortTestHost(connectionString, new EmptyCategoryRepository());

        var act = async () => await host.ValidateAndScanAsync();

        var exception = await act.Should().ThrowAsync<RetentionConfigurationException>();
        exception
            .Which.Errors.Should()
            .BeEquivalentTo(
                [
                    $"Retention category 'short-lived' for entity {typeof(Note).FullName} could not be resolved.",
                    $"Retention category 'blob-cleanup' for entity {typeof(BlobBackedFile).FullName} could not be resolved.",
                    $"Retention category 'soft-delete' for entity {typeof(SoftDeleteRecord).FullName} could not be resolved.",
                    $"Retention category 'anonymise' for entity {typeof(AnonymisedContact).FullName} could not be resolved.",
                    $"Retention category 'tenantless-purge' for entity {typeof(TenantlessLog).FullName} could not be resolved.",
                    $"Retention category 'tenantless-purge' for entity {typeof(ExternalNumberedLog).FullName} could not be resolved.",
                    $"Retention category 'tenantless-softdelete' for entity {typeof(TenantlessSoftDelete).FullName} could not be resolved.",
                    $"Retention category 'per-row-audit-override' for entity {typeof(PerRowAuditedLog).FullName} could not be resolved.",
                    $"Retention category 'tombstone-anonymise' for entity {typeof(TombstoneRecord).FullName} could not be resolved.",
                    $"Retention category 'nullable-anchor-purge' for entity {typeof(NullableAnchorEvent).FullName} could not be resolved.",
                ]
            );
    }

    [Fact]
    public async Task Shared_Test_Host_Uses_The_Cohort_Di_Entry_Point()
    {
        await Host.RunWithServicesAsync(serviceProvider =>
        {
            serviceProvider
                .GetServices<IHostedService>()
                .Should()
                .ContainSingle(service => service.GetType() == typeof(RetentionWorker));

            return Task.CompletedTask;
        });
    }

    [Fact]
    public async Task Shared_Test_Host_Registers_Row_Handler_Dispatcher_As_Both_Port_And_Hosted_Service_Without_Row_Handlers()
    {
        await Host.RunWithServicesAsync(serviceProvider =>
        {
            var dispatcher = serviceProvider.GetRequiredService<IRetentionRowDispatcher>();
            var hostedDispatcher = serviceProvider
                .GetServices<IHostedService>()
                .Single(service => service is IRetentionRowDispatcher);
            var noteHandlers = serviceProvider.GetServices<IRetentionHandler<Note>>();

            hostedDispatcher.Should().BeSameAs(dispatcher);
            noteHandlers.Should().BeEmpty();

            return Task.CompletedTask;
        });
    }

    [Fact]
    public async Task Shared_Test_Host_Binds_Row_Handler_Dispatch_Options_From_Cohort_Configuration()
    {
        using var host = new CohortTestHost(
            connectionString,
            configurationOverrides: new Dictionary<string, string?>
            {
                [$"{CohortOptions.SectionName}:RowHandlerDispatch:PollInterval"] = "00:00:03",
                [$"{CohortOptions.SectionName}:RowHandlerDispatch:BatchSize"] = "25",
                [$"{CohortOptions.SectionName}:RowHandlerDispatch:MaxAttempts"] = "7",
                [$"{CohortOptions.SectionName}:RowHandlerDispatch:MaxParallelism"] = "6",
                [$"{CohortOptions.SectionName}:RowHandlerDispatch:BaseBackoff"] = "00:00:02",
            }
        );

        await host.RunWithServicesAsync(serviceProvider =>
        {
            var options = serviceProvider.GetRequiredService<IOptions<CohortOptions>>().Value;

            options.RowHandlerDispatch.PollInterval.Should().Be(TimeSpan.FromSeconds(3));
            options.RowHandlerDispatch.BatchSize.Should().Be(25);
            options.RowHandlerDispatch.MaxAttempts.Should().Be(7);
            options.RowHandlerDispatch.MaxParallelism.Should().Be(6);
            options.RowHandlerDispatch.BaseBackoff.Should().Be(TimeSpan.FromSeconds(2));

            return Task.CompletedTask;
        });
    }

    [Fact]
    public async Task Hosted_Start_Rejects_Bound_Audit_Observer_Timeout_Above_The_Ceiling()
    {
        using var host = BuildSampleHostWithSetting("AuditObservers:Timeout", "01:00:00.001");

        var start = () => host.StartAsync();

        await start.Should()
            .ThrowAsync<OptionsValidationException>()
            .WithMessage("*AuditObservers Timeout must not exceed 1 hour*");
    }

    [Theory]
    [InlineData("RowHandlerDispatch:BatchSize", "10001", "BatchSize")]
    [InlineData("RowHandlerDispatch:MaxAttempts", "1001", "MaxAttempts")]
    [InlineData("RowHandlerDispatch:MaxParallelism", "257", "MaxParallelism")]
    public async Task Hosted_Start_Rejects_Bound_Row_Handler_Dispatch_Values_Above_Their_Ceilings(
        string key,
        string value,
        string optionName
    )
    {
        using var host = BuildSampleHostWithSetting(key, value);

        var start = () => host.StartAsync();

        await start.Should()
            .ThrowAsync<OptionsValidationException>()
            .WithMessage($"*RowHandlerDispatch {optionName} must not exceed*");
    }

    [Fact]
    public async Task AddRowHandler_Registers_Typed_Handlers_Without_Duplicates()
    {
        using var host = new CohortTestHost(
            connectionString,
            configureServices: services =>
            {
                services.AddRowHandler<Note, FirstNoteHandler>();
                services.AddRowHandler<Note, FirstNoteHandler>();
                services.AddRowHandler<Note, SecondNoteHandler>();
            }
        );

        await host.RunWithServicesAsync(serviceProvider =>
        {
            var handlers = serviceProvider.GetServices<IRetentionHandler<Note>>().ToArray();

            handlers.Should().HaveCount(2);
            handlers[0].Should().BeOfType<FirstNoteHandler>();
            handlers[1].Should().BeOfType<SecondNoteHandler>();

            return Task.CompletedTask;
        });
    }

    [Fact]
    public async Task AddRowHandler_Host_Composed_Dispatcher_Allows_Flush_Through_The_Port()
    {
        using var host = new CohortTestHost(
            connectionString,
            configureServices: services =>
            {
                services.AddRowHandler<Note, FirstNoteHandler>();
            }
        );

        await host.RunWithServicesAsync(async serviceProvider =>
        {
            var dispatcher = serviceProvider.GetRequiredService<IRetentionRowDispatcher>();

            await dispatcher.FlushAsync();
        });
    }

    [Fact]
    public async Task Sample_Host_Composition_Registers_Tombstone_Factories_For_Startup_Validation()
    {
        var builder = Microsoft.Extensions.Hosting.Host.CreateApplicationBuilder();
        builder.Configuration.AddInMemoryCollection(
            new Dictionary<string, string?>
            {
                [$"{SampleOptions.SectionName}:{nameof(SampleOptions.ConnectionString)}"] =
                    connectionString,
            }
        );

        builder.Services.AddSampleRetentionServices();

        using var host = builder.Build();
        await host.StartAsync();

        try
        {
            await using var scope = host.Services.CreateAsyncScope();
            var entries = await ValidateAndScanAsync(scope.ServiceProvider);
            var factoryTypes = entries[typeof(TombstoneRecord)]
                .AnonymiseFields.OfType<AnonymiseFactoryField>()
                .Select(field => field.FactoryType)
                .ToArray();

            entries.Should().ContainKey(typeof(TombstoneRecord));
            factoryTypes.Should().Contain(typeof(GuidTombstoneFactory));
            factoryTypes.Should().Contain(typeof(OriginalValueTombstoneFactory));
        }
        finally
        {
            await host.StopAsync();
        }
    }

    [Fact]
    public async Task Validation_Fails_When_AnonymiseWith_Uses_A_Type_That_Does_Not_Implement_The_Factory_Port()
    {
        var act = async () =>
            await RunFactoryValidationStartupAsync<InvalidFactoryTypeStartupDbContext>(
                new SingleCategoryRepository(
                    "invalid-factory-type",
                    new StaticTestRetentionRule(
                        new RetentionRule(TimeSpan.FromDays(30), Strategy.Anonymise)
                    )
                )
            );

        var exception = await act.Should().ThrowAsync<RetentionConfigurationException>();
        exception.Which.Errors.Should().ContainSingle();
        exception
            .Which.Errors[0]
            .Should()
            .Be(
                $"Anonymise convention on {typeof(InvalidFactoryTypeStartupRecord).FullName}: [AnonymiseWith] member ExternalId specifies factory type {typeof(NotAFactory).FullName} which does not implement {nameof(IAnonymiseValueFactory)}."
            );
    }

    [Fact]
    public async Task Validation_Fails_When_AnonymiseWith_Factory_Is_Not_Registered_In_Di()
    {
        var act = async () =>
            await RunFactoryValidationStartupAsync<UnregisteredFactoryStartupDbContext>(
                new SingleCategoryRepository(
                    "unregistered-factory",
                    new StaticTestRetentionRule(
                        new RetentionRule(TimeSpan.FromDays(30), Strategy.Anonymise)
                    )
                )
            );

        var exception = await act.Should().ThrowAsync<RetentionConfigurationException>();
        exception.Which.Errors.Should().ContainSingle();
        exception
            .Which.Errors[0]
            .Should()
            .Be(
                $"Anonymise convention on {typeof(UnregisteredFactoryStartupRecord).FullName}: [AnonymiseWith] member ExternalId specifies factory type {typeof(RegisteredFactory).FullName} but no matching {nameof(IAnonymiseValueFactory)} is registered in DI."
            );
    }

    [Fact]
    public async Task Validation_Allows_Registered_FactoryBacked_Anonymise_Metadata()
    {
        var entries = await RunFactoryValidationStartupAsync<RegisteredFactoryStartupDbContext>(
            new SingleCategoryRepository(
                "registered-factory",
                new StaticTestRetentionRule(
                    new RetentionRule(TimeSpan.FromDays(30), Strategy.Anonymise)
                )
            ),
            services => services.AddSingleton<IAnonymiseValueFactory, RegisteredFactory>()
        );

        entries.Should().ContainKey(typeof(RegisteredFactoryStartupRecord));
        entries[typeof(RegisteredFactoryStartupRecord)]
            .AnonymiseFields.Should()
            .ContainSingle(field => field is AnonymiseFactoryField);
    }

    [Fact]
    public async Task Validation_Fails_When_The_Same_Anonymise_Factory_Type_Is_Registered_Twice()
    {
        var factory = new RegisteredFactory();
        var act = async () =>
            await RunFactoryValidationStartupAsync<RegisteredFactoryStartupDbContext>(
                new SingleCategoryRepository(
                    "registered-factory",
                    new StaticTestRetentionRule(
                        new RetentionRule(TimeSpan.FromDays(30), Strategy.Anonymise)
                    )
                ),
                services =>
                {
                    services.AddSingleton<IAnonymiseValueFactory>(factory);
                    services.AddSingleton<IAnonymiseValueFactory>(factory);
                }
            );

        var exception = await act.Should().ThrowAsync<RetentionConfigurationException>();
        exception.Which.Errors.Should().ContainSingle();
        exception
            .Which.Errors[0]
            .Should()
            .Be(
                $"{nameof(IAnonymiseValueFactory)} concrete runtime type {typeof(RegisteredFactory).FullName} is registered 2 times in DI; exactly one registration per concrete runtime type is allowed."
            );
    }

    [Fact]
    public async Task Validation_Fails_When_Differently_Configured_Instances_Of_The_Same_Anonymise_Factory_Type_Are_Registered()
    {
        var act = async () =>
            await RunFactoryValidationStartupAsync<RegisteredFactoryStartupDbContext>(
                new SingleCategoryRepository(
                    "registered-factory",
                    new StaticTestRetentionRule(
                        new RetentionRule(TimeSpan.FromDays(30), Strategy.Anonymise)
                    )
                ),
                services =>
                {
                    services.AddSingleton<IAnonymiseValueFactory>(
                        new RegisteredFactory { Value = Guid.Empty }
                    );
                    services.AddSingleton<IAnonymiseValueFactory>(
                        new RegisteredFactory { Value = Guid.NewGuid() }
                    );
                }
            );

        var exception = await act.Should().ThrowAsync<RetentionConfigurationException>();
        exception.Which.Errors.Should().ContainSingle();
        exception
            .Which.Errors[0]
            .Should()
            .Be(
                $"{nameof(IAnonymiseValueFactory)} concrete runtime type {typeof(RegisteredFactory).FullName} is registered 2 times in DI; exactly one registration per concrete runtime type is allowed."
            );
    }

    [Fact]
    public async Task Validation_Allows_Registered_FactoryBacked_Metadata_Even_When_Category_Is_Not_Anonymise()
    {
        var entries = await RunFactoryValidationStartupAsync<RegisteredFactoryStartupDbContext>(
            new SingleCategoryRepository(
                "registered-factory",
                new StaticTestRetentionRule(
                    new RetentionRule(TimeSpan.FromDays(30), Strategy.Purge)
                )
            ),
            services => services.AddSingleton<IAnonymiseValueFactory, RegisteredFactory>()
        );

        entries.Should().ContainKey(typeof(RegisteredFactoryStartupRecord));
        entries[typeof(RegisteredFactoryStartupRecord)]
            .AnonymiseFields.Should()
            .ContainSingle(field => field is AnonymiseFactoryField);
    }

    [Fact]
    public async Task Validation_Allows_Registered_FactoryBacked_Metadata_When_Strategy_Is_Deferred()
    {
        var entries = await RunFactoryValidationStartupAsync<RegisteredFactoryStartupDbContext>(
            new SingleCategoryRepository(
                "registered-factory",
                new DeferredRuleResolver(
                    new RetentionRule(TimeSpan.FromDays(30), Strategy.Anonymise)
                )
            ),
            services => services.AddSingleton<IAnonymiseValueFactory, RegisteredFactory>()
        );

        entries.Should().ContainKey(typeof(RegisteredFactoryStartupRecord));
        entries[typeof(RegisteredFactoryStartupRecord)]
            .AnonymiseFields.Should()
            .ContainSingle(field => field is AnonymiseFactoryField);
    }

    private async Task<
        IReadOnlyDictionary<Type, RetentionEntry>
    > RunFactoryValidationStartupAsync<TContext>(
        ITestRetentionRuleProvider categoryRepository,
        Action<IServiceCollection>? registerServices = null
    )
        where TContext : DbContext
    {
        var services = new ServiceCollection();
        var configuration = new ConfigurationBuilder()
            .AddInMemoryCollection(new Dictionary<string, string?>())
            .Build();

        services.AddSingleton<IConfiguration>(configuration);
        services.AddLogging();
        services.AddDbContext<TContext>(options => options.UseNpgsql(connectionString));
        services.AddSingleton<IRetentionRuleProvider>(categoryRepository);
        registerServices?.Invoke(services);
        services.AddCohort<TContext>();

        await using var serviceProvider = services.BuildServiceProvider(validateScopes: true);
        await using var scope = serviceProvider.CreateAsyncScope();
        return await ValidateAndScanAsync(scope.ServiceProvider);
    }

    private IHost BuildSampleHostWithSetting(string key, string value)
    {
        var builder = Microsoft.Extensions.Hosting.Host.CreateApplicationBuilder();
        builder.Configuration.AddInMemoryCollection(
            new Dictionary<string, string?>
            {
                [$"{SampleOptions.SectionName}:{nameof(SampleOptions.ConnectionString)}"] =
                    connectionString,
                [$"{CohortOptions.SectionName}:{key}"] = value,
            }
        );
        builder.Services.AddSampleRetentionServices();
        return builder.Build();
    }

    private async Task<
        IReadOnlyDictionary<Type, RetentionEntry>
    > RunTenantScopeStartupAsync<TContext>(ITestRetentionRuleProvider categoryRepository)
        where TContext : DbContext
    {
        IReadOnlyDictionary<Type, RetentionEntry>? entries = null;

        await RunTenantScopeHostAsync<TContext>(
            categoryRepository,
            async serviceProvider =>
            {
                await using var scope = serviceProvider.CreateAsyncScope();
                entries = await ValidateAndScanAsync(scope.ServiceProvider);
            }
        );

        return entries!;
    }

    private static async Task<IReadOnlyDictionary<Type, RetentionEntry>> ValidateAndScanAsync(
        IServiceProvider services,
        CancellationToken ct = default
    )
    {
        await services.GetRequiredService<RetentionStartupValidator>().ValidateAsync(ct);
        return services.GetRequiredService<RetentionRegistry>().Scan();
    }

    private async Task RunTenantScopeHostAsync<TContext>(
        ITestRetentionRuleProvider categoryRepository,
        Func<ServiceProvider, Task> action
    )
        where TContext : DbContext
    {
        var services = new ServiceCollection();
        var configuration = new ConfigurationBuilder()
            .AddInMemoryCollection(new Dictionary<string, string?>())
            .Build();

        services.AddSingleton<IConfiguration>(configuration);
        services.AddLogging();
        services.AddDbContext<TContext>(options => options.UseNpgsql(connectionString));
        services.AddSingleton<IRetentionRuleProvider>(categoryRepository);
        services.AddCohort<TContext>();

        await using var serviceProvider = services.BuildServiceProvider(validateScopes: true);
        await action(serviceProvider);
    }

    private sealed class EmptyCategoryRepository : ITestRetentionRuleProvider
    {
        public Task<ITestRetentionRule?> GetAsync(string category, CancellationToken ct) =>
            Task.FromResult<ITestRetentionRule?>(null);
    }

    private sealed class MultiStrategyRuleProvider : IRetentionRuleProvider
    {
        private readonly SampleRetentionRuleProvider inner = new();

        public RetentionCategoryCapabilities? GetCapabilities(string category) =>
            category == "soft-delete"
                ? new([Strategy.SoftDelete, Strategy.Anonymise])
                : inner.GetCapabilities(category);

        public Task<RetentionRule?> ResolveAsync(
            RetentionResolutionContext context,
            CancellationToken ct
        ) => inner.ResolveAsync(context, ct);
    }

    private sealed class SingleCategoryRepository(string category, ITestRetentionRule resolver)
        : ITestRetentionRuleProvider
    {
        public Task<ITestRetentionRule?> GetAsync(
            string requestedCategory,
            CancellationToken ct
        )
        {
            return Task.FromResult<ITestRetentionRule?>(
                requestedCategory == category
                    ? resolver
                    : throw new InvalidOperationException(
                        $"Unexpected category lookup for '{requestedCategory}'."
                    )
            );
        }
    }

    private sealed class DeferredRuleResolver(RetentionRule rule) : ITestRetentionRule
    {
        public Task<RetentionRule> ResolveAsync(
            RetentionResolutionContext ctx,
            CancellationToken ct
        ) => Task.FromResult(rule);
    }

    private sealed class InvalidFactoryTypeStartupDbContext(
        DbContextOptions<InvalidFactoryTypeStartupDbContext> options
    ) : DbContext(options)
    {
        protected override void OnModelCreating(ModelBuilder modelBuilder)
        {
            modelBuilder.ConfigureCohortTables();
            modelBuilder.Entity<InvalidFactoryTypeStartupRecord>(entity =>
            {
                entity.ToTable("invalid_factory_type_startup_records");
                entity.HasKey(record => record.Id);
                entity.Property(record => record.CreatedAt).HasColumnName("created_at_utc");
                entity.Property(record => record.TenantId).HasColumnName("tenant_id");
                entity.Property(record => record.ExternalId).HasColumnName("external_id");
            });
        }
    }

    private sealed class UnregisteredFactoryStartupDbContext(
        DbContextOptions<UnregisteredFactoryStartupDbContext> options
    ) : DbContext(options)
    {
        protected override void OnModelCreating(ModelBuilder modelBuilder)
        {
            modelBuilder.ConfigureCohortTables();
            modelBuilder.Entity<UnregisteredFactoryStartupRecord>(entity =>
            {
                entity.ToTable("unregistered_factory_startup_records");
                entity.HasKey(record => record.Id);
                entity.Property(record => record.CreatedAt).HasColumnName("created_at_utc");
                entity.Property(record => record.TenantId).HasColumnName("tenant_id");
                entity.Property(record => record.ExternalId).HasColumnName("external_id");
            });
        }
    }

    private sealed class RegisteredFactoryStartupDbContext(
        DbContextOptions<RegisteredFactoryStartupDbContext> options
    ) : DbContext(options)
    {
        protected override void OnModelCreating(ModelBuilder modelBuilder)
        {
            modelBuilder.ConfigureCohortTables();
            modelBuilder.Entity<RegisteredFactoryStartupRecord>(entity =>
            {
                entity.ToTable("registered_factory_startup_records");
                entity.HasKey(record => record.Id);
                entity.Property(record => record.CreatedAt).HasColumnName("created_at_utc");
                entity.Property(record => record.TenantId).HasColumnName("tenant_id");
                entity.Property(record => record.ExternalId).HasColumnName("external_id");
            });
        }
    }

    private sealed class MisconfiguredTenantScopedDbContext(
        DbContextOptions<MisconfiguredTenantScopedDbContext> options
    ) : DbContext(options)
    {
        public DbSet<MisconfiguredTenantScopedRecord> MisconfiguredTenantScopedRecords =>
            Set<MisconfiguredTenantScopedRecord>();

        protected override void OnModelCreating(ModelBuilder modelBuilder)
        {
            modelBuilder.Entity<MisconfiguredTenantScopedRecord>(entity =>
            {
                entity.ToTable("misconfigured_tenant_scoped_records");
                entity.HasKey(record => record.Id);
                entity.Property(record => record.CreatedAt).HasColumnName("created_at_utc");
                entity.Property(record => record.Payload).HasColumnName("payload");
            });

            modelBuilder.ConfigureCohortTables();
        }
    }

    [Retain("invalid-factory-type", nameof(InvalidFactoryTypeStartupRecord.CreatedAt))]
    [RetentionEntityId("00000000-0000-0000-0001-00000000003e")]
    private sealed class InvalidFactoryTypeStartupRecord
    {
        public Guid Id { get; init; }
        public Guid TenantId { get; init; }
        public DateTimeOffset CreatedAt { get; init; }

        [AnonymiseWith(typeof(NotAFactory))]
        public Guid ExternalId { get; init; }

        public DateTimeOffset? AnonymisedAt { get; init; }
    }

    [Retain("unregistered-factory", nameof(UnregisteredFactoryStartupRecord.CreatedAt))]
    [RetentionEntityId("00000000-0000-0000-0001-00000000003f")]
    private sealed class UnregisteredFactoryStartupRecord
    {
        public Guid Id { get; init; }
        public Guid TenantId { get; init; }
        public DateTimeOffset CreatedAt { get; init; }

        [AnonymiseWith(typeof(RegisteredFactory))]
        public Guid ExternalId { get; init; }

        public DateTimeOffset? AnonymisedAt { get; init; }
    }

    [Retain("registered-factory", nameof(RegisteredFactoryStartupRecord.CreatedAt))]
    [RetentionEntityId("00000000-0000-0000-0001-000000000040")]
    private sealed class RegisteredFactoryStartupRecord
    {
        public Guid Id { get; init; }
        public Guid TenantId { get; init; }
        public DateTimeOffset CreatedAt { get; init; }

        [AnonymiseWith(typeof(RegisteredFactory))]
        public Guid ExternalId { get; init; }

        public DateTimeOffset? AnonymisedAt { get; init; }
    }

    [Retain("misconfigured-tenant-scope", nameof(MisconfiguredTenantScopedRecord.CreatedAt))]
    [RetentionEntityId("00000000-0000-0000-0001-000000000041")]
    private sealed class MisconfiguredTenantScopedRecord
    {
        public Guid Id { get; init; }
        public DateTimeOffset CreatedAt { get; init; }
        public string Payload { get; init; } = "";
    }

    private sealed class RegisteredFactory : IAnonymiseValueFactory
    {
        public Guid Value { get; init; } = Guid.Empty;

        public object? Create(AnonymiseValueContext context) => Value;
    }

    private sealed class FirstNoteHandler : IRetentionHandler<Note>;

    private sealed class SecondNoteHandler : IRetentionHandler<Note>;

    private sealed class NotAFactory;
}
