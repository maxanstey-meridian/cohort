using Cohort.Application;
using Cohort.Domain;

using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;

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
public sealed class CohortTestHost(
    string connectionString,
    IRetentionCategoryRepository? categoryRepository = null,
    IReadOnlyDictionary<string, string?>? configurationOverrides = null,
    Action<IServiceCollection>? configureServices = null
) : IDisposable
{
    private readonly DbContextOptions<SampleDbContext> _options = new DbContextOptionsBuilder<SampleDbContext>()
        .UseNpgsql(connectionString)
        .Options;
    private readonly ServiceProvider _services = BuildServices(
        connectionString,
        categoryRepository,
        configurationOverrides,
        configureServices
    );

    public SampleDbContext CreateDbContext() => new(_options);

    public async Task<IReadOnlyDictionary<Type, Domain.RetentionEntry>> RunStartupAsync(
        CancellationToken ct = default
    )
    {
        await using var scope = _services.CreateAsyncScope();
        var startup = scope.ServiceProvider.GetRequiredService<SampleRetentionStartupService>();
        return await startup.RunAsync(ct);
    }

    public async Task<RetentionSweepResult> RunSweepAsync(
        TenantContext tenant,
        DateTimeOffset now,
        CancellationToken ct = default
    )
    {
        await using var scope = _services.CreateAsyncScope();
        var startup = scope.ServiceProvider.GetRequiredService<SampleRetentionStartupService>();
        return await startup.RunSweepAsync(tenant, now, ct);
    }

    public async Task<RetentionSweepResult> RunPreviewAsync(
        TenantContext tenant,
        DateTimeOffset now,
        CancellationToken ct = default
    )
    {
        await using var scope = _services.CreateAsyncScope();
        var startup = scope.ServiceProvider.GetRequiredService<SampleRetentionStartupService>();
        return await startup.RunPreviewAsync(tenant, now, ct);
    }

    public async Task<ErasureResult> RunErasureAsync(
        TenantContext tenant,
        ErasureScope scope,
        DateTimeOffset now,
        CancellationToken ct = default
    )
    {
        await using var scopeServices = _services.CreateAsyncScope();
        var startup = scopeServices.ServiceProvider.GetRequiredService<SampleRetentionStartupService>();
        return await startup.RunErasureAsync(tenant, scope, now, ct);
    }

    public async Task<TResult> RunWithServicesAsync<TResult>(
        Func<IServiceProvider, Task<TResult>> action
    )
    {
        await using var scope = _services.CreateAsyncScope();
        return await action(scope.ServiceProvider);
    }

    public async Task RunWithServicesAsync(Func<IServiceProvider, Task> action)
    {
        await using var scope = _services.CreateAsyncScope();
        await action(scope.ServiceProvider);
    }

    public void Dispose()
    {
        _services.Dispose();
    }

    private static ServiceProvider BuildServices(
        string connectionString,
        IRetentionCategoryRepository? categoryRepository,
        IReadOnlyDictionary<string, string?>? configurationOverrides,
        Action<IServiceCollection>? configureServices
    )
    {
        var services = new ServiceCollection();
        var effectiveConfiguration = new Dictionary<string, string?>
        {
            [$"{SampleOptions.SectionName}:{nameof(SampleOptions.ConnectionString)}"] =
                connectionString,
        };

        if (configurationOverrides is not null)
        {
            foreach (var pair in configurationOverrides)
            {
                effectiveConfiguration[pair.Key] = pair.Value;
            }
        }

        var configuration = new ConfigurationBuilder()
            .AddInMemoryCollection(effectiveConfiguration)
            .Build();

        services.AddSingleton<IConfiguration>(configuration);
        services.AddLogging();
        services.AddSampleRetentionServices();
        services.RemoveAll<IRetentionCategoryRepository>();
        services.AddSingleton<IRetentionCategoryRepository>(
            categoryRepository ?? new SampleCategoryRepository()
        );
        configureServices?.Invoke(services);

        return services.BuildServiceProvider(validateScopes: true);
    }
}
