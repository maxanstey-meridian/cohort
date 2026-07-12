using Cohort.Application;
using Cohort.Domain;
using Cohort.Infrastructure;
using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using Npgsql;

namespace Cohort.Sample.Tests;

/// <summary>
/// One-line spin-up of the Cohort sample stack against a real Postgres instance.
/// Mirrors the role of <c>CompilationHelper</c> in rivet — every end-to-end test
/// calls <see cref="CreateDbContext"/> and gets a fresh <see cref="SampleDbContext"/>
/// pointed at the fixture connection string.
/// </summary>
public sealed class CohortTestHost(
    string connectionString,
    IRetentionRuleProvider? ruleProvider = null,
    IReadOnlyDictionary<string, string?>? configurationOverrides = null,
    Action<IServiceCollection>? configureServices = null,
    PostgreSqlCommandRecorder? commandRecorder = null
) : IDisposable
{
    private readonly NpgsqlDataSource? _dataSource = commandRecorder?.CreateDataSource(connectionString);
    private readonly DbContextOptions<SampleDbContext> _options =
        commandRecorder is null
            ? new DbContextOptionsBuilder<SampleDbContext>().UseNpgsql(connectionString).Options
            : new DbContextOptionsBuilder<SampleDbContext>()
                .UseNpgsql(commandRecorder.DataSource!)
                .Options;
    private readonly ServiceProvider _services = BuildServices(
        connectionString,
        ruleProvider,
        configurationOverrides,
        configureServices,
        commandRecorder
    );

    public SampleDbContext CreateDbContext() => new(_options);

    internal async Task<IReadOnlyDictionary<Type, RetentionEntry>> ValidateAndScanAsync(
        CancellationToken ct = default
    )
    {
        await using var scope = _services.CreateAsyncScope();
        await scope.ServiceProvider.GetRequiredService<RetentionStartupValidator>().ValidateAsync(ct);
        return scope.ServiceProvider.GetRequiredService<RetentionRegistry>().Scan();
    }

    public async Task<RetentionSweepResult> RunSweepAsync(
        TenantContext tenant,
        DateTimeOffset now,
        CancellationToken ct = default
    )
    {
        await using var scope = _services.CreateAsyncScope();
        var sweep = scope.ServiceProvider.GetRequiredService<IRetentionSweep>();
        return await sweep.SweepAsync(tenant, now, ct);
    }

    public async Task<RetentionSweepResult> RunPreviewAsync(
        TenantContext tenant,
        DateTimeOffset now,
        CancellationToken ct = default
    )
    {
        await using var scope = _services.CreateAsyncScope();
        var preview = scope.ServiceProvider.GetRequiredService<IRetentionPreview>();
        return await preview.PreviewAsync(tenant, now, ct);
    }

    public async Task<ErasureResult> RunErasureAsync(
        TenantContext tenant,
        ErasureScope scope,
        DateTimeOffset now,
        CancellationToken ct = default
    )
    {
        await using var scopeServices = _services.CreateAsyncScope();
        var erasure = scopeServices.ServiceProvider.GetRequiredService<IRetentionErasureService>();
        return await erasure.EraseAsync(tenant, scope, now, ct);
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
        _dataSource?.Dispose();
    }

    private static ServiceProvider BuildServices(
        string connectionString,
        IRetentionRuleProvider? ruleProvider,
        IReadOnlyDictionary<string, string?>? configurationOverrides,
        Action<IServiceCollection>? configureServices,
        PostgreSqlCommandRecorder? commandRecorder
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
        if (commandRecorder is not null)
        {
            services.AddDbContext<SampleDbContext>(options =>
                options.UseNpgsql(commandRecorder.DataSource!)
            );
        }
        services.RemoveAll<IRetentionRuleProvider>();
        services.AddSingleton<IRetentionRuleProvider>(
            ruleProvider ?? new SampleRetentionRuleProvider()
        );
        configureServices?.Invoke(services);

        return services.BuildServiceProvider(validateScopes: true);
    }
}
