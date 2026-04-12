using Cohort.Application;
using Cohort.Domain;

using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;

namespace Cohort.Hosting;

public sealed class RetentionWorker(
    IServiceScopeFactory scopeFactory,
    IOptionsMonitor<CohortOptions> options,
    ILogger<RetentionWorker> logger
) : BackgroundService
{
    private static readonly TimeSpan IdlePollInterval = TimeSpan.FromMilliseconds(200);

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        while (!stoppingToken.IsCancellationRequested)
        {
            var currentOptions = options.CurrentValue;
            if (currentOptions.KillSwitch || string.IsNullOrWhiteSpace(currentOptions.Schedule))
            {
                await DelayUntilNextPollAsync(stoppingToken);
                continue;
            }

            DateTimeOffset? nextOccurrence;
            try
            {
                nextOccurrence = CohortScheduleParser.GetNextOccurrence(
                    currentOptions.Schedule,
                    DateTimeOffset.UtcNow
                );
            }
            catch (Exception ex)
            {
                logger.LogError(ex, "Cohort worker schedule is invalid at runtime.");
                await DelayUntilNextPollAsync(stoppingToken);
                continue;
            }

            if (nextOccurrence is null)
            {
                await DelayUntilNextPollAsync(stoppingToken);
                continue;
            }

            var delay = nextOccurrence.Value - DateTimeOffset.UtcNow;
            if (delay > TimeSpan.Zero)
            {
                await Task.Delay(delay, stoppingToken);
            }

            if (stoppingToken.IsCancellationRequested)
            {
                break;
            }

            currentOptions = options.CurrentValue;
            if (currentOptions.KillSwitch)
            {
                continue;
            }

            await RunIterationAsync(currentOptions.DryRun, stoppingToken);
        }
    }

    private async Task RunIterationAsync(bool dryRun, CancellationToken ct)
    {
        await using var scope = scopeFactory.CreateAsyncScope();
        var services = scope.ServiceProvider;
        var tenant = services.GetService<TenantContext>();

        if (tenant is null)
        {
            throw new InvalidOperationException(
                "RetentionWorker requires a TenantContext registration when scheduling is enabled."
            );
        }

        var validator = services.GetRequiredService<RetentionStartupValidator>();
        await validator.ValidateAsync(ct);

        RetentionSweepResult result;
        if (dryRun)
        {
            var preview = services.GetRequiredService<IRetentionPreview>();
            result = await preview.PreviewAsync(tenant, DateTimeOffset.UtcNow, ct);
        }
        else
        {
            var engine = services.GetRequiredService<RetentionSweepEngine>();
            result = await engine.SweepAsync(tenant, DateTimeOffset.UtcNow, ct);
        }

        logger.LogInformation(
            "Cohort worker completed {Mode} iteration for tenant {TenantId} with {EntityCount} entity counts.",
            dryRun ? "dry-run" : "sweep",
            tenant.Id,
            result.Counts.Count
        );
    }

    private static Task DelayUntilNextPollAsync(CancellationToken ct)
    {
        return Task.Delay(IdlePollInterval, ct);
    }
}
