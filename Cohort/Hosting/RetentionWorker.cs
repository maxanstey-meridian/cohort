using Cohort.Application;
using Cohort.Domain;
using Cohort.Infrastructure;
using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;

namespace Cohort.Hosting;

internal sealed class RetentionWorker(
    IServiceScopeFactory scopeFactory,
    IOptionsMonitor<CohortOptions> optionsMonitor,
    ILogger<RetentionWorker> logger
) : BackgroundService
{
    private static readonly TimeSpan IdlePollInterval = TimeSpan.FromMilliseconds(200);

    // Long schedule gaps are slept in bounded chunks so a gap beyond Task.Delay's
    // ~49.7-day ceiling cannot throw.
    private static readonly TimeSpan MaxScheduleSleepChunk = TimeSpan.FromMinutes(1);

    // Session-level Postgres advisory lock key ("cohort01" in hex). Two replicas firing
    // at the same cron instant must not both sweep: double mutations are mostly benign,
    // but doubled audit runs and doubled handler side effects are not.
    private const long SweepAdvisoryLockKey = 0x636F_686F_7274_3031;

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        // A retention worker must outlive individual failures: a transient database
        // outage or a misconfigured category at 02:00 should cost one iteration,
        // not all future sweeps (or the host, under StopHost behavior).
        while (!stoppingToken.IsCancellationRequested)
        {
            try
            {
                await RunScheduleLoopOnceAsync(stoppingToken);
            }
            catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested)
            {
                break;
            }
            catch (Exception ex)
            {
                logger.LogError(
                    ex,
                    "Cohort worker iteration failed. The worker stays alive and will retry at the next scheduled occurrence."
                );
                await DelayUntilNextPollAsync(stoppingToken);
            }
        }
    }

    private async Task RunScheduleLoopOnceAsync(CancellationToken stoppingToken)
    {
        var currentOptions = optionsMonitor.CurrentValue;
        if (
            currentOptions.KillSwitch
            || string.IsNullOrWhiteSpace(currentOptions.Schedule)
        )
        {
            await DelayUntilNextPollAsync(stoppingToken);
            return;
        }

        var schedule = currentOptions.Schedule;
        DateTimeOffset? nextOccurrence;
        try
        {
            nextOccurrence = CohortScheduleParser.GetNextOccurrence(
                schedule,
                DateTimeOffset.UtcNow
            );
        }
        catch (Exception ex)
        {
            logger.LogError(ex, "Cohort worker schedule is invalid at runtime.");
            await DelayUntilNextPollAsync(stoppingToken);
            return;
        }

        if (nextOccurrence is null)
        {
            await DelayUntilNextPollAsync(stoppingToken);
            return;
        }

        if (!await TrySleepUntilAsync(nextOccurrence.Value, schedule, stoppingToken))
        {
            return;
        }

        var executionOptions = optionsMonitor.CurrentValue;
        if (
            stoppingToken.IsCancellationRequested
            || executionOptions.KillSwitch
            || !string.Equals(executionOptions.Schedule, schedule, StringComparison.Ordinal)
        )
        {
            return;
        }

        await RunIterationAsync(executionOptions.DryRun, stoppingToken);
    }

    private async Task<bool> TrySleepUntilAsync(
        DateTimeOffset occurrence,
        string schedule,
        CancellationToken ct
    )
    {
        var currentKillSwitch = optionsMonitor.CurrentValue.KillSwitch;
        var scheduleChanged = new TaskCompletionSource(
            TaskCreationOptions.RunContinuationsAsynchronously
        );
        using var registration = optionsMonitor.OnChange(updated =>
        {
            if (
                updated.KillSwitch != currentKillSwitch
                || !string.Equals(updated.Schedule, schedule, StringComparison.Ordinal)
            )
            {
                scheduleChanged.TrySetResult();
            }
        });

        while (!ct.IsCancellationRequested)
        {
            var remaining = occurrence - DateTimeOffset.UtcNow;
            if (remaining <= TimeSpan.Zero)
            {
                return true;
            }

            var delay = Task.Delay(
                remaining < MaxScheduleSleepChunk ? remaining : MaxScheduleSleepChunk,
                ct
            );
            if (await Task.WhenAny(delay, scheduleChanged.Task) == scheduleChanged.Task)
            {
                return false;
            }
            await delay;
        }

        return false;
    }

    private async Task RunIterationAsync(bool dryRun, CancellationToken ct)
    {
        await using var scope = scopeFactory.CreateAsyncScope();
        var services = scope.ServiceProvider;
        var db = services.GetRequiredKeyedService<DbContext>(CohortServiceKeys.DbContext);

        // The advisory lock is session-scoped, so the connection must stay open for the
        // whole iteration; the sweep itself reuses the already-open scoped connection.
        await db.Database.OpenConnectionAsync(ct);
        try
        {
            if (!await TryAcquireSweepLockAsync(db, ct))
            {
                logger.LogInformation(
                    "Cohort worker skipped this occurrence: another instance holds the sweep advisory lock."
                );
                return;
            }

            try
            {
                await RunLockedIterationAsync(services, dryRun, ct);
            }
            finally
            {
                await ReleaseSweepLockAsync(db);
            }
        }
        finally
        {
            await db.Database.CloseConnectionAsync();
        }
    }

    private async Task RunLockedIterationAsync(
        IServiceProvider services,
        bool dryRun,
        CancellationToken ct
    )
    {
        var entries = services.GetRequiredService<RetentionRegistry>().Scan().Values;
        var engine = services.GetRequiredService<RetentionSweepEngine>();
        var hasTenantedEntries = entries.Any(entry => entry.Tenant is not null);
        var tenants = hasTenantedEntries
            ? await services.GetRequiredService<IRetentionTenantSource>().GetTenantsAsync(ct)
            : [];

        if (hasTenantedEntries)
        {
            // An empty tenant list is only a problem when tenanted entities exist; a
            // tenantless-only deployment legitimately has no tenants, and its pass
            // below must still run.
            if (tenants.Count == 0)
            {
                logger.LogWarning(
                    "Cohort worker found no tenants to sweep; the IRetentionTenantSource returned an empty list, so tenanted entities will not be swept this iteration."
                );
            }

            foreach (var tenant in tenants)
            {
                if (KillSwitchEngagedMidIteration())
                {
                    return;
                }

                var result = await RunPassAsync(
                    engine,
                    tenant,
                    SweepEntityScope.TenantedOnly,
                    dryRun,
                    ct
                );

                logger.LogInformation(
                    "Cohort worker completed {Mode} iteration for tenant {TenantId} with {EntityCount} entity counts.",
                    dryRun ? "dry-run" : "sweep",
                    tenant.Id,
                    result.Counts.Count
                );
            }
        }

        // Tenantless tables hold one shared row set; sweeping them inside the per-tenant
        // loop retired nothing after the first pass but attributed the audit run (and
        // resolved retention rules) under whichever tenant happened to come first.
        if (entries.Any(entry => entry.Tenant is null))
        {
            if (KillSwitchEngagedMidIteration())
            {
                return;
            }

            var result = await RunPassAsync(
                engine,
                TenantContext.Tenantless,
                SweepEntityScope.TenantlessOnly,
                dryRun,
                ct
            );

            logger.LogInformation(
                "Cohort worker completed tenantless {Mode} with {EntityCount} entity counts.",
                dryRun ? "dry run" : "sweep",
                result.Counts.Count
            );
        }
    }

    private static Task<RetentionSweepResult> RunPassAsync(
        RetentionSweepEngine engine,
        TenantContext tenant,
        SweepEntityScope scope,
        bool dryRun,
        CancellationToken ct
    )
    {
        return dryRun
            ? engine.DryRunAsync(
                tenant,
                DateTimeOffset.UtcNow,
                SweepTriggerKind.Scheduled,
                scope,
                ct
            )
            : engine.SweepAsync(
                tenant,
                DateTimeOffset.UtcNow,
                SweepTriggerKind.Scheduled,
                scope,
                ct
            );
    }

    private bool KillSwitchEngagedMidIteration()
    {
        // The kill switch is an emergency brake: an in-flight sweep finishes, but no
        // further sweep starts — not even the remaining passes of the current iteration.
        if (!optionsMonitor.CurrentValue.KillSwitch)
        {
            return false;
        }

        logger.LogInformation(
            "Cohort worker kill switch engaged; skipping the remainder of this iteration."
        );
        return true;
    }

    private static async Task<bool> TryAcquireSweepLockAsync(DbContext db, CancellationToken ct)
    {
        await using var command = db.Database.GetDbConnection().CreateCommand();
        command.CommandText = "SELECT pg_try_advisory_lock(@key)";
        var parameter = command.CreateParameter();
        parameter.ParameterName = "key";
        parameter.Value = SweepAdvisoryLockKey;
        command.Parameters.Add(parameter);

        return (bool)(await command.ExecuteScalarAsync(ct))!;
    }

    private static async Task ReleaseSweepLockAsync(DbContext db)
    {
        await using var command = db.Database.GetDbConnection().CreateCommand();
        command.CommandText = "SELECT pg_advisory_unlock(@key)";
        var parameter = command.CreateParameter();
        parameter.ParameterName = "key";
        parameter.Value = SweepAdvisoryLockKey;
        command.Parameters.Add(parameter);

        await command.ExecuteScalarAsync(CancellationToken.None);
    }

    private static async Task DelayUntilNextPollAsync(CancellationToken ct)
    {
        try
        {
            await Task.Delay(IdlePollInterval, ct);
        }
        catch (OperationCanceledException) when (ct.IsCancellationRequested)
        {
            // Shutdown during an idle wait is not an error.
        }
    }
}
