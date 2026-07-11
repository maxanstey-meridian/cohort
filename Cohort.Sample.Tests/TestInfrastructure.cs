using Cohort.Hosting;
using Cohort.Infrastructure;
using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.Options;

namespace Cohort.Sample.Tests;

internal static class MetadataModelDbContextOptionsExtensions
{
    private const string NonconnectingConnectionString =
        "Host=127.0.0.1;Port=1;Database=cohort_metadata;Username=cohort;Password=cohort;Timeout=1";

    public static DbContextOptionsBuilder<TContext> UseNpgsqlMetadataModel<TContext>(
        this DbContextOptionsBuilder<TContext> builder,
        string _
    )
        where TContext : DbContext => builder.UseNpgsql(NonconnectingConnectionString);
}

internal sealed class TestRetentionExecutionSettings(IOptionsMonitor<CohortOptions> options)
    : IRetentionExecutionSettings
{
    public bool DryRun => options.CurrentValue.DryRun;

    public int SweepBatchSize => options.CurrentValue.SweepBatchSize;

    public RetentionRowHandlerSettings RowHandlerDispatch
    {
        get
        {
            var value = options.CurrentValue.RowHandlerDispatch;
            return new RetentionRowHandlerSettings(
                value.PollInterval,
                value.PayloadRetention,
                value.MaxParallelism,
                value.BatchSize,
                value.MaxAttempts,
                value.BaseBackoff,
                value.ClaimTimeout,
                value.SweepSettleTimeout
            );
        }
    }
}
