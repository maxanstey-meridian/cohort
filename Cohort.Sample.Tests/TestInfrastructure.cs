using System.Collections;
using System.Data.Common;
using Cohort.Hosting;
using Cohort.Infrastructure;
using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using Npgsql;

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

    public TimeSpan AuditObserverTimeout => options.CurrentValue.AuditObservers.Timeout;

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

public sealed class PostgreSqlCommandRecorder : ILoggerProvider, ILogger
{
    private readonly List<RecordedPostgreSqlCommand> commands = [];
    private readonly object gate = new();
    private ILoggerFactory? loggerFactory;

    internal NpgsqlDataSource? DataSource { get; private set; }

    internal NpgsqlDataSource CreateDataSource(string connectionString)
    {
        loggerFactory = LoggerFactory.Create(builder =>
        {
            builder.SetMinimumLevel(LogLevel.Trace);
            builder.AddProvider(this);
        });
        var builder = new NpgsqlDataSourceBuilder(connectionString);
        builder.UseLoggerFactory(loggerFactory);
        builder.EnableParameterLogging();
        DataSource = builder.Build();
        return DataSource;
    }

    internal IReadOnlyList<RecordedPostgreSqlCommand> Commands
    {
        get
        {
            lock (gate)
            {
                return commands.ToArray();
            }
        }
    }

    public ILogger CreateLogger(string categoryName) => this;

    public bool IsEnabled(LogLevel logLevel) => logLevel >= LogLevel.Debug;

    public IDisposable? BeginScope<TState>(TState state)
        where TState : notnull => null;

    public void Log<TState>(
        LogLevel logLevel,
        EventId eventId,
        TState state,
        Exception? exception,
        Func<TState, Exception?, string> formatter
    )
    {
        if (state is not IEnumerable<KeyValuePair<string, object?>> values)
        {
            return;
        }

        var fields = values.ToDictionary(pair => pair.Key, pair => pair.Value);
        if (!fields.TryGetValue("CommandText", out var commandTextValue))
        {
            return;
        }

        var commandText = commandTextValue?.ToString();
        if (string.IsNullOrWhiteSpace(commandText))
        {
            return;
        }

        var parameters = fields.TryGetValue("Parameters", out var parameterValue)
            ? ReadParameters(parameterValue)
            : [];
        lock (gate)
        {
            commands.Add(new RecordedPostgreSqlCommand(commandText, parameters));
        }
    }

    public void Dispose()
    {
        loggerFactory?.Dispose();
    }

    private static RecordedPostgreSqlParameter[] ReadParameters(object? value) => value switch
    {
        IEnumerable<DbParameter> parameters => parameters.Select(ReadParameter).ToArray(),
        IEnumerable enumerable => enumerable.Cast<object>().Select(ReadParameter).ToArray(),
        _ => [],
    };

    private static RecordedPostgreSqlParameter ReadParameter(object value)
    {
        if (value is DbParameter parameter)
        {
            return new RecordedPostgreSqlParameter(
                parameter.ParameterName,
                parameter.Value is string ? null : GetCollectionCount(parameter.Value)
            );
        }

        if (value is string text && text.StartsWith('[') && text.EndsWith(']'))
        {
            var content = text[1..^1];
            return new RecordedPostgreSqlParameter(
                "",
                content.Length == 0
                    ? 0
                    : content.Split(", ", StringSplitOptions.None).Length
            );
        }

        if (value is not string && GetCollectionCount(value) is { } collectionCount)
        {
            return new RecordedPostgreSqlParameter("", collectionCount);
        }

        var type = value.GetType();
        var name = type.GetProperty("ParameterName")?.GetValue(value)?.ToString() ?? "";
        var parameterValue = type.GetProperty("Value")?.GetValue(value);
        return new RecordedPostgreSqlParameter(
            name,
            parameterValue is string ? null : GetCollectionCount(parameterValue)
        );
    }

    private static int? GetCollectionCount(object? value)
    {
        return value switch
        {
            Array array => array.Length,
            ICollection collection => collection.Count,
            _ => null,
        };
    }
}

internal sealed record RecordedPostgreSqlCommand(
    string CommandText,
    IReadOnlyList<RecordedPostgreSqlParameter> Parameters
)
{
    public int ScalarParameterCount => Parameters.Count(parameter => parameter.ArrayLength is null);
    public IReadOnlyList<int> ArrayLengths => Parameters
        .Where(parameter => parameter.ArrayLength is not null)
        .Select(parameter => parameter.ArrayLength!.Value)
        .ToArray();
}

internal sealed record RecordedPostgreSqlParameter(string Name, int? ArrayLength);
