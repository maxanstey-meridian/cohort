using Cronos;

using Microsoft.Extensions.Options;

namespace Cohort.Hosting;

internal sealed class CohortOptionsValidator : IValidateOptions<CohortOptions>
{
    public ValidateOptionsResult Validate(string? name, CohortOptions options)
    {
        ArgumentNullException.ThrowIfNull(options);

        var errors = new List<string>();

        if (string.IsNullOrWhiteSpace(options.Schedule))
        {
            // Scheduling is opt-in. An empty schedule disables the worker loop.
        }
        else
        {
            try
            {
                CohortScheduleParser.Parse(options.Schedule);
            }
            catch (Exception ex)
            {
                errors.Add($"Cohort schedule '{options.Schedule}' is invalid: {ex.Message}");
            }
        }

        if (options.OccurrenceRetryDelay <= TimeSpan.Zero)
        {
            errors.Add("Cohort OccurrenceRetryDelay must be greater than zero.");
        }
        else if (options.OccurrenceRetryDelay > TimeSpan.FromDays(1))
        {
            errors.Add("Cohort OccurrenceRetryDelay must not exceed 1 day.");
        }

        if (options.OccurrenceMaxAttempts is < 1 or > 100)
        {
            errors.Add("Cohort OccurrenceMaxAttempts must be between 1 and 100.");
        }

        if (string.IsNullOrWhiteSpace(options.TimeZone))
        {
            errors.Add("Cohort TimeZone cannot be blank.");
        }
        else if (!TimeZoneInfo.TryFindSystemTimeZoneById(options.TimeZone, out _))
        {
            errors.Add($"Cohort TimeZone '{options.TimeZone}' is not a time zone this host knows.");
        }

        if (options.SweepBatchSize < 1)
        {
            errors.Add("Cohort SweepBatchSize must be at least 1.");
        }

        if (options.AuditObservers is null)
        {
            errors.Add("Cohort AuditObservers cannot be null.");
        }
        else if (options.AuditObservers.Timeout <= TimeSpan.Zero)
        {
            errors.Add("Cohort AuditObservers Timeout must be greater than zero.");
        }
        else if (options.AuditObservers.Timeout > AuditObserverOptions.MaximumTimeout)
        {
            errors.Add("Cohort AuditObservers Timeout must not exceed 1 hour.");
        }

        var conventions = options.Conventions;
        if (conventions is null)
        {
            errors.Add("Cohort Conventions cannot be null.");
        }
        else
        {
            ValidateConventionName(
                conventions.RecordIdPropertyName,
                nameof(conventions.RecordIdPropertyName),
                errors
            );
            ValidateConventionName(
                conventions.TenantPropertyName,
                nameof(conventions.TenantPropertyName),
                errors
            );
            ValidateConventionName(
                conventions.SoftDeletePropertyName,
                nameof(conventions.SoftDeletePropertyName),
                errors
            );
            ValidateConventionName(
                conventions.DeletedAtPropertyName,
                nameof(conventions.DeletedAtPropertyName),
                errors
            );
            ValidateConventionName(
                conventions.AnonymisedAtPropertyName,
                nameof(conventions.AnonymisedAtPropertyName),
                errors
            );
        }

        ValidateHistoryPruning(options.HistoryPruning, errors);

        var dispatch = options.RowHandlerDispatch;
        if (dispatch is null)
        {
            errors.Add("Cohort RowHandlerDispatch cannot be null.");
            return ValidateOptionsResult.Fail(errors);
        }
        var errorsBeforeDispatch = errors.Count;
        if (dispatch.PollInterval <= TimeSpan.Zero)
        {
            errors.Add("Cohort RowHandlerDispatch PollInterval must be greater than zero.");
        }
        if (dispatch.BatchSize < 1)
        {
            errors.Add("Cohort RowHandlerDispatch BatchSize must be at least 1.");
        }
        else if (dispatch.BatchSize > 10_000)
        {
            errors.Add("Cohort RowHandlerDispatch BatchSize must not exceed 10000.");
        }
        if (dispatch.MaxAttempts < 1)
        {
            errors.Add("Cohort RowHandlerDispatch MaxAttempts must be at least 1.");
        }
        else if (dispatch.MaxAttempts > 1_000)
        {
            errors.Add("Cohort RowHandlerDispatch MaxAttempts must not exceed 1000.");
        }
        if (dispatch.MaxParallelism < 1)
        {
            errors.Add("Cohort RowHandlerDispatch MaxParallelism must be at least 1.");
        }
        else if (dispatch.MaxParallelism > 256)
        {
            errors.Add("Cohort RowHandlerDispatch MaxParallelism must not exceed 256.");
        }
        if (dispatch.BaseBackoff <= TimeSpan.Zero)
        {
            errors.Add("Cohort RowHandlerDispatch BaseBackoff must be greater than zero.");
        }
        if (dispatch.MaxBackoff < dispatch.BaseBackoff)
        {
            errors.Add("Cohort RowHandlerDispatch MaxBackoff must be at least BaseBackoff.");
        }
        else if (dispatch.MaxBackoff > TimeSpan.FromDays(1))
        {
            errors.Add("Cohort RowHandlerDispatch MaxBackoff must not exceed 1 day.");
        }
        if (dispatch.ClaimTimeout < TimeSpan.FromSeconds(30))
        {
            errors.Add("Cohort RowHandlerDispatch ClaimTimeout must be at least 30 seconds.");
        }
        else if (dispatch.ClaimTimeout > TimeSpan.FromDays(1))
        {
            errors.Add("Cohort RowHandlerDispatch ClaimTimeout must not exceed 1 day.");
        }

        if (dispatch.SweepSettleTimeout < TimeSpan.FromMinutes(1))
        {
            errors.Add("Cohort RowHandlerDispatch:SweepSettleTimeout must be at least 1 minute.");
        }
        if (dispatch.PayloadRetention < TimeSpan.FromHours(1))
        {
            errors.Add("Cohort RowHandlerDispatch PayloadRetention must be at least 1 hour.");
        }
        else if (errors.Count == errorsBeforeDispatch)
        {
            // Past PayloadRetention the backstop scrubs the snapshot, so work still inside its
            // retry schedule could never complete.
            var span = RetentionRowDispatcher.RetryScheduleSpan(dispatch);
            if (span >= dispatch.PayloadRetention)
            {
                errors.Add(
                    $"Cohort RowHandlerDispatch PayloadRetention ({dispatch.PayloadRetention}) must be longer than a handler's retry schedule ({span}: every backoff wait for MaxAttempts attempts, capped at MaxBackoff, plus MaxAttempts times ClaimTimeout); otherwise the payload scrub ends work that is still retrying."
                );
            }
        }

        return errors.Count == 0
            ? ValidateOptionsResult.Success
            : ValidateOptionsResult.Fail(errors);
    }

    private static void ValidateHistoryPruning(
        HistoryPruningOptions? pruning,
        ICollection<string> errors
    )
    {
        if (pruning is null)
        {
            errors.Add("Cohort HistoryPruning cannot be null.");
            return;
        }

        ValidateRetention(pruning.SucceededRunRetention, nameof(pruning.SucceededRunRetention), errors);
        ValidateRetention(pruning.FailedRunRetention, nameof(pruning.FailedRunRetention), errors);
        ValidateRetention(pruning.InactiveHoldRetention, nameof(pruning.InactiveHoldRetention), errors);
    }

    private static void ValidateRetention(TimeSpan? retention, string name, ICollection<string> errors)
    {
        if (retention is not null && retention < HistoryPruningOptions.MinimumRetention)
        {
            errors.Add($"Cohort HistoryPruning {name} must be at least 1 day when set.");
        }
    }

    private static void ValidateConventionName(
        string value,
        string name,
        ICollection<string> errors
    )
    {
        if (string.IsNullOrWhiteSpace(value))
        {
            errors.Add($"Cohort convention {name} cannot be blank.");
        }
    }
}

internal static class CohortScheduleParser
{
    private static readonly TimeSpan MaximumLookback = TimeSpan.FromDays(8 * 366);
    private static readonly DateTime EarliestUtc = DateTime.SpecifyKind(DateTime.MinValue, DateTimeKind.Utc);

    internal static CronExpression Parse(string schedule)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(schedule);

        var fieldCount = schedule.Split(' ', StringSplitOptions.RemoveEmptyEntries).Length;
        var format = fieldCount switch
        {
            5 => CronFormat.Standard,
            6 => CronFormat.IncludeSeconds,
            _ => throw new InvalidOperationException(
                "Cron schedules must have either 5 fields (minute precision) or 6 fields (second precision)."
            ),
        };

        return CronExpression.Parse(schedule, format);
    }

    internal static DateTimeOffset? GetNextOccurrence(string schedule, DateTimeOffset fromUtc)
    {
        var expression = Parse(schedule);
        var nextUtc = expression.GetNextOccurrence(fromUtc.UtcDateTime, TimeZoneInfo.Utc);
        return nextUtc is null
            ? null
            : new DateTimeOffset(DateTime.SpecifyKind(nextUtc.Value, DateTimeKind.Utc));
    }

    /// <summary>
    /// The latest occurrence at or before <paramref name="nowUtc"/>, or null when there is none
    /// in the last eight years (the longest gap a cron can leave is 29 February across a
    /// century). Cronos only looks forward, so this walks back in doubling windows, each
    /// enumerating only the part the previous window didn't cover.
    /// </summary>
    internal static DateTimeOffset? GetPreviousOccurrence(string schedule, DateTimeOffset nowUtc)
    {
        var expression = Parse(schedule);
        var now = nowUtc.UtcDateTime;
        var covered = now;
        var toInclusive = true;
        for (var window = TimeSpan.FromMinutes(1); covered > EarliestUtc; window *= 2)
        {
            var reach = window < MaximumLookback ? window : MaximumLookback;
            var from = now - EarliestUtc < reach ? EarliestUtc : now - reach;
            DateTime? latest = null;
            foreach (var occurrence in expression.GetOccurrences(from, covered, fromInclusive: true, toInclusive))
            {
                latest = occurrence;
            }

            if (latest is { } found)
            {
                return new DateTimeOffset(DateTime.SpecifyKind(found, DateTimeKind.Utc));
            }

            if (reach == MaximumLookback)
            {
                break;
            }

            covered = from;
            toInclusive = false;
        }

        return null;
    }
}
