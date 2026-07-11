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

        if (options.SweepBatchSize < 1)
        {
            errors.Add("Cohort SweepBatchSize must be at least 1.");
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

        var dispatch = options.RowHandlerDispatch;
        if (dispatch is null)
        {
            errors.Add("Cohort RowHandlerDispatch cannot be null.");
            return ValidateOptionsResult.Fail(errors);
        }
        if (dispatch.PollInterval <= TimeSpan.Zero)
        {
            errors.Add("Cohort RowHandlerDispatch PollInterval must be greater than zero.");
        }
        if (dispatch.BatchSize < 1)
        {
            errors.Add("Cohort RowHandlerDispatch BatchSize must be at least 1.");
        }
        if (dispatch.MaxAttempts < 1)
        {
            errors.Add("Cohort RowHandlerDispatch MaxAttempts must be at least 1.");
        }
        if (dispatch.MaxParallelism < 1)
        {
            errors.Add("Cohort RowHandlerDispatch MaxParallelism must be at least 1.");
        }
        if (dispatch.BaseBackoff <= TimeSpan.Zero)
        {
            errors.Add("Cohort RowHandlerDispatch BaseBackoff must be greater than zero.");
        }
        if (dispatch.ClaimTimeout < TimeSpan.FromSeconds(30))
        {
            errors.Add("Cohort RowHandlerDispatch ClaimTimeout must be at least 30 seconds.");
        }

        if (dispatch.SweepSettleTimeout < TimeSpan.FromMinutes(1))
        {
            errors.Add("Cohort RowHandlerDispatch:SweepSettleTimeout must be at least 1 minute.");
        }
        if (dispatch.PayloadRetention < TimeSpan.FromHours(1))
        {
            errors.Add("Cohort RowHandlerDispatch PayloadRetention must be at least 1 hour.");
        }

        return errors.Count == 0
            ? ValidateOptionsResult.Success
            : ValidateOptionsResult.Fail(errors);
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
}
