using Cronos;

using Microsoft.Extensions.Options;

namespace Cohort.Hosting;

public sealed class CohortOptionsValidator : IValidateOptions<CohortOptions>
{
    public ValidateOptionsResult Validate(string? name, CohortOptions options)
    {
        ArgumentNullException.ThrowIfNull(options);

        if (string.IsNullOrWhiteSpace(options.Schedule))
        {
            return ValidateOptionsResult.Success;
        }

        try
        {
            CohortScheduleParser.Parse(options.Schedule);
            return ValidateOptionsResult.Success;
        }
        catch (Exception ex)
        {
            return ValidateOptionsResult.Fail(
                $"Cohort schedule '{options.Schedule}' is invalid: {ex.Message}"
            );
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
