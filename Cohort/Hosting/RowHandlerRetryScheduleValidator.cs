using Cohort.Infrastructure.Sweep;

using Microsoft.Extensions.Options;

namespace Cohort.Hosting;

/// <summary>
/// Checks <c>PayloadRetention</c> against the registered row handlers, on start and on every
/// reload. The scrub clock starts when a row is mutated, and the row's handlers run one after
/// another, so the entity with the most handlers sets how long its work can keep retrying.
/// <see cref="CohortOptionsValidator"/> keeps the single-handler floor and the bounds.
/// </summary>
internal sealed class RowHandlerRetryScheduleValidator(RetentionHandlerServiceRegistrations handlers)
    : IValidateOptions<CohortOptions>
{
    public ValidateOptionsResult Validate(string? name, CohortOptions options)
    {
        ArgumentNullException.ThrowIfNull(options);

        // Out-of-bounds options are CohortOptionsValidator's to report, and the span is only
        // defined within them.
        if (handlers.MostHandlersOnOneEntity() is not { } busiest
            || new CohortOptionsValidator().Validate(name, options).Failed)
        {
            return ValidateOptionsResult.Skip;
        }

        var dispatch = options.RowHandlerDispatch;
        var span = RetentionRowDispatcher.RowRetryScheduleSpan(dispatch, busiest.Count);
        return span < dispatch.PayloadRetention
            ? ValidateOptionsResult.Success
            : ValidateOptionsResult.Fail(
                $"Cohort RowHandlerDispatch PayloadRetention ({dispatch.PayloadRetention}) must be longer than the retry schedule of one row's handlers ({span}): {busiest.Entity.FullName} has {busiest.Count} row handlers, which run one after another, each through MaxAttempts attempts with every backoff wait, ClaimTimeout and PollInterval. Otherwise the payload scrub ends work that is still retrying."
            );
    }
}
