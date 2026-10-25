using Cohort.Application;
using Cohort.Domain;

using Microsoft.Extensions.DependencyInjection;

namespace Cohort.Infrastructure;

/// <summary>
/// The only way Cohort creates a DI scope: create it, and run the work inside the host's
/// <see cref="IRetentionExecutionScope"/>, so whatever the host sets up around the work (its
/// tenant, an <see cref="AsyncLocal{T}"/> flow) encloses everything the work does.
/// </summary>
internal static class RetentionExecutionScopes
{
    internal static async Task<T> RunInScopeAsync<T>(
        this IServiceScopeFactory scopeFactory,
        RetentionExecutionContext context,
        Func<IServiceProvider, Task<T>> work,
        CancellationToken ct
    )
    {
        await using var scope = scopeFactory.CreateAsyncScope();
        var hook = scope.ServiceProvider.GetRequiredService<IRetentionExecutionScope>();
        var runs = 0;
        Task<T>? run = null;
        var returnedEarly = false;
        try
        {
            await hook.RunAsync(
                context,
                () =>
                {
                    if (Interlocked.Increment(ref runs) != 1)
                    {
                        throw Refusal(hook, $"ran Cohort's {context} work more than once");
                    }

                    return run = RunAsync(work, scope.ServiceProvider);
                },
                ct
            );
        }
        finally
        {
            // Never dispose the scope under work the hook left running.
            returnedEarly = run is { IsCompleted: false };
            if (returnedEarly)
            {
                await ((Task)run!).ConfigureAwait(ConfigureAwaitOptions.SuppressThrowing);
            }
        }

        // The hook's own return says nothing about the work: it may have swallowed the work's
        // failure, or Cohort's refusal of a second run. The work's task is the answer.
        if (run is null)
        {
            throw Refusal(hook, $"returned without running Cohort's {context} work");
        }

        if (Volatile.Read(ref runs) > 1)
        {
            throw Refusal(hook, $"ran Cohort's {context} work more than once");
        }

        if (returnedEarly)
        {
            throw Refusal(hook, $"returned before Cohort's {context} work finished");
        }

        return await run;
    }

    // Async, so a synchronous throw from the work faults its task rather than escaping to the
    // hook, which could swallow it before Cohort sees it.
    private static async Task<T> RunAsync<T>(Func<IServiceProvider, Task<T>> work, IServiceProvider services) =>
        await work(services);

    private static InvalidOperationException Refusal(IRetentionExecutionScope hook, string what) =>
        new($"{hook.GetType().FullName}.RunAsync {what}; await it exactly once and let its exceptions propagate.");

    internal static Task RunInScopeAsync(
        this IServiceScopeFactory scopeFactory,
        RetentionExecutionContext context,
        Func<IServiceProvider, Task> work,
        CancellationToken ct
    ) =>
        scopeFactory.RunInScopeAsync<bool>(
            context,
            async services =>
            {
                await work(services);
                return true;
            },
            ct
        );

    /// <summary>
    /// The context for one tenant's work, or the tenantless pass for
    /// <see cref="TenantContext.Tenantless"/>.
    /// </summary>
    internal static RetentionExecutionContext For(TenantContext tenant) => ForTenant(tenant.Id);

    /// <summary>
    /// The context for one tenant's work, or the tenantless pass for <see cref="Guid.Empty"/>,
    /// the tenant ID Cohort records tenantless work under.
    /// </summary>
    internal static RetentionExecutionContext ForTenant(Guid tenantId) =>
        tenantId == Guid.Empty
            ? new RetentionExecutionContext.System(RetentionSystemWork.TenantlessPass)
            : new RetentionExecutionContext.Tenant(tenantId);
}
