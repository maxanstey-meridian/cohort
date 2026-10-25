using Cohort.Application;
using Cohort.Domain;

using Microsoft.Extensions.DependencyInjection;

namespace Cohort.Infrastructure;

/// <summary>
/// The scoped <see cref="IRetentionErasureService"/>. An <see cref="ErasureTransaction.Owned"/>
/// erasure runs in a fresh scope it creates, through the host's execution-scope hook, as the
/// sweep and preview ports do, so it never shares the caller's DbContext. A
/// <see cref="ErasureTransaction.Caller"/> erasure runs on this scope's DbContext and joins its
/// open transaction; the host already set that scope up, so the hook isn't called for the
/// erasure itself. Each batch with row handlers still gets its own scope through the hook,
/// on the caller's connection and inside its transaction (see <c>SweepStrategy.ExecuteAsync</c>).
/// </summary>
internal sealed class RetentionErasureEntryPoint(
    IServiceScopeFactory scopeFactory,
    IServiceProvider callerServices
) : IRetentionErasureService
{
    public async Task<ErasureResult> ExecuteAsync(
        RetentionErasureRequest request,
        CancellationToken ct = default
    )
    {
        ArgumentNullException.ThrowIfNull(request);

        if (request.Transaction == ErasureTransaction.Caller)
        {
            // Only the Caller branch may touch the caller's scope.
            return await callerServices
                .GetRequiredService<RetentionErasureService>()
                .ExecuteAsync(request, ct);
        }

        return await scopeFactory.RunInScopeAsync(
            RetentionExecutionScopes.For(request.RunTenant),
            services => services.GetRequiredService<RetentionErasureService>().ExecuteAsync(request, ct),
            ct
        );
    }
}
