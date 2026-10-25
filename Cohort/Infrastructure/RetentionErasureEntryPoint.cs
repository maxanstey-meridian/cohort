using Cohort.Application;
using Cohort.Domain;

using Microsoft.Extensions.DependencyInjection;

namespace Cohort.Infrastructure;

/// <summary>
/// The scoped <see cref="IRetentionErasureService"/>. An <see cref="ErasureTransaction.Owned"/>
/// erasure runs in a fresh scope it creates and enters, as the sweep and preview ports do, so
/// it never shares the caller's DbContext. A <see cref="ErasureTransaction.Caller"/> erasure
/// runs on this scope's DbContext and joins its open transaction; the host already entered
/// that scope, so the execution-scope hook isn't called.
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

        await using var executionScope = await scopeFactory.CreateEnteredScopeAsync(
            RetentionExecutionScopes.For(request.RunTenant),
            ct
        );
        return await executionScope.ServiceProvider
            .GetRequiredService<RetentionErasureService>()
            .ExecuteAsync(request, ct);
    }
}
