using Cohort.Application;
using Cohort.Domain;

using Microsoft.Extensions.DependencyInjection;

namespace Cohort.Infrastructure;

internal sealed class ScopeOwnedRetentionPreview(IServiceScopeFactory scopeFactory)
    : IRetentionPreview
{
    public Task<RetentionSweepResult> ExecuteAsync(
        RetentionPreviewRequest request,
        CancellationToken ct = default
    )
    {
        ArgumentNullException.ThrowIfNull(request);

        return scopeFactory.RunInScopeAsync(
            RetentionExecutionScopes.For(request.RunTenant),
            services => services.GetRequiredService<RetentionPreviewService>().ExecuteAsync(request, ct),
            ct
        );
    }
}
