using Cohort.Application;
using Cohort.Domain;
using Microsoft.Extensions.DependencyInjection;

namespace Cohort.Infrastructure;

internal sealed class ScopeOwnedRetentionPreview(IServiceScopeFactory scopeFactory)
    : IRetentionPreview
{
    public async Task<RetentionSweepResult> ExecuteAsync(
        RetentionPreviewRequest request,
        CancellationToken ct = default
    )
    {
        ArgumentNullException.ThrowIfNull(request);

        await using var scope = scopeFactory.CreateAsyncScope();
        var preview = scope.ServiceProvider.GetRequiredService<RetentionPreviewService>();
        return await preview.ExecuteAsync(request, ct);
    }
}
