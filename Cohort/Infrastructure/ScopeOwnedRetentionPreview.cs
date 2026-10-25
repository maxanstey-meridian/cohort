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

        var tenantId = request switch
        {
            RetentionPreviewRequest.TenantedRequest tenanted => tenanted.Tenant.Id,
            RetentionPreviewRequest.TenantlessRequest => Guid.Empty,
            _ => throw new ArgumentOutOfRangeException(nameof(request)),
        };
        await using var scope = await scopeFactory.CreateEnteredScopeAsync(
            RetentionExecutionScopes.ForTenant(tenantId),
            ct
        );
        var preview = scope.ServiceProvider.GetRequiredService<RetentionPreviewService>();
        return await preview.ExecuteAsync(request, ct);
    }
}
