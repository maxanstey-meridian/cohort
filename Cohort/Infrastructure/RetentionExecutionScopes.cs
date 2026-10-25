using Cohort.Application;
using Cohort.Domain;

using Microsoft.Extensions.DependencyInjection;

namespace Cohort.Infrastructure;

/// <summary>
/// The only way Cohort creates a DI scope: create it, then enter it through the host's
/// <see cref="IRetentionExecutionScope"/> before anything in it touches the database.
/// </summary>
internal static class RetentionExecutionScopes
{
    internal static async ValueTask<AsyncServiceScope> CreateEnteredScopeAsync(
        this IServiceScopeFactory scopeFactory,
        RetentionExecutionContext context,
        CancellationToken ct
    )
    {
        var scope = scopeFactory.CreateAsyncScope();
        try
        {
            await scope.ServiceProvider.GetRequiredService<IRetentionExecutionScope>().EnterAsync(context, ct);
            return scope;
        }
        catch
        {
            await scope.DisposeAsync();
            throw;
        }
    }

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
