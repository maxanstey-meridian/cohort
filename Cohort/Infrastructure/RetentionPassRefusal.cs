namespace Cohort.Infrastructure;

/// <summary>Why the worker refused a tenant's pass instead of sweeping it.</summary>
internal enum RetentionPassRefusal
{
    /// <summary>The tenant owns retained rows, but the tenant source didn't return it.</summary>
    NotInTenantSource,

    /// <summary>The tenant source returned the tenant more than once, with different contexts.</summary>
    ConflictingTenantContexts,
}

internal static class RetentionPassRefusals
{
    /// <summary>The refusal's Cohort-defined code, safe to persist in the run's <c>Error</c>.</summary>
    internal static string MachineCode(this RetentionPassRefusal refusal) => refusal switch
    {
        RetentionPassRefusal.NotInTenantSource => "cohort:tenant-not-in-source",
        RetentionPassRefusal.ConflictingTenantContexts => "cohort:conflicting-tenant-contexts",
        _ => throw new ArgumentOutOfRangeException(nameof(refusal)),
    };

    /// <summary>What the host should do about it. Names the tenant, so it goes to the logs only.</summary>
    internal static string Describe(this RetentionPassRefusal refusal, Guid tenantId) => refusal switch
    {
        RetentionPassRefusal.NotInTenantSource =>
            $"Tenant {tenantId} owns rows in retained tables, but IRetentionTenantSource did not return it. Cohort won't sweep a tenant without its jurisdiction and tags; add it to the tenant source.",
        RetentionPassRefusal.ConflictingTenantContexts =>
            $"IRetentionTenantSource returned tenant {tenantId} more than once with different contexts. Cohort won't guess which jurisdiction and tags apply; return one context per tenant.",
        _ => throw new ArgumentOutOfRangeException(nameof(refusal)),
    };
}
