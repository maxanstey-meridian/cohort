using Cohort.Domain;

namespace Cohort.Infrastructure.Sweep;

/// <summary>
/// The rows one strategy call acts on: rows past the retention cutoff for a sweep, or one
/// subject's rows (behind an optional legal-minimum cutoff) for an erasure.
/// </summary>
internal sealed record SweepScope(
    RetentionEntry Entry,
    RetentionRule Rule,
    TenantContext Tenant,
    DateTimeOffset Now,
    DateTimeOffset? Cutoff,
    RetentionPeriod ResolvedPeriod,
    ErasureSubjectPredicate? Subject
)
{
    /// <summary>
    /// <see cref="ResolvedPeriod"/> is the age recorded on audit summaries: whichever of the
    /// period and the legal minimum set the cutoff.
    /// </summary>
    public static SweepScope ForSweep(
        RetentionEntry entry,
        RetentionRule rule,
        RetentionResolutionContext context,
        TimeZoneInfo timeZone
    )
    {
        var (cutoff, resolvedPeriod) = CutoffCalculator.ResolveSweep(
            context.Now,
            rule.Period,
            rule.LegalMin,
            timeZone
        );
        return new(entry, rule, context.Tenant, context.Now, cutoff, resolvedPeriod, Subject: null);
    }

    /// <summary>
    /// <see cref="ResolvedPeriod"/> is the legal minimum alone, because an erasure ignores
    /// the period.
    /// </summary>
    public static SweepScope ForErasure(
        RetentionEntry entry,
        RetentionRule rule,
        ErasureSubjectPredicate subject,
        TenantContext tenant,
        DateTimeOffset now,
        TimeZoneInfo timeZone
    )
    {
        var (cutoff, minimumAge) = CutoffCalculator.ResolveErasure(now, rule.LegalMin, timeZone);
        return new(entry, rule, tenant, now, cutoff, minimumAge, subject);
    }

    /// <summary>
    /// A sweep skips rows another run has locked and retires them next time; an erasure
    /// must wait for them, because it has to cover every one of the subject's rows.
    /// </summary>
    public bool SkipLocked => Subject is null;
}

internal enum SweepCount
{
    /// <summary>Rows the strategy would act on now.</summary>
    Eligible,

    /// <summary>Rows it would act on but for an active hold.</summary>
    Held,

    /// <summary>Rows it could act on whose anchor is NULL, so no cutoff ever matches them.</summary>
    NullAnchor,
}
