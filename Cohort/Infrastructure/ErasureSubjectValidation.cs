namespace Cohort.Infrastructure;

/// <summary>
/// The check every use of an erasure subject shares: an erasure, and the read of the holds that
/// cover a subject's rows.
/// </summary>
internal static class ErasureSubjectValidation
{
    /// <summary>
    /// Refuses, before anything runs, a kind no retained entity in <paramref name="entities"/>
    /// declares (<paramref name="work"/> would match nothing) and a subject that isn't of its
    /// kind's CLR type. Neither message includes the subject's value. <paramref name="work"/>
    /// names the caller in the first message ("the erasure"). Run it after readiness: startup
    /// validation fills the state it reads.
    /// </summary>
    internal static void Validate(
        RetentionModelValidation validation,
        RetentionRegistry registry,
        string kind,
        object subject,
        SweepEntityScope entities,
        string work
    )
    {
        var declared = registry.Scan().Values
            .Where(entry => entities.Includes(entry))
            .SelectMany(entry => validation.ErasureSubjects[entry.EntityType])
            .Select(metadata => metadata.Kind)
            .ToHashSet(StringComparer.Ordinal);
        if (!declared.Contains(kind))
        {
            var declaredText = declared.Count == 0
                ? "none"
                : string.Join(", ", declared.Order(StringComparer.Ordinal).Select(declaredKind => $"'{declaredKind}'"));
            var which = entities == SweepEntityScope.TenantedOnly ? "tenanted" : "tenantless";
            throw new InvalidOperationException(
                $"Erasure subject kind '{kind}' isn't declared by any {which} retained entity's [ErasureSubject], so {work} would match nothing. Declared kinds: {declaredText}."
            );
        }

        var subjectType = validation.ErasureSubjectKinds[kind];
        if (!subjectType.IsInstanceOfType(subject))
        {
            throw new InvalidOperationException(
                $"Erasure subject kind '{kind}' identifies subjects by {subjectType.Name}, but the subject is a {subject.GetType().Name}."
            );
        }
    }
}
