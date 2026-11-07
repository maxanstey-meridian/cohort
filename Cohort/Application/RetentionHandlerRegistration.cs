namespace Cohort.Application;

internal interface IRetentionDependentsRegistration
{
    internal Type EntityType { get; }

    internal Type StepType { get; }

    internal bool RetiresReferencingRows { get; }
}

// Closed over the entity/step pair so each AddRetentionDependents call registers a distinct
// implementation type: TryAddEnumerable dedupes instance descriptors by the instance's
// concrete type.
internal sealed record RetentionDependentsRegistration<TEntity, TStep>(bool RetiresReferencingRows)
    : IRetentionDependentsRegistration
    where TStep : IRetentionDependents<TEntity>
{
    public Type EntityType { get; } = typeof(TEntity);

    public Type StepType { get; } = typeof(TStep);
}

internal static class RetentionDependentsRegistrations
{
    /// <summary>
    /// The dependents step registered for <paramref name="entityType"/>, or null without one.
    /// <c>AddRetentionDependents</c> allows one per entity.
    /// </summary>
    public static IRetentionDependentsRegistration? StepFor(
        this IEnumerable<IRetentionDependentsRegistration> registrations,
        Type entityType
    ) => registrations.SingleOrDefault(registration => registration.EntityType == entityType);

    /// <summary>
    /// Whether <paramref name="entityType"/> has a dependents step, read from the
    /// registrations alone so no step is constructed to find out.
    /// </summary>
    public static bool HasStep(this IEnumerable<IRetentionDependentsRegistration> registrations, Type entityType) =>
        registrations.StepFor(entityType) is not null;

    /// <summary>
    /// The one answer to "does purge leave this entity's still-referenced rows out up front".
    /// True only when <paramref name="entityType"/> has a dependents step registered with
    /// <c>retiresReferencingRows</c> true: that step may retire what references a row, so purge
    /// tries the row and a reference left after the step blocks it at the database. Otherwise
    /// purge keeps the blocking-key pre-filter and counts a still-referenced row blocked
    /// without trying it. The run, the preview, and readiness all ask here, so they can't drift.
    /// </summary>
    public static bool RetiresReferencingRows(
        this IEnumerable<IRetentionDependentsRegistration> registrations,
        Type entityType
    ) => registrations.StepFor(entityType) is { RetiresReferencingRows: true };
}
