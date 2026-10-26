namespace Cohort.Application;

internal interface IRetentionHandlerRegistration
{
    internal Type EntityType { get; }

    internal Type HandlerType { get; }

    internal RowHandlerDispatchPhase DispatchPhase { get; }

    internal Guid Identity { get; }
}

// Closed over the entity/handler pair so each AddRowHandler call registers a distinct
// implementation type. TryAddEnumerable dedupes instance descriptors by the instance's
// concrete type; a single shared registration type would silently drop the dispatch
// phase of every AddRowHandler call after the first.
internal sealed record RetentionHandlerRegistration<TEntity, THandler>(
    RowHandlerDispatchPhase DispatchPhase,
    Guid Identity
) : IRetentionHandlerRegistration
    where THandler : IRetentionHandler<TEntity>
{
    public Type EntityType { get; } = typeof(TEntity);

    public Type HandlerType { get; } = typeof(THandler);
}

internal interface IRetentionDependentsRegistration
{
    internal Type EntityType { get; }

    internal Type StepType { get; }

    internal Guid Identity { get; }
}

// Closed over the entity/step pair for the same reason as RetentionHandlerRegistration:
// TryAddEnumerable dedupes instance descriptors by the instance's concrete type.
internal sealed record RetentionDependentsRegistration<TEntity, TStep>(Guid Identity)
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
}
