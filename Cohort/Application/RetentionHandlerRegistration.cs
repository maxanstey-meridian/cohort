namespace Cohort.Application;

internal interface IRetentionHandlerRegistration
{
    internal Type EntityType { get; }

    internal Type HandlerType { get; }

    internal RowHandlerDispatchPhase DispatchPhase { get; }

    internal Guid? Identity { get; }
}

// Closed over the entity/handler pair so each AddRowHandler call registers a distinct
// implementation type. TryAddEnumerable dedupes instance descriptors by the instance's
// concrete type; a single shared registration type would silently drop the dispatch
// phase of every AddRowHandler call after the first.
internal sealed record RetentionHandlerRegistration<TEntity, THandler>(
    RowHandlerDispatchPhase DispatchPhase,
    Guid? Identity = null
) : IRetentionHandlerRegistration
    where THandler : IRetentionHandler<TEntity>
{
    public Type EntityType { get; } = typeof(TEntity);

    public Type HandlerType { get; } = typeof(THandler);
}
