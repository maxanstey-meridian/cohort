namespace Cohort.Application;

internal interface IRetentionHandlerRegistration
{
    internal Type EntityType { get; }

    internal Type HandlerType { get; }

    internal RowHandlerDispatchPhase DispatchPhase { get; }
}

internal sealed record RetentionHandlerRegistration(
    Type EntityType,
    Type HandlerType,
    RowHandlerDispatchPhase DispatchPhase
) : IRetentionHandlerRegistration;
