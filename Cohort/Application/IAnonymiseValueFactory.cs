using Cohort.Domain;

namespace Cohort.Application;

public interface IAnonymiseValueFactory
{
    public AnonymiseFactoryExecutionMode ExecutionMode => AnonymiseFactoryExecutionMode.Static;

    public object? Create(AnonymiseValueContext context);
}

public enum AnonymiseFactoryExecutionMode
{
    Static,
    PerRow,
    PerRowWithOriginalValue,
}
