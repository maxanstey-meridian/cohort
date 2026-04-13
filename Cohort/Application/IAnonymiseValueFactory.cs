using Cohort.Domain;

namespace Cohort.Application;

public interface IAnonymiseValueFactory
{
    public bool RequiresPerRowExecution => RequiresOriginalValue;

    public bool RequiresOriginalValue => false;

    public object? Create(AnonymiseValueContext context);
}
