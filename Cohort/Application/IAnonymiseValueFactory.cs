using Cohort.Domain;

namespace Cohort.Application;

public interface IAnonymiseValueFactory
{
    public bool RequiresOriginalValue => false;

    public object? Create(AnonymiseValueContext context);
}
