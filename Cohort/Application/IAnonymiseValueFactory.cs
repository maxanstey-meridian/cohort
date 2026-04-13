using Cohort.Domain;

namespace Cohort.Application;

public interface IAnonymiseValueFactory
{
    public object? Create(AnonymiseValueContext context);
}
