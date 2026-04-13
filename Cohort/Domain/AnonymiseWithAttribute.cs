namespace Cohort.Domain;

[AttributeUsage(AttributeTargets.Property, Inherited = false, AllowMultiple = false)]
public sealed class AnonymiseWithAttribute(Type factoryType) : Attribute
{
    public Type FactoryType { get; } =
        factoryType ?? throw new ArgumentNullException(nameof(factoryType));
}
