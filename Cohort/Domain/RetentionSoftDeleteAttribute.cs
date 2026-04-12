namespace Cohort.Domain;

[AttributeUsage(AttributeTargets.Property, Inherited = false, AllowMultiple = false)]
public sealed class RetentionSoftDeleteAttribute : Attribute;
