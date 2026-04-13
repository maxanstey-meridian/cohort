namespace Cohort.Domain;

[AttributeUsage(AttributeTargets.Class, Inherited = false, AllowMultiple = false)]
public sealed class RetentionTenantlessAttribute : Attribute;
