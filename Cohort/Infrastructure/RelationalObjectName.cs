namespace Cohort.Infrastructure;

internal sealed record RelationalObjectName(string Schema, string Name);

internal static class PostgreSqlIdentifier
{
    internal static string Quote(string identifier)
    {
        ArgumentNullException.ThrowIfNull(identifier);
        return $"\"{identifier.Replace("\"", "\"\"")}\"";
    }

    internal static string Format(RelationalObjectName identifier)
    {
        ArgumentNullException.ThrowIfNull(identifier);
        return $"{Quote(identifier.Schema)}.{Quote(identifier.Name)}";
    }
}
