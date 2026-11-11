namespace Cohort.Infrastructure;

internal sealed record RelationalObjectName(string Schema, string Name);

internal static class PostgreSqlIdentifier
{
    internal static string Quote(string identifier) =>
        $"\"{identifier.Replace("\"", "\"\"")}\"";

    internal static string Format(RelationalObjectName identifier) =>
        $"{Quote(identifier.Schema)}.{Quote(identifier.Name)}";
}
