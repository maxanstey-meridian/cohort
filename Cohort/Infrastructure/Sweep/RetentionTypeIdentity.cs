using System.Text.RegularExpressions;

namespace Cohort.Infrastructure.Sweep;

internal static partial class RetentionTypeIdentity
{
    public static string GetPersistedName(Type type)
    {
        ArgumentNullException.ThrowIfNull(type);

        var assemblyQualifiedName =
            type.AssemblyQualifiedName
            ?? throw new InvalidOperationException(
                $"Retention type '{type.FullName}' cannot be persisted without an assembly-qualified name."
            );

        return Normalize(assemblyQualifiedName);
    }

    public static string Normalize(string persistedName)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(persistedName);

        return PublicKeyTokenPattern()
            .Replace(
                CulturePattern().Replace(VersionPattern().Replace(persistedName, string.Empty), string.Empty),
                string.Empty
            );
    }

    [GeneratedRegex(@", Version=[^,\]]+", RegexOptions.CultureInvariant)]
    private static partial Regex VersionPattern();

    [GeneratedRegex(@", Culture=[^,\]]+", RegexOptions.CultureInvariant)]
    private static partial Regex CulturePattern();

    [GeneratedRegex(@", PublicKeyToken=[^,\]]+", RegexOptions.CultureInvariant)]
    private static partial Regex PublicKeyTokenPattern();
}
