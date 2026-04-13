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

    public static Type Resolve(string persistedName)
    {
        var normalized = Normalize(persistedName);

        var resolved = Type.GetType(normalized, throwOnError: false, ignoreCase: false);
        if (resolved is not null)
        {
            return resolved;
        }

        foreach (var assembly in AppDomain.CurrentDomain.GetAssemblies())
        {
            resolved = assembly.GetType(normalized, throwOnError: false, ignoreCase: false);
            if (resolved is not null)
            {
                return resolved;
            }
        }

        throw new InvalidOperationException(
            $"Retention type '{persistedName}' could not be resolved."
        );
    }

    [GeneratedRegex(@", Version=[^,\]]+", RegexOptions.CultureInvariant)]
    private static partial Regex VersionPattern();

    [GeneratedRegex(@", Culture=[^,\]]+", RegexOptions.CultureInvariant)]
    private static partial Regex CulturePattern();

    [GeneratedRegex(@", PublicKeyToken=[^,\]]+", RegexOptions.CultureInvariant)]
    private static partial Regex PublicKeyTokenPattern();
}
