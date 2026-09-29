using System.Text.RegularExpressions;

namespace Cohort.Infrastructure.Sweep;

internal static partial class PostgresStoreTypeSql
{
    /// <summary>
    /// The store type to cast a parameter to, or null when it is not a plain type name.
    /// Length and precision modifiers are dropped: an explicit cast to <c>varchar(8)</c>
    /// silently truncates, which would let an over-long value equal a different row's.
    /// </summary>
    internal static string? CastType(string? storeType)
    {
        if (storeType is null || StoreTypePattern().Match(storeType) is not { Success: true } match)
        {
            return null;
        }

        var baseType = match.Groups["base"].Value;
        // Without a modifier these mean character(1) and bit(1), which truncate too.
        baseType = baseType.ToLowerInvariant() switch
        {
            "character" or "char" => "bpchar",
            "bit" => "varbit",
            _ => baseType,
        };
        return baseType + match.Groups["array"].Value;
    }

    [GeneratedRegex(
        "^(?<base>[A-Za-z_][A-Za-z0-9_]*(?:\\.[A-Za-z_][A-Za-z0-9_]*)?(?: [A-Za-z_][A-Za-z0-9_]*)*)(?:\\([0-9]+(?:,[0-9]+)?\\))?(?<array>(?:\\[\\])?)$",
        RegexOptions.CultureInvariant
    )]
    private static partial Regex StoreTypePattern();
}
