using System.Text.RegularExpressions;

namespace Cohort.Infrastructure.Sweep;

internal static partial class PostgresStoreTypeSql
{
    internal static string? Validate(string? storeType)
    {
        return storeType is not null && StoreTypePattern().IsMatch(storeType) ? storeType : null;
    }

    [GeneratedRegex(
        "^[A-Za-z_][A-Za-z0-9_]*(?:\\.[A-Za-z_][A-Za-z0-9_]*)?(?: [A-Za-z_][A-Za-z0-9_]*)*(?:\\([0-9]+(?:,[0-9]+)?\\))?(?:\\[\\])?$",
        RegexOptions.CultureInvariant
    )]
    private static partial Regex StoreTypePattern();
}
