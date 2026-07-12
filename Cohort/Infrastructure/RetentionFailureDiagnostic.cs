using System.Globalization;
using System.Data.Common;

namespace Cohort.Infrastructure;

internal sealed record RetentionFailureDiagnostic
{
    private const int MaximumExceptionTypeLength = 512;

    private RetentionFailureDiagnostic(Guid diagnosticId, string exceptionType, string machineCode)
    {
        DiagnosticId = diagnosticId;
        ExceptionType = exceptionType;
        MachineCode = machineCode;
    }

    public Guid DiagnosticId { get; }

    public string DiagnosticIdText => DiagnosticId.ToString("N", CultureInfo.InvariantCulture);

    public string ExceptionType { get; }

    public string MachineCode { get; }

    public static RetentionFailureDiagnostic Create(Exception exception)
    {
        ArgumentNullException.ThrowIfNull(exception);

        var root = exception.GetBaseException();
        return new RetentionFailureDiagnostic(
            Guid.NewGuid(),
            FormatExceptionType(root.GetType()),
            GetMachineCode(root)
        );
    }

    public override string ToString() =>
        $"type={ExceptionType};code={MachineCode};diagnosticId={DiagnosticIdText}";

    private static string GetMachineCode(Exception exception)
    {
        if (
            exception is DbException { SqlState.Length: 5 } databaseException
            && databaseException.SqlState.All(character =>
                character is >= '0' and <= '9' or >= 'A' and <= 'Z'
            )
        )
        {
            return $"sqlstate:{databaseException.SqlState}";
        }

        return $"hresult:0x{unchecked((uint)exception.HResult):X8}";
    }

    private static string FormatExceptionType(Type type)
    {
        var name = type.FullName ?? type.Name;
        var safeName = new string(
            name.Take(MaximumExceptionTypeLength)
                .Select(character =>
                    character is >= 'a' and <= 'z'
                        or >= 'A' and <= 'Z'
                        or >= '0' and <= '9'
                        or '.'
                        or '_'
                        or '+'
                        or '`'
                        ? character
                        : '_'
                )
                .ToArray()
        );
        return safeName.Length == 0 ? "System.Exception" : safeName;
    }
}
