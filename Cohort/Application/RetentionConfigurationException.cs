namespace Cohort.Application;

public sealed class RetentionConfigurationException : Exception
{
    public RetentionConfigurationException(IEnumerable<string> errors)
        : base(BuildMessage(errors, out var materializedErrors))
    {
        Errors = materializedErrors;
    }

    public IReadOnlyList<string> Errors { get; }

    private static string BuildMessage(
        IEnumerable<string> errors,
        out IReadOnlyList<string> materializedErrors
    )
    {
        materializedErrors = errors.Distinct().ToArray();
        if (materializedErrors.Count == 0)
        {
            throw new ArgumentException(
                "At least one retention configuration error is required.",
                nameof(errors)
            );
        }

        return
            """
            Retention configuration is invalid:
            """
            + Environment.NewLine
            + string.Join(
                Environment.NewLine,
                materializedErrors.Select(error => $"- {error}")
            );
    }
}
