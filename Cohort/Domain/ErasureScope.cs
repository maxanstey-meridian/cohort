namespace Cohort.Domain;

public sealed record ErasureScope
{
    public ErasureScope(object subject)
    {
        Subject = subject ?? throw new ArgumentNullException(nameof(subject));
    }

    public object Subject { get; }
}
