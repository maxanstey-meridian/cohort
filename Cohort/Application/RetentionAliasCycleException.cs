namespace Cohort.Application;

public sealed class RetentionAliasCycleException : Exception
{
    public RetentionAliasCycleException() { }

    public RetentionAliasCycleException(string? message)
        : base(message) { }

    public RetentionAliasCycleException(string? message, Exception? innerException)
        : base(message, innerException) { }
}
