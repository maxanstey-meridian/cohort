namespace Cohort.Infrastructure;

internal interface IRetentionExecutionSettings
{
    public bool DryRun { get; }

    public int SweepBatchSize { get; }

    public RetentionRowHandlerSettings RowHandlerDispatch { get; }
}

internal sealed record RetentionRowHandlerSettings(
    TimeSpan PollInterval,
    TimeSpan PayloadRetention,
    int MaxParallelism,
    int BatchSize,
    int MaxAttempts,
    TimeSpan BaseBackoff,
    TimeSpan ClaimTimeout,
    TimeSpan SweepSettleTimeout
);

internal sealed class RetentionModelConventions
{
    public string RecordIdPropertyName { get; init; } = "Id";

    public string TenantPropertyName { get; init; } = "TenantId";

    public string SoftDeletePropertyName { get; init; } = "IsDeleted";

    public string DeletedAtPropertyName { get; init; } = "DeletedAt";

    public string AnonymisedAtPropertyName { get; init; } = "AnonymisedAt";
}
