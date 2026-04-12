namespace Cohort.Hosting;

public sealed class CohortOptions
{
    public const string SectionName = "Cohort";

    public string? Schedule { get; init; }

    public bool DryRun { get; init; }

    public bool KillSwitch { get; init; }

    public bool ApplyMigrations { get; init; }
}
