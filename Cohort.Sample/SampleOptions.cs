using System.ComponentModel.DataAnnotations;

namespace Cohort.Sample;

public sealed class SampleOptions
{
    // Deliberately not "Cohort": that section is reserved for the library's own
    // CohortOptions (Schedule/DryRun/KillSwitch/ApplyMigrations), and sharing it
    // teaches consumers a config surface Cohort does not have.
    public const string SectionName = "Sample";

    [Required]
    [MinLength(1)]
    public string ConnectionString { get; init; } = "";
}
