using System.ComponentModel.DataAnnotations;

namespace Cohort.Sample;

public sealed class SampleOptions
{
    public const string SectionName = "Cohort";

    [Required]
    [MinLength(1)]
    public string ConnectionString { get; init; } = "";
}
