using Cohort.Application;
using Cohort.Domain;

namespace Cohort.Sample;

public sealed class SampleRetentionStartupService(
    RetentionRegistry registry,
    RetentionStartupValidator validator
)
{
    public async Task<IReadOnlyDictionary<Type, RetentionEntry>> RunAsync(
        CancellationToken ct = default
    )
    {
        var entries = registry.Scan();
        await validator.ValidateAsync(ct);
        return entries;
    }
}
