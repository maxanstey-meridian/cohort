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
        await validator.ValidateAsync(ct);
        return registry.Scan();
    }
}
