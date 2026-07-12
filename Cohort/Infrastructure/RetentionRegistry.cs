using System.Collections.Frozen;
using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.DependencyInjection;

namespace Cohort.Infrastructure;

/// Walks an EF Core model, reads `[Retain]` attributes, validates anchors, and returns
/// an immutable lookup of `RetentionEntry` records keyed by CLR type.
///
/// Takes `DbContext` as a port-shaped dependency: it's the host's "here is my model"
/// contract. The registry never touches `DbSet`, never issues SQL — it only reads
/// metadata. SQL belongs in `Infrastructure/`.
///
internal sealed class RetentionRegistry(
    [FromKeyedServices(CohortServiceKeys.DbContext)] DbContext db,
    RetentionEntryBuilder entryBuilder
)
{
    private FrozenDictionary<Type, RetentionEntry>? cachedEntries;

    public IReadOnlyDictionary<Type, RetentionEntry> Scan()
    {
        return cachedEntries ??= db
            .Model.GetEntityTypes()
            .Select(entityType => entryBuilder.TryBuild(entityType))
            .Where(entry => entry is not null)
            .Cast<RetentionEntry>()
            .ToFrozenDictionary(entry => entry.EntityType);
    }
}
