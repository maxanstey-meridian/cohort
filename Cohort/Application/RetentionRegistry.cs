using System.Collections.Frozen;

using Microsoft.EntityFrameworkCore;

namespace Cohort.Application;

/// Walks an EF Core model, reads `[Retain]` attributes, validates anchors, and returns
/// an immutable lookup of `RetentionEntry` records keyed by CLR type.
///
/// Takes `DbContext` as a port-shaped dependency: it's the host's "here is my model"
/// contract. The registry never touches `DbSet`, never issues SQL — it only reads
/// metadata. SQL belongs in `Infrastructure/`.
///
/// Crude error handling: throws `InvalidOperationException` on the first failure.
/// Multi-error aggregation via `RetentionConfigurationException` is Milestone A.
public sealed class RetentionRegistry(DbContext db, RetentionEntryBuilder entryBuilder)
{
    private FrozenDictionary<Type, Domain.RetentionEntry>? cachedEntries;

    public IReadOnlyDictionary<Type, Domain.RetentionEntry> Scan()
    {
        return cachedEntries ??= db
            .Model.GetEntityTypes()
            .Select(entityType => entryBuilder.TryBuild(entityType))
            .Where(entry => entry is not null)
            .Cast<Domain.RetentionEntry>()
            .ToFrozenDictionary(entry => entry.EntityType);
    }
}
