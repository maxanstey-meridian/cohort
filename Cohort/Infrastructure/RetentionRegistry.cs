using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.DependencyInjection;

namespace Cohort.Infrastructure;

/// <summary>
/// The retained entities of the host's EF model, keyed by CLR type. Reads metadata only;
/// entries are built once per model and shared across scopes.
/// </summary>
internal sealed class RetentionRegistry(
    [FromKeyedServices(CohortServiceKeys.DbContext)] DbContext db,
    RetentionEntryBuilder entryBuilder
)
{
    public IReadOnlyDictionary<Type, RetentionEntry> Scan() => entryBuilder.BuildAll(db.Model);
}
