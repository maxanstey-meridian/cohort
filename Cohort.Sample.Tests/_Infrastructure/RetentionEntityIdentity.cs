using Cohort.Domain;

namespace Cohort.Sample.Tests;

internal static class RetentionEntityIdentity
{
    internal static Guid For<TEntity>() =>
        typeof(TEntity)
                .GetCustomAttributes(typeof(RetentionEntityIdAttribute), inherit: false)
                .Cast<RetentionEntityIdAttribute>()
                .Single()
                .Id;

    internal static Guid ForTable(string tableName) =>
        tableName switch
        {
            "notes" => For<Cohort.Sample.Entities.Note>(),
            "soft_delete_records" => For<Cohort.Sample.Entities.SoftDeleteRecord>(),
            "anonymised_contacts" => For<Cohort.Sample.Entities.AnonymisedContact>(),
            "nullable_anchor_events" => For<Cohort.Sample.Entities.NullableAnchorEvent>(),
            _ => throw new InvalidOperationException($"No test retention identity is registered for '{tableName}'."),
        };
}
