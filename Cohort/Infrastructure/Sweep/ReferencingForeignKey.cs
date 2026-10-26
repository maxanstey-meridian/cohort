using System.Data.Common;

using Cohort.Domain;

namespace Cohort.Infrastructure.Sweep;

/// <summary>
/// A <c>RESTRICT</c> or <c>NO ACTION</c> foreign key from <see cref="ChildTable"/> into a
/// retained table: while a child row references a parent, deleting the parent fails. Read
/// from <c>pg_catalog</c> rather than the EF model, so database-only keys and tables outside
/// the host's DbContext count too.
/// </summary>
internal sealed record ReferencingForeignKey(
    string ConstraintName,
    RelationalObjectName ChildTable,
    IReadOnlyList<string> ChildColumns,
    IReadOnlyList<string> ParentColumns
);

internal static class ReferencingForeignKeySql
{
    /// <summary>
    /// Gives every Purge scope the foreign keys that would block deleting its rows, read once
    /// per run. Only a deletion is blocked by a reference: the other strategies keep the row.
    /// A child table the session can't read is left out, so the sweep never fails on it;
    /// the database still refuses the delete, and that row is blocked when it is tried. An
    /// entity <paramref name="hasDependents"/> gets none: its step may retire what references
    /// a row, so its rows are tried, and a row still referenced after the step is blocked by
    /// the constraint itself.
    /// </summary>
    public static async Task<IReadOnlyList<SweepScope>> AttachAsync(
        DbConnection connection,
        IReadOnlyList<SweepScope> plan,
        Func<RetentionEntry, bool> hasDependents,
        CancellationToken ct
    )
    {
        bool Blockable(SweepScope scope) => scope.Rule.Strategy == Strategy.Purge && !hasDependents(scope.Entry);

        var tables = plan
            .Where(Blockable)
            .Select(scope => scope.Entry.Table)
            .Distinct()
            .ToArray();
        if (tables.Length == 0)
        {
            return plan;
        }

        var references = tables.ToDictionary(table => table, _ => new List<ReferencingForeignKey>());
        await using (
            var command = new SqlParams
            {
                ["schemas"] = tables.Select(table => table.Schema).ToArray(),
                ["tables"] = tables.Select(table => table.Name).ToArray(),
            }.CreateCommand(
                connection,
                null,
                """
                SELECT requested.ordinal,
                       foreign_key.conname,
                       child_namespace.nspname,
                       child.relname,
                       ARRAY(SELECT attribute.attname
                             FROM pg_catalog.unnest(foreign_key.conkey) WITH ORDINALITY AS key(attnum, position)
                             JOIN pg_catalog.pg_attribute attribute
                               ON attribute.attrelid = foreign_key.conrelid AND attribute.attnum = key.attnum
                             ORDER BY key.position),
                       ARRAY(SELECT attribute.attname
                             FROM pg_catalog.unnest(foreign_key.confkey) WITH ORDINALITY AS key(attnum, position)
                             JOIN pg_catalog.pg_attribute attribute
                               ON attribute.attrelid = foreign_key.confrelid AND attribute.attnum = key.attnum
                             ORDER BY key.position)
                FROM ROWS FROM (
                    pg_catalog.unnest(@schemas),
                    pg_catalog.unnest(@tables)
                ) WITH ORDINALITY AS requested(schema_name, table_name, ordinal)
                JOIN pg_catalog.pg_namespace namespace ON namespace.nspname = requested.schema_name
                JOIN pg_catalog.pg_class parent
                  ON parent.relnamespace = namespace.oid AND parent.relname = requested.table_name
                JOIN pg_catalog.pg_constraint foreign_key
                  ON foreign_key.contype = 'f'
                 AND foreign_key.confrelid = parent.oid
                 AND foreign_key.confdeltype IN ('a', 'r')
                 -- A partitioned child's key is cloned onto each partition; the root's covers them.
                 AND foreign_key.conparentid = 0
                JOIN pg_catalog.pg_class child ON child.oid = foreign_key.conrelid
                JOIN pg_catalog.pg_namespace child_namespace ON child_namespace.oid = child.relnamespace
                WHERE pg_catalog.has_table_privilege(foreign_key.conrelid, 'SELECT')
                ORDER BY requested.ordinal, foreign_key.conname
                """
            )
        )
        {
            await using var reader = await command.ExecuteReaderAsync(ct);
            while (await reader.ReadAsync(ct))
            {
                references[tables[reader.GetInt64(0) - 1]].Add(
                    new ReferencingForeignKey(
                        reader.GetString(1),
                        new RelationalObjectName(reader.GetString(2), reader.GetString(3)),
                        reader.GetFieldValue<string[]>(4),
                        reader.GetFieldValue<string[]>(5)
                    )
                );
            }
        }

        return plan
            .Select(scope => Blockable(scope)
                ? scope with { BlockingReferences = references[scope.Entry.Table] }
                : scope)
            .ToArray();
    }

    /// <summary>
    /// Predicate over <c>target</c> for rows no blocking key references, or "" when nothing
    /// can block them. A self-reference from the row to itself doesn't block its own delete.
    /// </summary>
    public static string NotReferencedSql(SweepScope scope)
    {
        var clauses = new List<string>();
        for (var index = 0; index < scope.BlockingReferences.Count; index++)
        {
            var reference = scope.BlockingReferences[index];
            var alias = $"cohort_reference{index}";
            var matches = reference.ChildColumns
                .Zip(reference.ParentColumns, (child, parent) =>
                    $"{alias}.{PostgreSqlIdentifier.Quote(child)} = target.{PostgreSqlIdentifier.Quote(parent)}")
                .ToList();
            if (reference.ChildTable == scope.Entry.Table)
            {
                matches.Add($"({alias}.tableoid, {alias}.ctid) <> (target.tableoid, target.ctid)");
            }

            clauses.Add(
                $"NOT EXISTS (SELECT 1 FROM {PostgreSqlIdentifier.Format(reference.ChildTable)} AS {alias} WHERE {string.Join(" AND ", matches)})"
            );
        }

        return string.Join(" AND ", clauses);
    }
}
