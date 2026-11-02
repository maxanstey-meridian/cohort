using System.Reflection;

using Cohort.Domain;
using Cohort.Infrastructure.Sweep;

using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Metadata;

namespace Cohort.Infrastructure.Holds;

/// <summary>Which attribute declared a relation, and so which side owns its link column.</summary>
internal enum HoldRelationShape
{
    /// <summary><c>[HeldWith]</c>: the declaring entity owns the link column.</summary>
    HeldWith,

    /// <summary><c>[HeldWithReferencing]</c>: the related entity owns the link column.</summary>
    HeldWithReferencing,
}

/// <summary>Whether a hop's link column lives on the entity the hop starts from or the one it reaches.</summary>
internal enum HoldLinkSide
{
    /// <summary>On the entity the hop starts from: <c>near.&lt;link&gt; = far.&lt;record id&gt;</c>.</summary>
    Near,

    /// <summary>On the entity the hop reaches: <c>far.&lt;link&gt; = near.&lt;record id&gt;</c>.</summary>
    Far,
}

/// <summary>
/// The scalars a SQL renderer needs about one end of a hop, copied out of the entity's
/// <see cref="RetentionEntry"/> so that entries never reference each other.
/// </summary>
/// <param name="EntityType">The entity's CLR type.</param>
/// <param name="RetentionEntityId">The stable id a hold stores for the entity.</param>
/// <param name="Table">The entity's table.</param>
/// <param name="RecordId">The column and type a link must equal, and a hold's canonical record id is read from.</param>
/// <param name="Tenant">The tenant column, or null for a tenantless entity.</param>
internal sealed record HoldRelationEnd(
    Type EntityType,
    Guid RetentionEntityId,
    RelationalObjectName Table,
    RecordIdConvention RecordId,
    TenantConvention? Tenant
)
{
    internal static HoldRelationEnd From(RetentionEntry entry) =>
        new(entry.EntityType, entry.RetentionEntityId, entry.Table, entry.RecordId, entry.Tenant);
}

/// <summary>One validated attribute declaration: coverage flows from <see cref="Related"/> to <see cref="Declaring"/>.</summary>
/// <param name="Shape">Which attribute declared it.</param>
/// <param name="Declaring">The entity whose rows become held (the one carrying the attribute).</param>
/// <param name="Related">The entity whose hold covers the declaring entity's rows.</param>
/// <param name="LinkMember">The CLR member that names the target row's record id.</param>
/// <param name="LinkColumn">The link member's column, on <see cref="Declaring"/>'s table for <see cref="HoldRelationShape.HeldWith"/> and on <see cref="Related"/>'s for <see cref="HoldRelationShape.HeldWithReferencing"/>.</param>
/// <param name="Declaration">The declaration as startup and readiness name it in messages.</param>
internal sealed record HoldRelation(
    HoldRelationShape Shape,
    HoldRelationEnd Declaring,
    HoldRelationEnd Related,
    string LinkMember,
    string LinkColumn,
    string Declaration
)
{
    /// <summary>The hop from the declaring entity up to the related one, the direction a hold covers it by.</summary>
    internal HoldHop Holder() =>
        new(
            Declaring,
            Related,
            LinkMember,
            LinkColumn,
            Shape == HoldRelationShape.HeldWith ? HoldLinkSide.Near : HoldLinkSide.Far,
            Declaration
        );
}

/// <summary>
/// One step along a path, from <see cref="Near"/> to <see cref="Far"/>. The two rows are joined by
/// the link column, and by tenant when the entities are tenanted (<c>far.&lt;tenant&gt; =
/// near.&lt;tenant&gt;</c>; both ends are tenanted or neither is). The record id each side joins on
/// is the <see cref="HoldRelationEnd.RecordId"/> of the end that is the link's target.
/// </summary>
/// <param name="Near">The end the hop starts from: the entry's own entity for a path's first hop, else the previous hop's <see cref="Far"/>.</param>
/// <param name="Far">The related end the hop reaches.</param>
/// <param name="LinkMember">The CLR member that holds the link.</param>
/// <param name="LinkColumn">The link's column, on <see cref="Near"/>'s table or <see cref="Far"/>'s as <paramref name="LinkSide"/> says.</param>
/// <param name="LinkSide">Which end's table owns <paramref name="LinkColumn"/>.</param>
/// <param name="Declaration">The attribute declaration this hop follows, for messages.</param>
internal sealed record HoldHop(
    HoldRelationEnd Near,
    HoldRelationEnd Far,
    string LinkMember,
    string LinkColumn,
    HoldLinkSide LinkSide,
    string Declaration
)
{
    /// <summary>The same relation walked the other way, from <see cref="Far"/> back to <see cref="Near"/>.</summary>
    internal HoldHop Reverse() =>
        new(
            Far,
            Near,
            LinkMember,
            LinkColumn,
            LinkSide == HoldLinkSide.Near ? HoldLinkSide.Far : HoldLinkSide.Near,
            Declaration
        );
}

/// <summary>A chain of one or more hops, each starting where the previous one reached.</summary>
/// <param name="Hops">The hops in walking order. Never empty.</param>
internal sealed record HoldPath(IReadOnlyList<HoldHop> Hops)
{
    /// <summary>The entity the path starts from.</summary>
    internal HoldRelationEnd Origin => Hops[0].Near;

    /// <summary>The entity the path reaches.</summary>
    internal HoldRelationEnd End => Hops[^1].Far;

    internal HoldPath Reverse() => new([.. Hops.Reverse().Select(hop => hop.Reverse())]);
}

/// <summary>What <see cref="HoldRelations.Resolve"/> found: the valid relations and every refusal.</summary>
internal sealed record HoldRelationResolution(IReadOnlyList<HoldRelation> Relations, IReadOnlyList<string> Errors);

/// <summary>
/// The relations a host declares with <see cref="HeldWithAttribute"/> and
/// <see cref="HeldWithReferencingAttribute"/>, validated once for startup, runtime scans, and
/// readiness, and turned into the paths every hold SQL renderer walks.
/// </summary>
/// <remarks>
/// A row is covered by an active hold on itself (the empty chain) or on a row it reaches through a
/// <em>holder path</em>. <see cref="RetentionEntry.HolderPaths"/> lists, for the entry's entity,
/// every non-empty chain of hops up to an entity whose hold would cover it; each prefix of a chain
/// is itself listed, so a hold on any entity along a chain is found by the path that ends there
/// (<see cref="HoldPath.End"/>). Each hop's <see cref="HoldHop.Near"/> is the entity the walk is
/// on and <see cref="HoldHop.Far"/> the one it reaches. <see cref="RetentionEntry.CoveredPaths"/>
/// is the inverse for a holder entity: every chain of hops, walked the other way, down to the rows
/// its hold covers, again with each prefix listed, ordered by length and then by entity names.
/// The graph is acyclic, so each list is finite.
/// </remarks>
internal static class HoldRelations
{
    private const string HeldWithName = "HeldWith";
    private const string HeldWithReferencingName = "HeldWithReferencing";

    /// <summary>
    /// Reads every declaration in <paramref name="model"/>, refusing the ones that can't work.
    /// <paramref name="entries"/> holds the retained entities that built; a declaration that
    /// touches a retained entity missing from it is skipped, because the entity's own error is
    /// reported where it failed. Only the validated relations are returned.
    /// </summary>
    internal static HoldRelationResolution Resolve(
        IModel model,
        IReadOnlyDictionary<Type, RetentionEntry> entries
    )
    {
        var errors = new List<string>();
        var relations = new List<HoldRelation>();
        var seen = new HashSet<(Type Declaring, Type Related, string Member, HoldRelationShape Shape)>();

        foreach (var entityType in model.GetEntityTypes().OrderBy(type => type.ClrType.FullName, StringComparer.Ordinal))
        {
            foreach (var declaration in ReadDeclarations(entityType.ClrType, errors))
            {
                var relation = ResolveRelation(model, entries, entityType.ClrType, declaration, seen, errors);
                if (relation is not null)
                {
                    relations.Add(relation);
                }
            }
        }

        ValidateAcyclic(relations, errors);
        return new HoldRelationResolution(relations, errors);
    }

    /// <summary>
    /// Attaches each entry's holder paths and each holder's covered paths. <paramref name="relations"/>
    /// must be the error-free result of <see cref="Resolve"/>, so the graph is acyclic.
    /// </summary>
    internal static Dictionary<Type, RetentionEntry> WithPaths(
        IReadOnlyDictionary<Type, RetentionEntry> entries,
        IReadOnlyList<HoldRelation> relations
    )
    {
        if (relations.Count == 0)
        {
            return new Dictionary<Type, RetentionEntry>(entries);
        }

        var byDeclaring = relations
            .OrderBy(relation => relation.Declaring.EntityType.FullName, StringComparer.Ordinal)
            .ThenBy(relation => relation.Related.EntityType.FullName, StringComparer.Ordinal)
            .ThenBy(relation => relation.Shape)
            .ThenBy(relation => relation.LinkMember, StringComparer.Ordinal)
            .ToLookup(relation => relation.Declaring.EntityType);
        var holderPathsByType = new Dictionary<Type, List<HoldPath>>();
        var coveredPathsByType = new Dictionary<Type, List<HoldPath>>();
        foreach (var type in entries.Keys)
        {
            foreach (var path in HolderPathsOf(type, byDeclaring, holderPathsByType))
            {
                if (!coveredPathsByType.TryGetValue(path.End.EntityType, out var covered))
                {
                    coveredPathsByType[path.End.EntityType] = covered = [];
                }

                covered.Add(path.Reverse());
            }
        }

        return entries.ToDictionary(
            pair => pair.Key,
            pair =>
                pair.Value with
                {
                    HolderPaths = holderPathsByType.GetValueOrDefault(pair.Key) ?? [],
                    CoveredPaths =
                    [
                        .. (coveredPathsByType.GetValueOrDefault(pair.Key) ?? [])
                            .OrderBy(path => path.Hops.Count)
                            .ThenBy(
                                path => string.Join('>', path.Hops.Select(hop => hop.Far.EntityType.FullName)),
                                StringComparer.Ordinal
                            ),
                    ],
                }
        );
    }

    private static List<HoldPath> HolderPathsOf(
        Type entityType,
        ILookup<Type, HoldRelation> byDeclaring,
        Dictionary<Type, List<HoldPath>> memo
    )
    {
        if (memo.TryGetValue(entityType, out var known))
        {
            return known;
        }

        var paths = new List<HoldPath>();
        foreach (var relation in byDeclaring[entityType])
        {
            var hop = relation.Holder();
            paths.Add(new HoldPath([hop]));
            paths.AddRange(
                HolderPathsOf(relation.Related.EntityType, byDeclaring, memo)
                    .Select(onward => new HoldPath([hop, .. onward.Hops]))
            );
        }

        memo[entityType] = paths;
        return paths;
    }

    private sealed record Declaration(HoldRelationShape Shape, Type RelatedType, string LinkMember);

    private static IEnumerable<Declaration> ReadDeclarations(Type clrType, List<string> errors)
    {
        var declarations = new List<Declaration>();
        try
        {
            declarations.AddRange(
                clrType
                    .GetCustomAttributes<HeldWithAttribute>(inherit: false)
                    .Select(attribute => new Declaration(HoldRelationShape.HeldWith, attribute.RelatedType, attribute.LinkMember))
            );
            declarations.AddRange(
                clrType
                    .GetCustomAttributes<HeldWithReferencingAttribute>(inherit: false)
                    .Select(attribute =>
                        new Declaration(HoldRelationShape.HeldWithReferencing, attribute.RelatedType, attribute.LinkMember)
                    )
            );
        }
        catch (Exception ex) when (ex is ArgumentException or TargetInvocationException or CustomAttributeFormatException)
        {
            errors.Add(
                $"[{HeldWithName}] or [{HeldWithReferencingName}] on {clrType.FullName} is invalid: {(ex.InnerException ?? ex).Message}"
            );
        }

        return declarations;
    }

    private static string AttributeName(HoldRelationShape shape) =>
        shape == HoldRelationShape.HeldWith ? HeldWithName : HeldWithReferencingName;

    private static string Describe(Type declaring, Declaration declaration) =>
        $"[{AttributeName(declaration.Shape)}(typeof({declaration.RelatedType.FullName}), \"{declaration.LinkMember}\")] on {declaring.FullName}";

    private static HoldRelation? ResolveRelation(
        IModel model,
        IReadOnlyDictionary<Type, RetentionEntry> entries,
        Type declaringType,
        Declaration declaration,
        HashSet<(Type Declaring, Type Related, string Member, HoldRelationShape Shape)> seen,
        List<string> errors
    )
    {
        var label = Describe(declaringType, declaration);
        var relatedType = declaration.RelatedType;

        if (!seen.Add((declaringType, relatedType, declaration.LinkMember, declaration.Shape)))
        {
            errors.Add($"{label}: the same relation is declared more than once. Declare it once.");
            return null;
        }

        if (declaringType.GetCustomAttribute<RetainAttribute>(inherit: false) is null)
        {
            errors.Add(
                $"{label}: {declaringType.FullName} isn't a [Retain] entity, so its rows are never swept and a hold relation on it would do nothing. Annotate it with [Retain], or remove the declaration."
            );
            return null;
        }

        if (relatedType.GetCustomAttribute<RetainAttribute>(inherit: false) is null || model.FindEntityType(relatedType) is null)
        {
            errors.Add(
                $"{label}: {relatedType.FullName} isn't a [Retain] entity in the DbContext model, so no hold can be placed on it. Annotate it with [Retain], map it in the model, or remove the declaration."
            );
            return null;
        }

        if (!entries.TryGetValue(declaringType, out var declaringEntry) || !entries.TryGetValue(relatedType, out var relatedEntry))
        {
            return null;
        }

        var linkOwner = declaration.Shape == HoldRelationShape.HeldWith ? declaringEntry : relatedEntry;
        var target = declaration.Shape == HoldRelationShape.HeldWith ? relatedEntry : declaringEntry;
        var linkProperty = FindLinkProperty(model, linkOwner, declaration.LinkMember, out var linkColumn);
        if (linkProperty is null || linkColumn is null)
        {
            errors.Add(
                $"{label}: link member '{declaration.LinkMember}' isn't a mapped public property with a table column on {linkOwner.EntityType.FullName}, which holds the link for [{AttributeName(declaration.Shape)}]."
            );
            return null;
        }

        var valid = true;
        var targetProperty = model.FindEntityType(target.EntityType)?.FindProperty(target.RecordId.RecordIdMember);
        if (targetProperty is not null && TypeMismatch(linkProperty, targetProperty, target.RecordId) is { } mismatch)
        {
            errors.Add(
                $"{label}: link member {linkOwner.EntityType.FullName}.{declaration.LinkMember} is {mismatch.Link}, but the record id {target.EntityType.FullName}.{target.RecordId.RecordIdMember} it must equal is {mismatch.Target}. A link has to be the same type as the record id it names, because a hold stores that record id."
            );
            valid = false;
        }

        if ((declaringEntry.Tenant is null) != (relatedEntry.Tenant is null))
        {
            var (tenanted, tenantless) = declaringEntry.Tenant is null
                ? (relatedEntry.EntityType, declaringEntry.EntityType)
                : (declaringEntry.EntityType, relatedEntry.EntityType);
            errors.Add(
                $"{label}: {tenanted.FullName} is tenanted but {tenantless.FullName} is tenantless. A hold relation joins rows of one tenant, so both entities must be tenanted or both tenantless."
            );
            valid = false;
        }

        return valid
            ? new HoldRelation(
                declaration.Shape,
                HoldRelationEnd.From(declaringEntry),
                HoldRelationEnd.From(relatedEntry),
                declaration.LinkMember,
                linkColumn,
                label
            )
            : null;
    }

    private static IProperty? FindLinkProperty(IModel model, RetentionEntry owner, string member, out string? column)
    {
        column = null;
        if (ReflectionMemberResolver.FindPropertyByName(owner.EntityType, member) is null)
        {
            return null;
        }

        var entityType = model.FindEntityType(owner.EntityType);
        var property = entityType?.FindProperty(member);
        var storeObject = entityType is null ? null : StoreObjectIdentifier.Create(entityType, StoreObjectType.Table);
        column = storeObject is null ? null : property?.GetColumnName(storeObject.Value);
        return property;
    }

    /// <summary>
    /// Two types match when their provider CLR types agree (after any value converter and after
    /// unwrapping <see cref="Nullable{T}"/>) and, when both store types are known, they agree
    /// once normalised by <see cref="PostgresStoreTypeSql.CastType"/>. Null when they match.
    /// </summary>
    private static (string Link, string Target)? TypeMismatch(IProperty link, IProperty target, RecordIdConvention targetRecordId)
    {
        var linkClr = ProviderClrType(link);
        var targetClr = ProviderClrType(target);
        var linkStore = PostgresStoreTypeSql.CastType(RetentionEntryBuilder.TryGetStoreType(link));
        var targetStore = PostgresStoreTypeSql.CastType(targetRecordId.RecordIdStoreType);
        if (linkClr == targetClr && (linkStore is null || targetStore is null || string.Equals(linkStore, targetStore, StringComparison.OrdinalIgnoreCase)))
        {
            return null;
        }

        return (Name(linkClr, linkStore), Name(targetClr, targetStore));

        static string Name(Type clr, string? store) => store is null ? clr.Name : $"{clr.Name} ({store})";
    }

    private static Type ProviderClrType(IProperty property)
    {
        var type = property.GetTypeMapping().Converter?.ProviderClrType ?? property.ClrType;
        return Nullable.GetUnderlyingType(type) ?? type;
    }

    private static void ValidateAcyclic(IReadOnlyList<HoldRelation> relations, List<string> errors)
    {
        var byDeclaring = relations
            .OrderBy(relation => relation.Declaring.EntityType.FullName, StringComparer.Ordinal)
            .ThenBy(relation => relation.Related.EntityType.FullName, StringComparer.Ordinal)
            .ThenBy(relation => relation.LinkMember, StringComparer.Ordinal)
            .ToLookup(relation => relation.Declaring.EntityType);
        var finished = new HashSet<Type>();
        var onStack = new List<(Type Type, HoldRelation? Via)>();
        var reported = new HashSet<string>(StringComparer.Ordinal);

        foreach (var start in byDeclaring.Select(group => group.Key).OrderBy(type => type.FullName, StringComparer.Ordinal))
        {
            Visit(start, null);
        }

        void Visit(Type type, HoldRelation? via)
        {
            if (finished.Contains(type))
            {
                return;
            }

            onStack.Add((type, via));
            foreach (var relation in byDeclaring[type])
            {
                var next = relation.Related.EntityType;
                var index = onStack.FindIndex(frame => frame.Type == next);
                if (index >= 0)
                {
                    ReportCycle([.. onStack.Skip(index + 1).Select(frame => frame.Via!), relation], errors, reported);
                    continue;
                }

                Visit(next, relation);
            }

            onStack.RemoveAt(onStack.Count - 1);
            finished.Add(type);
        }
    }

    private static void ReportCycle(HoldRelation[] edges, List<string> errors, HashSet<string> reported)
    {
        var members = edges
            .Select(edge => edge.Declaring.EntityType.FullName!)
            .Distinct(StringComparer.Ordinal)
            .Order(StringComparer.Ordinal)
            .ToArray();
        if (!reported.Add(string.Join('|', members)))
        {
            return;
        }

        errors.Add(
            $"Hold relations among retained entities {string.Join(", ", members)} form a cycle (a relation from an entity to itself is one): {string.Join("; ", edges.Select(edge => edge.Declaration))}. Coverage must flow one way, so cycles aren't supported; remove one of these declarations."
        );
    }
}
