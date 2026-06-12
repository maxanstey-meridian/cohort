using System.Reflection;

using Cohort.Domain;

using Microsoft.EntityFrameworkCore;

namespace Cohort.Application;

public sealed class RetentionStartupValidator(
    DbContext db,
    IRetentionCategoryRepository categoryRepository,
    RetentionEntryBuilder entryBuilder,
    IEnumerable<IAnonymiseValueFactory>? anonymiseValueFactories = null
)
{
    // Instance field, not static: NullabilityInfoContext is documented as not thread-safe,
    // and concurrent validations (parallel previews/sweeps/erasures) share statics.
    private readonly NullabilityInfoContext nullabilityInfoContext = new();
    private static readonly Type[] AllowedSoftDeleteTimestampTypes =
    [
        typeof(DateTime),
        typeof(DateTime?),
        typeof(DateTimeOffset),
        typeof(DateTimeOffset?),
    ];
    private readonly HashSet<Type> registeredAnonymiseFactoryTypes = new(
        (anonymiseValueFactories ?? Array.Empty<IAnonymiseValueFactory>()).Select(factory =>
            factory.GetType()
        )
    );

    public async Task ValidateAsync(CancellationToken ct = default)
    {
        var errors = new List<string>();

        foreach (var entityType in db.Model.GetEntityTypes())
        {
            if (entityType.ClrType == typeof(Dictionary<string, object>))
            {
                continue;
            }

            var clrType = entityType.ClrType;
            var retain = clrType.GetCustomAttribute<RetainAttribute>(inherit: false);
            var exempt = clrType.GetCustomAttribute<ExemptFromRetentionAttribute>(inherit: false);

            if (retain is not null && exempt is not null)
            {
                errors.Add(
                    $"Entity {clrType.FullName} must declare exactly one of [Retain] or [ExemptFromRetention], not both."
                );
                continue;
            }

            if (retain is null)
            {
                continue;
            }

            if (entityType.BaseType is not null || entityType.GetDirectlyDerivedTypes().Any())
            {
                errors.Add(
                    $"[Retain] on {clrType.FullName}: entity participates in an EF inheritance hierarchy (TPH/TPT/TPC). Sweep SQL targets the mapped table without a type discriminator, so rows of sibling or derived types would be swept too. Retention on inheritance-mapped entities is not supported."
                );
                continue;
            }

            var schema = entityType.GetSchema() ?? db.Model.GetDefaultSchema();
            if (schema is not null && !string.Equals(schema, "public", StringComparison.Ordinal))
            {
                errors.Add(
                    $"[Retain] on {clrType.FullName}: entity is mapped to schema '{schema}'. Cohort SQL does not schema-qualify identifiers and resolves tables via the connection search_path, so entities outside the default 'public' schema are not supported."
                );
                continue;
            }

            ValidateMarkerAttributeUniqueness(clrType, errors);

            RetentionEntry entry;
            try
            {
                entry =
                    entryBuilder.TryBuild(entityType)
                    ?? throw new InvalidOperationException(
                        $"[Retain] on {clrType.FullName}: retention entry could not be built."
                    );
            }
            catch (InvalidOperationException ex)
            {
                errors.Add(ex.Message);
                continue;
            }

            ValidateTenantConvention(entry, errors);
            ValidateTimestampStoreTypes(entityType, entry, errors);

            var resolver = await categoryRepository.GetAsync(entry.Category, ct);
            if (resolver is null)
            {
                errors.Add(
                    $"Retention category '{entry.Category}' for entity {clrType.FullName} could not be resolved."
                );
                continue;
            }

            try
            {
                var startupRule = resolver.TryResolveAtStartup();
                if (startupRule is null || startupRule.Strategy == Strategy.Purge)
                {
                    ValidateCascadeDeletePaths(entityType, errors);
                }

                if (entry.AnonymiseFields.Any(field => field is AnonymiseFactoryField))
                {
                    ValidateFactoryBackedAnonymiseSupport(
                        entry,
                        errors,
                        $"Anonymise convention on {clrType.FullName}:"
                    );
                }

                if (startupRule?.Strategy == Strategy.SoftDelete)
                {
                    ValidateSoftDeleteConvention(entry, errors, $"Soft-delete convention on {clrType.FullName}:");
                }

                if (startupRule?.Strategy == Strategy.Anonymise)
                {
                    ValidateAnonymiseConvention(entry, errors, $"Anonymise convention on {clrType.FullName}:");

                    if (entry.AnonymisedAt is null)
                    {
                        errors.Add(
                            $"Anonymise convention on {clrType.FullName}: retained Anonymise categories require a nullable DateTimeOffset marker property (named AnonymisedAt by convention, or marked with [RetentionAnonymisedAt]). NULL marks rows not yet anonymised; without it anonymisation re-scrubs every expired row on every sweep."
                        );
                    }
                }
            }
            catch (Exception ex)
            {
                errors.Add(
                    $"Retention category '{entry.Category}' for entity {clrType.FullName} failed startup validation: {ex.Message}"
                );
            }
        }

        if (errors.Count > 0)
        {
            throw new RetentionConfigurationException(errors);
        }
    }

    private void ValidateTenantConvention(RetentionEntry entry, List<string> errors)
    {
        // Tenantedness is decided by the resolved tenant convention everywhere (scope
        // filtering, SQL tenant clauses, the worker's pass split) — an entity that
        // declares [RetentionTenantless] while also exposing a tenant property would be
        // swept per tenant with the attribute silently ignored.
        if (entry.Tenant is not null && entry.IsExplicitlyTenantless)
        {
            errors.Add(
                $"Tenant convention on {entry.EntityType.FullName}: entity is marked [RetentionTenantless] but exposes tenant property '{entry.Tenant.TenantMember}'. The tenant property wins and the entity would be swept per tenant, so the marker is contradictory; remove [RetentionTenantless] or the tenant property."
            );
            return;
        }

        if (entry.Tenant is not null || entry.IsExplicitlyTenantless)
        {
            return;
        }

        errors.Add(
            $"Tenant convention on {entry.EntityType.FullName}: retained entities must expose a public Guid or nullable Guid tenant property named '{entryBuilder.ExpectedTenantPropertyName}' by convention, or mark the tenant property with [RetentionTenant], unless the entity is explicitly marked with [RetentionTenantless]."
        );
    }

    private void ValidateFactoryBackedAnonymiseSupport(
        RetentionEntry entry,
        List<string> errors,
        string messagePrefix
    )
    {
        foreach (var field in entry.AnonymiseFields.OfType<AnonymiseFactoryField>())
        {
            if (!typeof(IAnonymiseValueFactory).IsAssignableFrom(field.FactoryType))
            {
                errors.Add(
                    $"{messagePrefix} [AnonymiseWith] member {field.MemberName} specifies factory type {field.FactoryType.FullName} which does not implement {nameof(IAnonymiseValueFactory)}."
                );
                continue;
            }

            if (!registeredAnonymiseFactoryTypes.Contains(field.FactoryType))
            {
                errors.Add(
                    $"{messagePrefix} [AnonymiseWith] member {field.MemberName} specifies factory type {field.FactoryType.FullName} but no matching {nameof(IAnonymiseValueFactory)} is registered in DI."
                );
            }
        }
    }

    private static void ValidateSoftDeleteConvention(
        RetentionEntry entry,
        List<string> errors,
        string messagePrefix
    )
    {
        errors.AddRange(GetSoftDeleteConventionErrors(entry, messagePrefix));
    }

    private void ValidateAnonymiseConvention(
        RetentionEntry entry,
        List<string> errors,
        string messagePrefix
    )
    {
        errors.AddRange(GetAnonymiseConventionErrors(entry, messagePrefix));
    }

    private static bool IsNonNullableValueType(Type type)
    {
        return type.IsValueType && Nullable.GetUnderlyingType(type) is null;
    }

    private static List<string> GetSoftDeleteConventionErrors(
        RetentionEntry entry,
        string messagePrefix
    )
    {
        var errors = new List<string>();

        if (entry.SoftDelete is null)
        {
            errors.Add(
                $"{messagePrefix} retained SoftDelete categories require a public bool soft-delete flag property (named IsDeleted by convention, or marked with [RetentionSoftDelete]) mapped by EF."
            );
            return errors;
        }

        var clrType = entry.EntityType;
        var isDeletedMember = ReflectionMemberResolver.FindPropertyByName(clrType, entry.SoftDelete.IsDeletedMember);
        if (isDeletedMember is null || isDeletedMember.PropertyType != typeof(bool))
        {
            errors.Add(
                $"{messagePrefix} soft-delete flag '{entry.SoftDelete.IsDeletedMember}' must be a public bool CLR property."
            );
            return errors;
        }

        if (entry.SoftDelete.DeletedAtMember is not null)
        {
            var deletedAtMember = ReflectionMemberResolver.FindPropertyByName(clrType, entry.SoftDelete.DeletedAtMember);
            if (
                deletedAtMember is not null
                && !AllowedSoftDeleteTimestampTypes.Contains(deletedAtMember.PropertyType)
            )
            {
                errors.Add(
                    $"{messagePrefix} '{entry.SoftDelete.DeletedAtMember}' must be DateTime or DateTimeOffset (nullable allowed), got {deletedAtMember.PropertyType.Name}."
                );
            }
        }

        return errors;
    }

    private List<string> GetAnonymiseConventionErrors(
        RetentionEntry entry,
        string messagePrefix
    )
    {
        var errors = new List<string>();

        if (entry.AnonymiseFields.Count == 0)
        {
            errors.Add(
                $"{messagePrefix} retained Anonymise categories require at least one [Anonymise]-annotated property mapped by EF."
            );
            return errors;
        }

        foreach (var field in entry.AnonymiseFields)
        {
            var property = ReflectionMemberResolver.FindPropertyByName(entry.EntityType, field.MemberName);
            if (property is null)
            {
                errors.Add(
                    $"{messagePrefix} could not find public CLR property '{field.MemberName}' for anonymise metadata."
                );
                continue;
            }

            if (field is not AnonymiseLiteralField literalField)
            {
                continue;
            }

            switch (literalField.Method)
            {
                case AnonymiseMethod.Null when !CanAssignNull(property):
                    errors.Add(
                        $"{messagePrefix} [Anonymise] member {property.Name} uses Null but {property.PropertyType.Name} is not nullable."
                    );
                    break;
                case AnonymiseMethod.EmptyString when property.PropertyType != typeof(string):
                    errors.Add(
                        $"{messagePrefix} [Anonymise] member {property.Name} uses EmptyString but {property.PropertyType.Name} is not string."
                    );
                    break;
                case AnonymiseMethod.FixedLiteral when property.PropertyType != typeof(string):
                    errors.Add(
                        $"{messagePrefix} [Anonymise] member {property.Name} uses FixedLiteral but {property.PropertyType.Name} is not string."
                    );
                    break;
            }
        }

        return errors;
    }

    private static void ValidateTimestampStoreTypes(
        Microsoft.EntityFrameworkCore.Metadata.IEntityType entityType,
        RetentionEntry entry,
        List<string> errors
    )
    {
        ValidateTimestampStoreType(entityType, entry, entry.AnchorMember, "anchor", errors);
        ValidateTimestampStoreType(
            entityType,
            entry,
            entry.SoftDelete?.DeletedAtMember,
            "soft-delete DeletedAt",
            errors
        );
        ValidateTimestampStoreType(
            entityType,
            entry,
            entry.AnonymisedAt?.AnonymisedAtMember,
            "AnonymisedAt marker",
            errors
        );
    }

    private static void ValidateTimestampStoreType(
        Microsoft.EntityFrameworkCore.Metadata.IEntityType entityType,
        RetentionEntry entry,
        string? memberName,
        string role,
        List<string> errors
    )
    {
        if (memberName is null)
        {
            return;
        }

        var property = entityType.FindProperty(memberName);
        if (property is null)
        {
            return;
        }

        var clrType = Nullable.GetUnderlyingType(property.ClrType) ?? property.ClrType;
        if (clrType != typeof(DateTime) && clrType != typeof(DateTimeOffset))
        {
            return;
        }

        string storeType;
        try
        {
            storeType = property.GetColumnType();
        }
        catch (Exception ex) when (ex is InvalidOperationException or InvalidCastException)
        {
            // Non-relational providers (e.g. InMemory in unit tests) expose no store type.
            return;
        }

        if (!string.Equals(storeType, "timestamp with time zone", StringComparison.OrdinalIgnoreCase))
        {
            errors.Add(
                $"Timestamp convention on {entry.EntityType.FullName}: {role} property '{memberName}' is mapped to '{storeType}'. Cohort compares and writes retention timestamps as UTC instants, which requires 'timestamp with time zone'; 'timestamp without time zone' silently drifts with the session TimeZone and rejects UTC-kinded parameters. Map the property with HasColumnType(\"timestamptz\") or use DateTimeOffset."
            );
        }
    }

    private static void ValidateCascadeDeletePaths(
        Microsoft.EntityFrameworkCore.Metadata.IEntityType entityType,
        List<string> errors
    )
    {
        var visited = new HashSet<Microsoft.EntityFrameworkCore.Metadata.IEntityType> { entityType };
        var queue = new Queue<Microsoft.EntityFrameworkCore.Metadata.IEntityType>();
        queue.Enqueue(entityType);

        while (queue.Count > 0)
        {
            var current = queue.Dequeue();
            foreach (var foreignKey in current.GetReferencingForeignKeys())
            {
                if (foreignKey.DeleteBehavior != DeleteBehavior.Cascade)
                {
                    continue;
                }

                var dependent = foreignKey.DeclaringEntityType;
                if (!visited.Add(dependent))
                {
                    continue;
                }

                if (dependent.ClrType.GetCustomAttribute<RetainAttribute>(inherit: false) is not null)
                {
                    errors.Add(
                        $"[Retain] on {entityType.ClrType.FullName}: purging this entity cascades (ON DELETE CASCADE) into retained entity {dependent.ClrType.FullName}, bypassing that entity's retention window, legal holds, and audit trail. Configure the relationship with DeleteBehavior.Restrict or NoAction so dependents are retired by their own retention rules."
                    );
                }

                queue.Enqueue(dependent);
            }
        }
    }

    private static void ValidateMarkerAttributeUniqueness(Type clrType, List<string> errors)
    {
        AddDuplicateMarkerError<RetentionRecordIdAttribute>(clrType, errors);
        AddDuplicateMarkerError<RetentionTenantAttribute>(clrType, errors);
        AddDuplicateMarkerError<RetentionSoftDeleteAttribute>(clrType, errors);
        AddDuplicateMarkerError<RetentionDeletedAtAttribute>(clrType, errors);
        AddDuplicateMarkerError<RetentionAnonymisedAtAttribute>(clrType, errors);
    }

    private static void AddDuplicateMarkerError<TAttribute>(Type clrType, List<string> errors)
        where TAttribute : Attribute
    {
        var markedPropertyNames = clrType
            .GetProperties(BindingFlags.Public | BindingFlags.Instance)
            .Where(property => property.GetCustomAttribute<TAttribute>() is not null)
            .Select(property => property.Name)
            .Distinct(StringComparer.Ordinal)
            .Order(StringComparer.Ordinal)
            .ToArray();

        if (markedPropertyNames.Length > 1)
        {
            var markerName = typeof(TAttribute).Name.Replace("Attribute", "", StringComparison.Ordinal);
            errors.Add(
                $"Marker convention on {clrType.FullName}: [{markerName}] is declared on multiple properties ({string.Join(", ", markedPropertyNames)}); exactly one is allowed."
            );
        }
    }

    private bool CanAssignNull(PropertyInfo property)
    {
        if (!property.PropertyType.IsValueType)
        {
            return nullabilityInfoContext.Create(property).ReadState == NullabilityState.Nullable;
        }

        return !IsNonNullableValueType(property.PropertyType);
    }
}
