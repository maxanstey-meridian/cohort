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
    private static readonly NullabilityInfoContext NullabilityInfoContext = new();
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

    private static void ValidateAnonymiseConvention(
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

    private static List<string> GetAnonymiseConventionErrors(
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

    private static bool CanAssignNull(PropertyInfo property)
    {
        if (!property.PropertyType.IsValueType)
        {
            return NullabilityInfoContext.Create(property).ReadState == NullabilityState.Nullable;
        }

        return !IsNonNullableValueType(property.PropertyType);
    }
}
