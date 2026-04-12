using System.Reflection;

using Cohort.Domain;

using Microsoft.EntityFrameworkCore;

namespace Cohort.Application;

public sealed class RetentionStartupValidator(
    DbContext db,
    IRetentionCategoryRepository categoryRepository
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

    private readonly RetentionEntryBuilder entryBuilder = new();

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

            if (retain is null && exempt is null)
            {
                errors.Add(
                    $"Entity {clrType.FullName} must declare exactly one of [Retain] or [ExemptFromRetention]."
                );
                continue;
            }

            if (retain is not null && exempt is not null)
            {
                errors.Add(
                    $"Entity {clrType.FullName} must declare exactly one of [Retain] or [ExemptFromRetention]."
                );
                continue;
            }

            if (exempt is not null)
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
                var possibleStrategies =
                    startupRule is null ? resolver.GetPossibleStrategiesAtStartup() : null;

                if (
                    startupRule?.Strategy == Strategy.SoftDelete
                    || possibleStrategies?.Contains(Strategy.SoftDelete) == true
                )
                {
                    ValidateSoftDeleteConvention(entry, errors, $"Soft-delete convention on {clrType.FullName}:");
                }

                if (
                    startupRule?.Strategy == Strategy.Anonymise
                    || possibleStrategies?.Contains(Strategy.Anonymise) == true
                )
                {
                    ValidateAnonymiseConvention(entry, errors, $"Anonymise convention on {clrType.FullName}:");
                }
                else if (startupRule is null && possibleStrategies is null)
                {
                    ValidateOpaqueDeferredResolverCompatibility(entry, errors);
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

    private static void ValidateSoftDeleteConvention(
        RetentionEntry entry,
        List<string> errors,
        string messagePrefix
    )
    {
        errors.AddRange(GetSoftDeleteConventionErrors(entry, messagePrefix));
    }

    private static void ValidateOpaqueDeferredResolverCompatibility(
        RetentionEntry entry,
        List<string> errors
    )
    {
        var clrType = entry.EntityType;
        var messagePrefix =
            $"Retention category '{entry.Category}' for entity {clrType.FullName} uses a deferred resolver that does not declare its possible strategies at startup.";
        var softDeleteErrors = GetSoftDeleteConventionErrors(entry, messagePrefix);
        var anonymiseErrors = GetAnonymiseConventionErrors(entry, messagePrefix);
        var hasSoftDeleteSignals =
            clrType.GetProperty("IsDeleted", BindingFlags.Public | BindingFlags.Instance) is not null
            || clrType.GetProperty("DeletedAt", BindingFlags.Public | BindingFlags.Instance) is not null
            || entry.SoftDelete is not null;
        var hasAnonymiseSignals = entry.AnonymiseFields.Count > 0;

        if (!hasSoftDeleteSignals && !hasAnonymiseSignals)
        {
            errors.Add(
                $"{messagePrefix} Opaque deferred resolvers must either advertise their strategies or target entities with a valid soft-delete or anonymise convention."
            );
            return;
        }

        if (softDeleteErrors.Count == 0 && anonymiseErrors.Count == 0)
        {
            return;
        }

        errors.Add(
            $"{messagePrefix} Opaque deferred resolvers must advertise their possible strategies at startup unless the target entity is valid for both soft-delete and anonymise conventions."
        );
    }

    private static void ValidateAnonymiseConvention(
        RetentionEntry entry,
        List<string> errors,
        string messagePrefix
    )
    {
        errors.AddRange(GetAnonymiseConventionErrors(entry, messagePrefix));
    }

    private static void ValidateSoftDeleteTenantConvention(
        RetentionEntry entry,
        List<string> errors,
        string messagePrefix
    )
    {
        if (entry.Tenant is null)
        {
            errors.Add(
                $"{messagePrefix} retained SoftDelete categories require tenant metadata via a public Guid or nullable Guid TenantId property mapped by EF."
            );
        }
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
        var clrType = entry.EntityType;

        ValidateSoftDeleteTenantConvention(entry, errors, messagePrefix);

        var isDeletedMember = clrType.GetProperty("IsDeleted", BindingFlags.Public | BindingFlags.Instance);
        if (isDeletedMember is null || isDeletedMember.PropertyType != typeof(bool))
        {
            errors.Add(
                $"{messagePrefix} retained SoftDelete categories require a public bool IsDeleted CLR property."
            );
            return errors;
        }

        if (entry.SoftDelete is null)
        {
            errors.Add(
                $"{messagePrefix} retained SoftDelete categories require IsDeleted to be mapped by EF."
            );
            return errors;
        }

        var deletedAtMember = clrType.GetProperty("DeletedAt", BindingFlags.Public | BindingFlags.Instance);
        if (
            deletedAtMember is not null
            && !AllowedSoftDeleteTimestampTypes.Contains(deletedAtMember.PropertyType)
        )
        {
            errors.Add(
                $"{messagePrefix} DeletedAt must be DateTime or DateTimeOffset (nullable allowed), got {deletedAtMember.PropertyType.Name}."
            );
        }

        return errors;
    }

    private static List<string> GetAnonymiseConventionErrors(
        RetentionEntry entry,
        string messagePrefix
    )
    {
        var errors = new List<string>();

        if (entry.Tenant is null)
        {
            errors.Add(
                $"{messagePrefix} retained Anonymise categories require tenant metadata via a public Guid or nullable Guid TenantId property mapped by EF."
            );
        }

        if (entry.AnonymiseFields.Count == 0)
        {
            errors.Add(
                $"{messagePrefix} retained Anonymise categories require at least one [Anonymise]-annotated property mapped by EF."
            );
            return errors;
        }

        foreach (var field in entry.AnonymiseFields)
        {
            var property = entry.EntityType.GetProperty(field.MemberName, BindingFlags.Public | BindingFlags.Instance);
            if (property is null)
            {
                errors.Add(
                    $"{messagePrefix} could not find public CLR property '{field.MemberName}' for anonymise metadata."
                );
                continue;
            }

            switch (field.Method)
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
