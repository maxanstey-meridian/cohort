using System.Reflection;

using Cohort.Domain;

using Microsoft.EntityFrameworkCore;

namespace Cohort.Application;

public sealed class RetentionStartupValidator(
    DbContext db,
    IRetentionCategoryRepository categoryRepository
)
{
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
                    ValidateSoftDeleteConvention(entry, errors);
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

    private static void ValidateSoftDeleteConvention(RetentionEntry entry, List<string> errors)
    {
        var clrType = entry.EntityType;
        var isDeletedMember = clrType.GetProperty("IsDeleted", BindingFlags.Public | BindingFlags.Instance);
        if (isDeletedMember is null || isDeletedMember.PropertyType != typeof(bool))
        {
            errors.Add(
                $"Soft-delete convention on {clrType.FullName}: retained SoftDelete categories require a public bool IsDeleted CLR property."
            );
            return;
        }

        if (entry.SoftDelete is null)
        {
            errors.Add(
                $"Soft-delete convention on {clrType.FullName}: retained SoftDelete categories require IsDeleted to be mapped by EF."
            );
            return;
        }

        var deletedAtMember = clrType.GetProperty("DeletedAt", BindingFlags.Public | BindingFlags.Instance);
        if (
            deletedAtMember is not null
            && !AllowedSoftDeleteTimestampTypes.Contains(deletedAtMember.PropertyType)
        )
        {
            errors.Add(
                $"Soft-delete convention on {clrType.FullName}: DeletedAt must be DateTime or DateTimeOffset (nullable allowed), got {deletedAtMember.PropertyType.Name}."
            );
        }
    }

    private static void ValidateOpaqueDeferredResolverCompatibility(
        RetentionEntry entry,
        List<string> errors
    )
    {
        var clrType = entry.EntityType;
        var isDeletedMember = clrType.GetProperty("IsDeleted", BindingFlags.Public | BindingFlags.Instance);
        var deletedAtMember = clrType.GetProperty("DeletedAt", BindingFlags.Public | BindingFlags.Instance);

        if (isDeletedMember is null && deletedAtMember is null)
        {
            errors.Add(
                $"Retention category '{entry.Category}' for entity {clrType.FullName} uses a deferred resolver that does not declare its possible strategies at startup. Opaque deferred resolvers must either advertise their strategies or target entities with a valid soft-delete convention."
            );
            return;
        }

        if (isDeletedMember is null || isDeletedMember.PropertyType != typeof(bool))
        {
            errors.Add(
                $"Retention category '{entry.Category}' for entity {clrType.FullName} uses a deferred resolver that does not declare its possible strategies at startup. Opaque deferred resolvers must either advertise their strategies or target entities with a public bool IsDeleted CLR property."
            );
            return;
        }

        if (entry.SoftDelete is null)
        {
            errors.Add(
                $"Retention category '{entry.Category}' for entity {clrType.FullName} uses a deferred resolver that does not declare its possible strategies at startup. Opaque deferred resolvers must either advertise their strategies or target entities whose IsDeleted convention is mapped by EF."
            );
            return;
        }

        if (
            deletedAtMember is not null
            && !AllowedSoftDeleteTimestampTypes.Contains(deletedAtMember.PropertyType)
        )
        {
            errors.Add(
                $"Retention category '{entry.Category}' for entity {clrType.FullName} uses a deferred resolver that does not declare its possible strategies at startup. Opaque deferred resolvers must either advertise their strategies or target entities whose DeletedAt property is DateTime or DateTimeOffset (nullable allowed), got {deletedAtMember.PropertyType.Name}."
            );
        }
    }
}
