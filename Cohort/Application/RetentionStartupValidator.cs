using System.Reflection;

using Cohort.Domain;

using Microsoft.EntityFrameworkCore;

namespace Cohort.Application;

public sealed class RetentionStartupValidator(
    DbContext db,
    IRetentionCategoryRepository categoryRepository
)
{
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

            _ = resolver.TryResolveAtStartup();
        }

        if (errors.Count > 0)
        {
            throw new RetentionConfigurationException(errors);
        }
    }
}
