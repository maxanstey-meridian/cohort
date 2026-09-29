using System.Collections.Concurrent;
using Cohort.Application;
using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.DependencyInjection;

namespace Cohort.Infrastructure;

internal sealed class RetentionRuntimeReadinessValidator(
    [FromKeyedServices(CohortServiceKeys.DbContext)] DbContext db,
    RetentionStartupValidator startupValidator,
    CohortSchemaValidator schemaValidator,
    RetentionRuntimeReadinessState state
)
{
    internal IReadOnlyDictionary<string, Cohort.Domain.RetentionCategoryCapabilities>
        ValidatedCapabilities => startupValidator.ValidatedCapabilities;

    public async Task ValidateAsync(CancellationToken ct = default)
    {
        ValidateProvider();
        var readiness = state.For(CreateKey());
        if (readiness.Validated)
        {
            return;
        }

        await readiness.Gate.WaitAsync(ct);
        try
        {
            if (readiness.Validated)
            {
                return;
            }

            await startupValidator.ValidateAsync(ct);
            await schemaValidator.ValidateAsync(ct);
            if (db.Database.CurrentTransaction is null)
            {
                readiness.Validated = true;
            }
        }
        finally
        {
            readiness.Gate.Release();
        }
    }

    // Readiness depends on the model (and so Cohort's table mapping) and on the database it
    // runs against; Npgsql's DataSource already carries host and port.
    private RetentionRuntimeReadinessKey CreateKey()
    {
        var connection = db.Database.GetDbConnection();
        return new RetentionRuntimeReadinessKey(db.Model, connection.DataSource, connection.Database);
    }

    private void ValidateProvider()
    {
        string? providerName;
        try
        {
            providerName = db.Database.ProviderName;
        }
        catch (InvalidOperationException)
        {
            providerName = null;
        }

        if (
            !string.Equals(
                providerName,
                "Npgsql.EntityFrameworkCore.PostgreSQL",
                StringComparison.Ordinal
            )
        )
        {
            throw new RetentionConfigurationException([
                $"Cohort requires the Npgsql Entity Framework Core provider; configured provider is '{providerName ?? "<unknown>"}'.",
            ]);
        }
    }
}

internal sealed class RetentionRuntimeReadinessState
{
    private readonly ConcurrentDictionary<
        RetentionRuntimeReadinessKey,
        RetentionRuntimeReadinessEntry
    > entries = new();

    internal RetentionRuntimeReadinessEntry For(RetentionRuntimeReadinessKey key) =>
        entries.GetOrAdd(key, static _ => new RetentionRuntimeReadinessEntry());
}

internal sealed record RetentionRuntimeReadinessKey(
    Microsoft.EntityFrameworkCore.Metadata.IModel Model,
    string DataSource,
    string Database
);

internal sealed class RetentionRuntimeReadinessEntry
{
    internal SemaphoreSlim Gate { get; } = new(1, 1);

    internal volatile bool Validated;
}
