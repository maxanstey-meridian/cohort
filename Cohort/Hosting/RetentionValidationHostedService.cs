using Cohort.Application;
using Cohort.Infrastructure;
using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;

namespace Cohort.Hosting;

internal sealed class RetentionValidationHostedService(IServiceScopeFactory scopeFactory)
    : IHostedService
{
    public async Task StartAsync(CancellationToken cancellationToken)
    {
        await using var scope = scopeFactory.CreateAsyncScope();
        var db = scope.ServiceProvider.GetRequiredKeyedService<DbContext>(
            CohortServiceKeys.DbContext
        );
        if (
            !string.Equals(
                db.Database.ProviderName,
                "Npgsql.EntityFrameworkCore.PostgreSQL",
                StringComparison.Ordinal
            )
        )
        {
            throw new RetentionConfigurationException([
                $"Cohort requires the Npgsql Entity Framework Core provider; configured provider is '{db.Database.ProviderName ?? "<unknown>"}'.",
            ]);
        }

        await scope
            .ServiceProvider.GetRequiredService<RetentionStartupValidator>()
            .ValidateAsync(cancellationToken);
        await scope
            .ServiceProvider.GetRequiredService<CohortSchemaValidator>()
            .ValidateAsync(cancellationToken);
    }

    public Task StopAsync(CancellationToken cancellationToken) => Task.CompletedTask;
}
