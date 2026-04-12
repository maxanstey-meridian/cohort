using Cohort.Application;
using Cohort.Infrastructure.Audit;
using Cohort.Infrastructure.Holds;
using Cohort.Infrastructure.Sweep;

using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Options;

namespace Cohort.Hosting;

public static class ServiceCollectionExtensions
{
    public static IServiceCollection AddCohort<TContext>(this IServiceCollection services)
        where TContext : DbContext
    {
        ArgumentNullException.ThrowIfNull(services);

        services.AddOptions<CohortOptions>().BindConfiguration(CohortOptions.SectionName).ValidateOnStart();
        services.TryAddEnumerable(
            ServiceDescriptor.Singleton<IValidateOptions<CohortOptions>, CohortOptionsValidator>()
        );

        services.TryAddScoped<DbContext>(sp => sp.GetRequiredService<TContext>());
        services.TryAddSingleton<IRetentionCategoryRepository, MissingRetentionCategoryRepository>();
        services.TryAddScoped<IRetentionAuditWriter, EfRetentionAuditWriter>();
        services.TryAddScoped<IRetentionHoldsRepository, EfRetentionHoldsRepository>();
        services.TryAddEnumerable(
            ServiceDescriptor.Scoped<IRetentionSweepStrategy, PurgeSweepStrategy>()
        );
        services.TryAddEnumerable(
            ServiceDescriptor.Scoped<IRetentionSweepStrategy, SoftDeleteSweepStrategy>()
        );
        services.TryAddEnumerable(
            ServiceDescriptor.Scoped<IRetentionSweepStrategy, AnonymiseSweepStrategy>()
        );
        services.TryAddScoped<IRetentionPreview, RetentionPreviewService>();
        services.TryAddScoped<IRetentionErasureService, RetentionErasureService>();
        services.TryAddScoped<RetentionRegistry>();
        services.TryAddScoped<RetentionStartupValidator>();
        services.TryAddScoped<RetentionSweepEngine>();
        services.TryAddEnumerable(ServiceDescriptor.Singleton<IHostedService, RetentionWorker>());

        return services;
    }

    private sealed class MissingRetentionCategoryRepository : IRetentionCategoryRepository
    {
        public Task<IRetentionRuleResolver?> GetAsync(string category, CancellationToken ct)
        {
            return Task.FromResult<IRetentionRuleResolver?>(null);
        }
    }
}
