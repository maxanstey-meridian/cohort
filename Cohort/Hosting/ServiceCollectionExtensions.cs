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

        services.TryAddSingleton(sp =>
            new RetentionEntryBuilder(sp.GetRequiredService<IOptions<CohortOptions>>().Value.Conventions));

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
        services.TryAddSingleton<IRetentionRowDispatcher>(sp =>
            (IRetentionRowDispatcher)sp.GetServices<IHostedService>().Single(service =>
                service is NoOpRetentionRowDispatcher
            )
        );
        services.TryAddEnumerable(
            ServiceDescriptor.Singleton<IHostedService, NoOpRetentionRowDispatcher>()
        );
        services.TryAddEnumerable(ServiceDescriptor.Singleton<IHostedService, RetentionWorker>());

        return services;
    }

    public static IServiceCollection AddRowHandler<TEntity, THandler>(this IServiceCollection services)
        where THandler : class, IRetentionHandler<TEntity>
    {
        ArgumentNullException.ThrowIfNull(services);

        services.TryAddEnumerable(ServiceDescriptor.Scoped<IRetentionHandler<TEntity>, THandler>());

        return services;
    }

    private sealed class MissingRetentionCategoryRepository : IRetentionCategoryRepository
    {
        public Task<IRetentionRuleResolver?> GetAsync(string category, CancellationToken ct)
        {
            throw new InvalidOperationException(
                $"No IRetentionCategoryRepository has been registered. "
                + $"Register one before calling AddCohort<TContext>() via "
                + $"services.AddSingleton<IRetentionCategoryRepository, YourRepository>(). "
                + $"(Attempted to resolve category '{category}'.)"
            );
        }
    }

    private sealed class NoOpRetentionRowDispatcher : BackgroundService, IRetentionRowDispatcher
    {
        public Task FlushAsync(CancellationToken ct = default)
        {
            return Task.CompletedTask;
        }

        protected override Task ExecuteAsync(CancellationToken stoppingToken)
        {
            return Task.Delay(Timeout.InfiniteTimeSpan, stoppingToken);
        }
    }
}
