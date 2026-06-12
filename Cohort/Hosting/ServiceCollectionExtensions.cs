using Cohort.Application;
using Cohort.Domain;
using Cohort.Infrastructure.Audit;
using Cohort.Infrastructure.Handlers;
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

        // Idempotency guard: most registrations below are TryAdd*, but the dispatcher's
        // IHostedService registration cannot be (factory descriptors do not dedupe), and
        // a second registration would start two polling loops on the same dispatcher.
        if (services.Any(descriptor => descriptor.ServiceType == typeof(CohortRegistrationMarker)))
        {
            return services;
        }

        services.AddSingleton<CohortRegistrationMarker>();

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
        services.TryAddScoped<IRetentionSweep>(sp => sp.GetRequiredService<RetentionSweepEngine>());
        services.TryAddScoped<IRetentionTenantSource, SingleTenantContextSource>();
        services.TryAddSingleton<RetentionRowDispatcher>();
        services.TryAddSingleton<IRetentionRowDispatcher>(sp =>
            sp.GetRequiredService<RetentionRowDispatcher>()
        );
        services.AddSingleton<IHostedService>(sp => sp.GetRequiredService<RetentionRowDispatcher>());
        services.TryAddEnumerable(ServiceDescriptor.Singleton<IHostedService, RetentionWorker>());

        return services;
    }

    public static IServiceCollection AddRowHandler<TEntity, THandler>(
        this IServiceCollection services,
        RowHandlerDispatchPhase dispatchPhase = RowHandlerDispatchPhase.Immediate
    )
        where THandler : class, IRetentionHandler<TEntity>
    {
        ArgumentNullException.ThrowIfNull(services);

        services.TryAddEnumerable(ServiceDescriptor.Scoped<IRetentionHandler<TEntity>, THandler>());
        services.TryAddEnumerable(
            ServiceDescriptor.Singleton(
                typeof(IRetentionHandlerRegistration),
                new RetentionHandlerRegistration<TEntity, THandler>(dispatchPhase)
            )
        );

        return services;
    }

    private sealed class CohortRegistrationMarker;

    private sealed class SingleTenantContextSource(IServiceProvider services)
        : IRetentionTenantSource
    {
        public Task<IReadOnlyList<TenantContext>> GetTenantsAsync(CancellationToken ct)
        {
            var tenant = services.GetService<TenantContext>();
            if (tenant is null)
            {
                throw new InvalidOperationException(
                    "RetentionWorker requires a TenantContext registration when scheduling is enabled. Multi-tenant hosts should register their own IRetentionTenantSource enumerating every tenant to sweep."
                );
            }

            return Task.FromResult<IReadOnlyList<TenantContext>>([tenant]);
        }
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
}
