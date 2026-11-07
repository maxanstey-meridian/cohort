using Cohort.Application;
using Cohort.Domain;
using Cohort.Infrastructure;
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

        // Idempotency guard: most registrations below are TryAdd*, but the recovery service's
        // and the history pruner's IHostedService registrations cannot be (factory descriptors
        // do not dedupe), and a second registration would start two loops on one instance.
        var existingRegistration = services
            .Where(descriptor => descriptor.ServiceType == typeof(CohortRegistrationMarker))
            .Select(descriptor => descriptor.ImplementationInstance)
            .OfType<CohortRegistrationMarker>()
            .SingleOrDefault();
        if (existingRegistration is not null)
        {
            if (existingRegistration.ContextType != typeof(TContext))
            {
                throw new InvalidOperationException(
                    $"Cohort is already registered for DbContext {existingRegistration.ContextType.FullName}; it cannot be registered for a different DbContext ({typeof(TContext).FullName})."
                );
            }

            return services;
        }

        services.AddSingleton(new CohortRegistrationMarker(typeof(TContext)));

        services
            .AddOptions<CohortOptions>()
            .BindConfiguration(CohortOptions.SectionName)
            .ValidateOnStart();
        services.TryAddEnumerable(
            ServiceDescriptor.Singleton<IValidateOptions<CohortOptions>, CohortOptionsValidator>()
        );

        services.TryAddSingleton(sp =>
            new RetentionEntryBuilder(sp.GetRequiredService<IOptions<CohortOptions>>().Value.Conventions)
        );
        services.TryAddSingleton<CohortOptionsSnapshot>();
        services.TryAddSingleton<IRetentionExecutionSettings>(sp =>
            sp.GetRequiredService<CohortOptionsSnapshot>()
        );

        services.AddKeyedScoped<DbContext>(CohortServiceKeys.DbContext, (sp, _) =>
            sp.GetRequiredService<TContext>()
        );
        services.TryAddSingleton<IRetentionRuleProvider, MissingRetentionRuleProvider>();
        services.TryAddSingleton<IRetentionExecutionScope, MissingRetentionExecutionScope>();
        services.AddScoped<EfRetentionAuditWriter>();
        services.AddScoped<RetentionAuditNotifier>();
        services.TryAddScoped<RetentionTargetResolver>();
        services.TryAddScoped<IRetentionHoldsRepository, EfRetentionHoldsRepository>();
        services.TryAddScoped<RetentionHoldGuard>();
        services.TryAddScoped<IRetentionDeletion, EfRetentionDeletion>();
        services.TryAddEnumerable(
            ServiceDescriptor.Scoped<SweepStrategy, PurgeSweepStrategy>()
        );
        services.TryAddEnumerable(
            ServiceDescriptor.Scoped<SweepStrategy, SoftDeleteSweepStrategy>()
        );
        services.TryAddEnumerable(
            ServiceDescriptor.Scoped<SweepStrategy, AnonymiseSweepStrategy>()
        );
        services.TryAddScoped<RetentionPreviewService>();
        services.TryAddSingleton<IRetentionPreview, ScopeOwnedRetentionPreview>();
        services.TryAddScoped<RetentionErasureService>();
        // Scoped: a Caller-transaction erasure runs on the calling scope's DbContext.
        services.TryAddScoped<IRetentionErasureService, RetentionErasureEntryPoint>();
        // EF has no other commit hook: Caller-transaction erasures defer their observers to
        // the caller's commit through this interceptor. Singleton options configuration, so
        // it also reaches pooled contexts and factories, and it is appended to (never
        // replaces) the host's own interceptors.
        services.TryAddSingleton<CohortCommitInterceptor>();
        services.ConfigureDbContext<TContext>(
            (sp, options) => options.AddInterceptors(sp.GetRequiredService<CohortCommitInterceptor>()),
            ServiceLifetime.Singleton
        );
        services.TryAddScoped<RetentionRegistry>();
        services.TryAddScoped<ErasureSubjectMetadataResolver>();
        services.TryAddSingleton<RetentionValidationState>();
        // The collection itself, read when Cohort validates: AddRetentionDependents and the
        // host's own step registrations may come after AddCohort.
        services.AddSingleton(new RetentionHandlerServiceRegistrations(services));
        services.TryAddScoped<RetentionStartupValidator>();
        services.TryAddScoped<CohortSchemaValidator>();
        services.TryAddSingleton<RetentionRuntimeReadinessState>();
        services.TryAddScoped<AnonymiseAssignmentResolver>();
        services.TryAddScoped<RetentionRuntimeReadinessValidator>();
        services.TryAddScoped<RetentionSweepEngine>();
        services.TryAddSingleton<IRetentionSweep, ScopeOwnedRetentionSweep>();
        services.TryAddScoped<IRetentionTenantSource, SingleTenantContextSource>();
        services.TryAddEnumerable(
            ServiceDescriptor.Singleton<IHostedService, RetentionValidationHostedService>()
        );
        services.TryAddSingleton<RetentionRunRecovery>();
        services.TryAddSingleton<IRetentionRunRecovery>(sp =>
            sp.GetRequiredService<RetentionRunRecovery>()
        );
        services.AddSingleton<IHostedService>(sp =>
            sp.GetRequiredService<RetentionRunRecovery>()
        );
        services.TryAddEnumerable(ServiceDescriptor.Singleton<IHostedService, RetentionWorker>());
        services.TryAddSingleton<RetentionHistoryPruner>();
        services.AddSingleton<IHostedService>(sp =>
            sp.GetRequiredService<RetentionHistoryPruner>()
        );

        return services;
    }

    /// <summary>
    /// Registers the dependents step of <typeparamref name="TEntity"/>: it retires what
    /// depends on a row in the row's own transaction, before Cohort mutates the row. One step
    /// per entity; a host with several kinds of dependents composes them in one step.
    /// </summary>
    /// <param name="services">The host's service collection.</param>
    /// <param name="retiresReferencingRows">
    /// Whether the step retires the rows that reference <typeparamref name="TEntity"/>. Pass
    /// <c>false</c> for a step that doesn't (one that only writes to an outbox, say): purge then
    /// leaves a still-referenced row out up front and counts it blocked, rather than running
    /// the step and failing at the database. With the default <c>true</c>, purge tries every
    /// due row, and a row still referenced after the step is blocked by the constraint itself.
    /// A repeat call for the same entity and step must pass the same value.
    /// </param>
    public static IServiceCollection AddRetentionDependents<TEntity, TStep>(
        this IServiceCollection services,
        bool retiresReferencingRows = true
    )
        where TStep : class, IRetentionDependents<TEntity>
    {
        ArgumentNullException.ThrowIfNull(services);

        var entityStep = new RetentionHandlerServiceRegistrations(services)
            .DependentsRegistrations()
            .FirstOrDefault(registration => registration.EntityType == typeof(TEntity));
        if (entityStep is not null && entityStep.StepType != typeof(TStep))
        {
            throw new InvalidOperationException(
                $"Entity {typeof(TEntity).FullName} already has the dependents step {entityStep.StepType.FullName}; {typeof(TStep).FullName} can't be a second. "
                    + "Each entity has one dependents step: compose the work into one class."
            );
        }

        // TryAddEnumerable would silently keep the first registration of this pair, so a
        // repeat call with a different flag must fail loudly instead of appearing to take
        // effect. An identical repeat stays idempotent.
        if (entityStep is not null && entityStep.RetiresReferencingRows != retiresReferencingRows)
        {
            throw new InvalidOperationException(
                $"Dependents step {typeof(TStep).FullName} for entity {typeof(TEntity).FullName} is already registered with retiresReferencingRows: {(entityStep.RetiresReferencingRows ? "true" : "false")}; this call's value ({(retiresReferencingRows ? "true" : "false")}) would be silently ignored. Register each step once."
            );
        }

        services.TryAddEnumerable(ServiceDescriptor.Scoped<IRetentionDependents<TEntity>, TStep>());
        services.TryAddEnumerable(
            ServiceDescriptor.Singleton(
                typeof(IRetentionDependentsRegistration),
                new RetentionDependentsRegistration<TEntity, TStep>(retiresReferencingRows)
            )
        );

        return services;
    }

    private sealed record CohortRegistrationMarker(Type ContextType);

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

    private sealed class MissingRetentionExecutionScope : IRetentionExecutionScope
    {
        public Task RunAsync(RetentionExecutionContext context, Func<Task> work, CancellationToken ct) =>
            throw new RetentionConfigurationException([
                "No IRetentionExecutionScope has been registered. Cohort runs each scope's work through it and says whose work it is, so a host's tenant isolation covers Cohort's own work. "
                    + "Register your host's hook with services.AddScoped<IRetentionExecutionScope, YourExecutionScope>(), "
                    + "or, for a host without tenant isolation, services.AddSingleton(RetentionExecutionScope.Unisolated).",
            ]);
    }

    private sealed class MissingRetentionRuleProvider : IRetentionRuleProvider
    {
        public RetentionCategoryCapabilities? GetCapabilities(string category)
        {
            throw new InvalidOperationException(
                $"No IRetentionRuleProvider has been registered. "
                    + $"Register one before calling AddCohort<TContext>() via "
                    + $"services.AddSingleton<IRetentionRuleProvider, YourProvider>(). "
                    + $"(Attempted to resolve category '{category}'.)"
            );
        }

        public Task<RetentionRule?> ResolveAsync(
            RetentionResolutionContext context,
            CancellationToken ct
        ) => throw new InvalidOperationException("No IRetentionRuleProvider has been registered.");
    }
}
