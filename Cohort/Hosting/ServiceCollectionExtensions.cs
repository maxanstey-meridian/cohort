using System.Reflection;

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

        // Idempotency guard: most registrations below are TryAdd*, but the dispatcher's and
        // the history pruner's IHostedService registrations cannot be (factory descriptors
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
        services.TryAddEnumerable(
            ServiceDescriptor.Singleton<IValidateOptions<CohortOptions>, RowHandlerRetryScheduleValidator>()
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
        // The collection itself, read when Cohort validates: AddRowHandler and the host's
        // own handler registrations may come after AddCohort.
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
        services.TryAddSingleton<RetentionRowDispatcher>();
        services.TryAddSingleton<IRetentionRowDispatcher>(sp =>
            sp.GetRequiredService<RetentionRowDispatcher>()
        );
        services.AddSingleton<IHostedService>(sp =>
            sp.GetRequiredService<RetentionRowDispatcher>()
        );
        services.TryAddEnumerable(ServiceDescriptor.Singleton<IHostedService, RetentionWorker>());
        services.TryAddSingleton<RetentionHistoryPruner>();
        services.AddSingleton<IHostedService>(sp =>
            sp.GetRequiredService<RetentionHistoryPruner>()
        );

        return services;
    }

    /// <summary>
    /// Registers a row handler. <typeparamref name="THandler"/> must carry
    /// <see cref="RowHandlerIdAttribute"/>: queued handler work is persisted under that
    /// UUID, so it survives renaming or moving the class.
    /// </summary>
    public static IServiceCollection AddRowHandler<TEntity, THandler>(
        this IServiceCollection services,
        RowHandlerDispatchPhase dispatchPhase = RowHandlerDispatchPhase.Immediate
    )
        where THandler : class, IRetentionHandler<TEntity>
    {
        ArgumentNullException.ThrowIfNull(services);
        if (!Enum.IsDefined(dispatchPhase))
        {
            throw new ArgumentOutOfRangeException(
                nameof(dispatchPhase),
                dispatchPhase,
                "Row handler dispatch phase must be defined."
            );
        }

        var identity = ReadRowHandlerId(
            typeof(THandler),
            "Row handler",
            "Every row handler needs a permanent UUID: queued work is persisted under it."
        );

        var existingRegistrations = new RetentionHandlerServiceRegistrations(services).Registrations();

        // TryAddEnumerable would silently keep the first registration of this pair, so
        // a repeat call with a different phase must fail loudly instead of appearing to
        // take effect. An identical repeat stays idempotent.
        var samePair = existingRegistrations.FirstOrDefault(registration =>
            registration.EntityType == typeof(TEntity)
            && registration.HandlerType == typeof(THandler)
        );
        if (samePair is not null && samePair.DispatchPhase != dispatchPhase)
        {
            throw new InvalidOperationException(
                $"Row handler {typeof(THandler).FullName} for entity {typeof(TEntity).FullName} is already registered with dispatch phase {samePair.DispatchPhase}; this call's phase ({dispatchPhase}) would be silently ignored. Register each handler pair once."
            );
        }

        // The id names one handler class, whatever its entity: two classes sharing it would
        // have their work dispatched interchangeably, and a requeue by handler id would reach
        // both. One class registered for two entities keeps its single id.
        var identityClash = existingRegistrations.FirstOrDefault(registration =>
            registration.HandlerType != typeof(THandler)
            && registration.Identity == identity
        );
        if (identityClash is not null)
        {
            throw new InvalidOperationException(
                $"Row handler id {identity} is already used by {identityClash.HandlerType.FullName}; give {typeof(THandler).FullName} its own [RowHandlerId] so queued work, and a requeue by handler id, reach only the handler that persisted it."
            );
        }

        // One id space with dependents steps, whose failures are recorded under the same id.
        var stepClash = new RetentionHandlerServiceRegistrations(services).DependentsRegistrations()
            .FirstOrDefault(registration =>
                registration.Identity == identity
                && registration.StepType != typeof(THandler)
            );
        if (stepClash is not null)
        {
            throw new InvalidOperationException(
                $"Row handler id {identity} is already used by the dependents step {stepClash.StepType.FullName}; give {typeof(THandler).FullName} its own [RowHandlerId]. "
                    + "One class keeps its one id as a row handler and as a dependents step, for every entity."
            );
        }

        services.TryAddEnumerable(ServiceDescriptor.Scoped<IRetentionHandler<TEntity>, THandler>());
        services.TryAddEnumerable(
            ServiceDescriptor.Singleton(
                typeof(IRetentionHandlerRegistration),
                new RetentionHandlerRegistration<TEntity, THandler>(dispatchPhase, identity)
            )
        );

        return services;
    }

    /// <summary>
    /// Registers the dependents step of <typeparamref name="TEntity"/>: it retires what
    /// depends on a row in the row's own transaction, before Cohort mutates the row. One step
    /// per entity; a host with several kinds of dependents composes them in one step.
    /// <typeparamref name="TStep"/> must carry <see cref="RowHandlerIdAttribute"/>: a step's
    /// failure is recorded under that UUID, in the same id space as row handlers.
    /// </summary>
    public static IServiceCollection AddRetentionDependents<TEntity, TStep>(this IServiceCollection services)
        where TStep : class, IRetentionDependents<TEntity>
    {
        ArgumentNullException.ThrowIfNull(services);

        var identity = ReadRowHandlerId(
            typeof(TStep),
            "Dependents step",
            "Every dependents step needs a permanent UUID: its failures are recorded under it."
        );

        var existingSteps = new RetentionHandlerServiceRegistrations(services).DependentsRegistrations();
        var entityStep = existingSteps.FirstOrDefault(registration => registration.EntityType == typeof(TEntity));
        if (entityStep is not null && entityStep.StepType != typeof(TStep))
        {
            throw new InvalidOperationException(
                $"Entity {typeof(TEntity).FullName} already has the dependents step {entityStep.StepType.FullName}; {typeof(TStep).FullName} can't be a second. "
                    + "Each entity has one dependents step: compose the work into one class."
            );
        }

        var stepClash = existingSteps.FirstOrDefault(registration =>
            registration.StepType != typeof(TStep) && registration.Identity == identity
        );
        if (stepClash is not null)
        {
            throw new InvalidOperationException(
                $"Row handler id {identity} is already used by the dependents step {stepClash.StepType.FullName}; give {typeof(TStep).FullName} its own [RowHandlerId] so a failure recorded under it names only one class."
            );
        }

        var handlerClash = new RetentionHandlerServiceRegistrations(services).Registrations()
            .FirstOrDefault(registration =>
                registration.Identity == identity
                && registration.HandlerType != typeof(TStep)
            );
        if (handlerClash is not null)
        {
            throw new InvalidOperationException(
                $"Row handler id {identity} is already used by the row handler {handlerClash.HandlerType.FullName}; give {typeof(TStep).FullName} its own [RowHandlerId]. "
                    + "One class keeps its one id as a row handler and as a dependents step, for every entity."
            );
        }

        services.TryAddEnumerable(ServiceDescriptor.Scoped<IRetentionDependents<TEntity>, TStep>());
        services.TryAddEnumerable(
            ServiceDescriptor.Singleton(
                typeof(IRetentionDependentsRegistration),
                new RetentionDependentsRegistration<TEntity, TStep>(identity)
            )
        );

        return services;
    }

    private static Guid ReadRowHandlerId(Type type, string role, string why)
    {
        RowHandlerIdAttribute? idAttribute;
        try
        {
            idAttribute = type.GetCustomAttribute<RowHandlerIdAttribute>(inherit: false);
        }
        catch (Exception ex) when (ex is ArgumentException or TargetInvocationException or CustomAttributeFormatException)
        {
            throw new InvalidOperationException(
                $"{role} {type.FullName} has an invalid [RowHandlerId]: it must be a non-empty UUID.",
                ex
            );
        }

        return idAttribute?.Id
            ?? throw new InvalidOperationException(
                $"{role} {type.FullName} has no [RowHandlerId(\"<uuid>\")] attribute. {why}"
            );
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
