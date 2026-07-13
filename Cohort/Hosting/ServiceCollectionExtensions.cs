using Cohort.Application;
using Cohort.Domain;
using Cohort.Infrastructure;
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
        {
            var conventions = sp.GetRequiredService<IOptions<CohortOptions>>().Value.Conventions;
            return new RetentionEntryBuilder(
                new RetentionModelConventions
                {
                    RecordIdPropertyName = conventions.RecordIdPropertyName,
                    TenantPropertyName = conventions.TenantPropertyName,
                    SoftDeletePropertyName = conventions.SoftDeletePropertyName,
                    DeletedAtPropertyName = conventions.DeletedAtPropertyName,
                    AnonymisedAtPropertyName = conventions.AnonymisedAtPropertyName,
                }
            );
        });
        services.TryAddSingleton<IRetentionExecutionSettings, HostingRetentionExecutionSettings>();

        services.AddKeyedScoped<DbContext>(CohortServiceKeys.DbContext, (sp, _) =>
            sp.GetRequiredService<TContext>()
        );
        services.TryAddSingleton<IRetentionRuleProvider, MissingRetentionRuleProvider>();
        services.AddScoped<EfRetentionAuditWriter>();
        services.AddScoped<RetentionAuditNotifier>();
        services.TryAddScoped<RetentionTargetResolver>();
        services.TryAddScoped<IRetentionHoldsRepository, EfRetentionHoldsRepository>();
        services.TryAddScoped<IRetentionDeletion, EfRetentionDeletion>();
        services.TryAddEnumerable(
            ServiceDescriptor.Scoped<IRetentionSweepStrategy, PurgeSweepStrategy>()
        );
        services.TryAddEnumerable(
            ServiceDescriptor.Scoped<IRetentionSweepStrategy, SoftDeleteSweepStrategy>()
        );
        services.TryAddEnumerable(
            ServiceDescriptor.Scoped<IRetentionSweepStrategy, AnonymiseSweepStrategy>()
        );
        services.TryAddScoped<RetentionPreviewService>();
        services.TryAddSingleton<IRetentionPreview, ScopeOwnedRetentionPreview>();
        services.TryAddScoped<RetentionErasureService>();
        services.TryAddSingleton<IRetentionErasureService, ScopeOwnedRetentionErasureService>();
        services.TryAddScoped<RetentionRegistry>();
        services.TryAddScoped<ErasureSubjectMetadataResolver>();
        services.TryAddSingleton<RetentionValidationState>();
        services.TryAddScoped<RetentionStartupValidator>();
        services.TryAddScoped<CohortSchemaValidator>();
        services.TryAddSingleton<RetentionRuntimeReadinessState>();
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

        return services;
    }

    /// <summary>
    /// Registers a row handler. <paramref name="identity"/> is the stable identity that
    /// queued handler work is persisted under; without it, the handler's CLR type name
    /// is used, and renaming the class dead-letters any rows still queued under the old
    /// name. Give long-lived handlers an explicit UUID so persisted work survives
    /// refactors.
    /// </summary>
    public static IServiceCollection AddRowHandler<TEntity, THandler>(
        this IServiceCollection services,
        RowHandlerDispatchPhase dispatchPhase = RowHandlerDispatchPhase.Immediate,
        Guid? identity = null
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

        var existingRegistrations = services
            .Where(descriptor => descriptor.ServiceType == typeof(IRetentionHandlerRegistration))
            .Select(descriptor => descriptor.ImplementationInstance)
            .OfType<IRetentionHandlerRegistration>()
            .ToArray();

        // TryAddEnumerable would silently keep the first registration of this pair, so
        // a repeat call with a different phase or identity must fail loudly instead of
        // appearing to take effect. An identical repeat stays idempotent.
        var samePair = existingRegistrations.FirstOrDefault(registration =>
            registration.EntityType == typeof(TEntity)
            && registration.HandlerType == typeof(THandler)
        );
        if (
            samePair is not null
            && (samePair.DispatchPhase != dispatchPhase || samePair.Identity != identity)
        )
        {
            throw new InvalidOperationException(
                $"Row handler {typeof(THandler).FullName} for entity {typeof(TEntity).FullName} is already registered with dispatch phase {samePair.DispatchPhase} and identity {samePair.Identity?.ToString() ?? "<none>"}; this call's values ({dispatchPhase}, {identity?.ToString() ?? "<none>"}) would be silently ignored. Register each handler pair once."
            );
        }

        // Two handlers persisting queued work under the same identity would have their
        // rows dispatched interchangeably to whichever resolves first.
        var identityClash = identity is not null
            ? existingRegistrations.FirstOrDefault(registration =>
                registration.EntityType == typeof(TEntity)
                && registration.HandlerType != typeof(THandler)
                && registration.Identity == identity
            )
            : null;
        if (identityClash is not null)
        {
            throw new InvalidOperationException(
                $"Row handler identity {identity} for entity {typeof(TEntity).FullName} is already used by {identityClash.HandlerType.FullName}; identities must be unique per entity so queued work dispatches to the handler that persisted it."
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

    private sealed record CohortRegistrationMarker(Type ContextType);

    private sealed class HostingRetentionExecutionSettings : IRetentionExecutionSettings, IDisposable
    {
        private CohortOptions current;
        private readonly IDisposable? reloadSubscription;

        public HostingRetentionExecutionSettings(IOptionsMonitor<CohortOptions> options)
        {
            current = options.CurrentValue;
            reloadSubscription = options.OnChange(updated => Volatile.Write(ref current, updated));
        }

        public bool DryRun => Volatile.Read(ref current).DryRun;

        public int SweepBatchSize => Volatile.Read(ref current).SweepBatchSize;

        public TimeSpan AuditObserverTimeout => Volatile.Read(ref current).AuditObservers.Timeout;

        public RetentionRowHandlerSettings RowHandlerDispatch
        {
            get
            {
                var value = Volatile.Read(ref current).RowHandlerDispatch;
                return new RetentionRowHandlerSettings(
                    value.PollInterval,
                    value.PayloadRetention,
                    value.MaxParallelism,
                    value.BatchSize,
                    value.MaxAttempts,
                    value.BaseBackoff,
                    value.ClaimTimeout,
                    value.SweepSettleTimeout
                );
            }
        }

        public void Dispose()
        {
            reloadSubscription?.Dispose();
        }
    }

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
