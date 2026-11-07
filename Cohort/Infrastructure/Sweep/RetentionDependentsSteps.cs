using System.Collections;
using System.Data.Common;

using Cohort.Application;
using Cohort.Domain;
using Cohort.Infrastructure.Audit;

using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;

namespace Cohort.Infrastructure.Sweep;

internal static class RetentionDependentsSteps
{
    /// <summary>
    /// The dependents step <paramref name="registration"/> names, resolved from
    /// <paramref name="services"/>. A step the provider resolves that the registration
    /// doesn't name (another container's) is refused, and so is a registration the provider
    /// resolves no step for: its rows would be mutated without their dependents retired.
    /// </summary>
    public static ResolvedRetentionDependents ResolveDependents(
        IServiceProvider services,
        IRetentionDependentsRegistration registration
    )
    {
        var entityType = registration.EntityType;
        var stepInterface = typeof(IRetentionDependents<>).MakeGenericType(entityType);
        var steps = ((IEnumerable)services.GetRequiredService(typeof(IEnumerable<>).MakeGenericType(stepInterface)))
            .Cast<object>()
            .ToArray();
        var stray = steps.FirstOrDefault(step => step.GetType() != registration.StepType);
        if (stray is not null)
        {
            throw new RetentionConfigurationException(
                [RetentionDependentsServiceRegistrations.DependentsNotRegisteredError(
                    $"Dependents step {stray.GetType().FullName}, resolved as IRetentionDependents<{entityType.FullName}>,")]
            );
        }

        if (steps.Length != 1)
        {
            throw new RetentionConfigurationException([
                $"Dependents step {registration.StepType.FullName} is registered for {entityType.FullName} with AddRetentionDependents, but the service provider resolves {steps.Length} IRetentionDependents<{entityType.FullName}> services instead of one, so its rows' dependents would not be retired exactly once. Register it only with services.AddRetentionDependents<TEntity, TStep>().",
            ]);
        }

        return new ResolvedRetentionDependents(steps[0]);
    }

    /// <summary>
    /// Detects explicitly, so a context with automatic detection turned off is still
    /// checked. Detection that throws (a step changed a tracked entity's key) is the
    /// step's unsaved change too.
    /// </summary>
    public static bool HasUnsavedChanges(DbContext db)
    {
        try
        {
            db.ChangeTracker.DetectChanges();
            return db.ChangeTracker.HasChanges();
        }
        catch (Exception)
        {
            return true;
        }
    }

    /// <summary>
    /// Records a row left unmutated because the dependents step failed before its mutation:
    /// one row detail whose <c>Failure</c> says why, in the mutation's transaction, whatever
    /// the entity's row-detail audit level. It is not a <see cref="SweepEvent.RowDetail"/>,
    /// so audit observers see mutated rows only. An exception is recorded as the sanitized
    /// diagnostic envelope; a failure Cohort found itself as its plain reason.
    /// </summary>
    public static async Task PersistBeforeFailureAsync(
        DbConnection conn,
        DbTransaction transaction,
        SweepMutationContext execution,
        RetentionEntry entry,
        Strategy strategy,
        Guid tenantId,
        string recordId,
        BeforeMutationFailure failed,
        ILogger logger,
        CancellationToken ct
    )
    {
        string failure;
        if (failed.Failure is { } exception)
        {
            var diagnostic = RetentionFailureDiagnostic.Create(exception);
            failure = diagnostic.ToString();
            logger.LogError(
                exception,
                "Cohort dependents step {StepType} failed before mutation for sweep {SweepId} and entity {EntityType}. Diagnostic {DiagnosticId}.",
                failed.StepType.FullName,
                execution.SweepId,
                entry.EntityType.FullName,
                diagnostic.DiagnosticIdText
            );
        }
        else
        {
            failure = $"reason={failed.Reason}";
            logger.LogError(
                "Cohort dependents step {StepType} failed before mutation for sweep {SweepId} and entity {EntityType} ({Reason}).",
                failed.StepType.FullName,
                execution.SweepId,
                entry.EntityType.FullName,
                failed.Reason
            );
        }

        var statement = EfRetentionAuditWriter.RowDetailStatement(
            entry.CohortTables,
            execution.SweepId,
            execution.At,
            entry.EntityType,
            entry.RetentionEntityId,
            recordId,
            entry.Category,
            strategy,
            tenantId,
            failure
        );
        await using var command = statement.Parameters.CreateCommand(conn, transaction, statement.Sql);
        await command.ExecuteNonQueryAsync(ct);
    }
}

/// <summary>
/// The host's service registrations, read when Cohort validates its configuration. Only
/// <c>AddRetentionDependents</c> makes a dependents step: an
/// <see cref="IRetentionDependents{TEntity}"/> service it didn't register would never run
/// while its rows are still mutated, and one registered twice would run twice per row. Read
/// from the descriptors alone, so nothing is constructed to find out. The collection is
/// complete by the time the provider that resolves this exists.
/// </summary>
internal sealed class RetentionDependentsServiceRegistrations(IServiceCollection services)
{
    /// <summary>
    /// Every dependents step service the host registered some other way than with
    /// <c>AddRetentionDependents</c>, or more than once, and every registration whose service
    /// was later removed or replaced.
    /// </summary>
    public IReadOnlyList<string> FindInvalidStepServices()
    {
        var registrations = DependentsRegistrations()
            .Select(registration => (Entity: registration.EntityType, Implementation: registration.StepType))
            .ToHashSet();
        var errors = new List<string>();
        var registeredServices = new List<(Type Entity, Type Implementation)>();
        var refusedServices = new HashSet<(Type Entity, Type Implementation)>();
        foreach (var descriptor in services.Where(descriptor =>
                     descriptor.ServiceType.IsGenericType
                     && descriptor.ServiceType.GetGenericTypeDefinition() == typeof(IRetentionDependents<>)))
        {
            var entity = descriptor.ServiceType.IsGenericTypeDefinition ? null : descriptor.ServiceType.GenericTypeArguments[0];
            var service = entity is null ? "IRetentionDependents<>" : $"IRetentionDependents<{entity.FullName}>";

            // Only AddRetentionDependents's own shape, scoped by type, runs in each batch's
            // scope. A singleton or instance registered first makes its add a no-op, so a
            // matching registration isn't enough.
            var registered = descriptor switch
            {
                { IsKeyedService: true } => $"A keyed {service} service",
                { ImplementationFactory: not null } => $"A factory-registered {service} service",
                { ImplementationInstance: { } instance } => $"An instance of {instance.GetType().FullName}, registered as {service},",
                { Lifetime: not ServiceLifetime.Scoped } =>
                    $"Dependents step {descriptor.ImplementationType!.FullName}, registered as a {descriptor.Lifetime.ToString().ToLowerInvariant()} {service},",
                _ when entity is not null && registrations.Contains((entity, descriptor.ImplementationType!)) => null,
                _ => $"Dependents step {descriptor.ImplementationType!.FullName}, registered as {service},",
            };
            if (registered is null)
            {
                registeredServices.Add((entity!, descriptor.ImplementationType!));
                continue;
            }

            // The implementation type TryAddEnumerable itself compares: a factory's is its
            // delegate's return type.
            if (entity is not null
                && (descriptor.ImplementationType
                    ?? descriptor.ImplementationInstance?.GetType()
                    ?? descriptor.ImplementationFactory?.GetType().GenericTypeArguments[^1]) is { } implementation)
            {
                refusedServices.Add((entity, implementation));
            }

            errors.Add(DependentsNotRegisteredError(registered));
        }

        foreach (var duplicate in registeredServices.GroupBy(pair => pair).Where(group => group.Count() > 1))
        {
            errors.Add(
                $"Dependents step {duplicate.Key.Implementation.FullName} is registered {duplicate.Count()} times as IRetentionDependents<{duplicate.Key.Entity.FullName}>, so it would run {duplicate.Count()} times per row. Register it once, with AddRetentionDependents."
            );
        }

        // A service removed or replaced after AddRetentionDependents ran leaves its
        // registration behind, so its rows would be mutated without it. One refused above in
        // the wrong shape is already reported.
        foreach (var (entity, implementation) in registrations
                     .Except(registeredServices)
                     .Except(refusedServices)
                     .OrderBy(pair => pair.Implementation.FullName, StringComparer.Ordinal)
                     .ThenBy(pair => pair.Entity.FullName, StringComparer.Ordinal))
        {
            errors.Add(
                $"Dependents step {implementation.FullName} is registered for {entity.FullName} with AddRetentionDependents, but its scoped IRetentionDependents<{entity.FullName}> service was removed or replaced, so it wouldn't run in each batch's scope, or at all while its rows are still swept or erased. "
                    + "Register it only with services.AddRetentionDependents<TEntity, TStep>() and leave the service it adds in place."
            );
        }

        return errors;
    }

    public IRetentionDependentsRegistration[] DependentsRegistrations() =>
        services
            .Where(descriptor => descriptor.ServiceType == typeof(IRetentionDependentsRegistration) && !descriptor.IsKeyedService)
            .Select(descriptor => descriptor.ImplementationInstance)
            .OfType<IRetentionDependentsRegistration>()
            .ToArray();

    public static string DependentsNotRegisteredError(string registered) =>
        $"{registered} isn't registered with AddRetentionDependents alone, so it wouldn't run in each batch's scope, or at all while its rows are still swept or erased. "
            + "Register it only with services.AddRetentionDependents<TEntity, TStep>(), which lets DI construct it in each batch's scope; a dependents step can't be a singleton, transient, an instance, a factory, keyed or open generic.";
}

/// <summary>
/// The dependents step of an entity, resolved for one batch.
/// </summary>
internal sealed class ResolvedRetentionDependents(object instance)
{
    public object Instance { get; } = instance;

    public Type StepType { get; } = instance.GetType();
}

/// <summary>
/// One batch's dependents step, and the context it was resolved with: a scope of its own
/// whose context is on the run's connection and transaction, never the run's context.
/// </summary>
internal sealed record RetentionDependentsScope(DbContext Db, ResolvedRetentionDependents Dependents);

/// <summary>
/// What failed a row before its mutation: the dependents step, and either the exception or
/// a Cohort-defined reason, which carries no step or row data. Exactly one of
/// the two is set, so it is built only through
/// <see cref="FromException(ResolvedRetentionDependents, Exception)"/> or
/// <see cref="FromReason(ResolvedRetentionDependents, string)"/>.
/// </summary>
internal sealed record BeforeMutationFailure
{
    /// <summary>The step returned with unsaved changes on its DbContext.</summary>
    public const string DependentsUnsavedChangesReason = "dependents-unsaved-changes";

    /// <summary>The step changed or removed a row under an active hold.</summary>
    public const string DependentsChangedHeldRowReason = "dependents-changed-held-row";

    /// <summary>The step took its own row out of the mutation's scope.</summary>
    public const string DependentsChangedRowReason = "dependents-changed-row";

    private BeforeMutationFailure(Type stepType, Exception? failure, string? reason)
    {
        StepType = stepType;
        Failure = failure;
        Reason = reason;
    }

    public Type StepType { get; }

    public Exception? Failure { get; }

    public string? Reason { get; }

    public static BeforeMutationFailure FromException(ResolvedRetentionDependents step, Exception failure) =>
        new(step.StepType, failure, null);

    public static BeforeMutationFailure FromReason(ResolvedRetentionDependents step, string reason) =>
        new(step.StepType, null, reason);
}
