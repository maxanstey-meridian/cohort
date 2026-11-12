using System.Collections.Concurrent;
using System.Transactions;

using Cohort.Application;
using Cohort.Domain;
using Cohort.Sample.Entities;

using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;

namespace Cohort.Sample.Tests;

// How a Caller erasure inside a System.Transactions transaction tells its observers once the
// transaction completes: Cohort's handler for TransactionCompleted runs on the thread that
// disposes the host's scope, under whatever ambient transaction that scope leaves behind.
public sealed class CallerErasureCompletionEndToEndTests(PostgresFixture fixture)
    : IntegrationTestBase(fixture)
{
    private const string ObserverNoteBody = "written-by-observer";

    // A scope is popped before it commits, so a RequiresNew scope nested in the host's outer
    // scope completes with the outer transaction ambient. The observers are told from that
    // handler: a write of their own must not join the host's outer transaction.
    [Fact]
    public async Task A_Scope_Nested_In_An_Outer_Scope_Tells_Observers_Outside_The_Outer_Transaction()
    {
        var tenant = new TenantContext(Guid.NewGuid(), "uk", new Dictionary<string, string>());
        var subject = Guid.NewGuid();
        var erasedId = await SeedNoteForErasureAsync(tenant.Id, subject);
        var observer = new WritingObserver(ConnectionString);
        using var host = new CohortTestHost(
            ConnectionString,
            configureServices: services => services.AddSingleton<IRetentionAuditObserver>(observer)
        );

        ErasureResult result;
        using (new TransactionScope(
            TransactionScopeOption.Required,
            ReadCommitted,
            TransactionScopeAsyncFlowOption.Enabled
        ))
        {
            result = await host.RunWithServicesAsync(async services =>
            {
                using var inner = new TransactionScope(
                    TransactionScopeOption.RequiresNew,
                    ReadCommitted,
                    TransactionScopeAsyncFlowOption.Enabled
                );
                var erased = await services.GetRequiredService<IRetentionErasureService>().ExecuteAsync(
                    RetentionErasureRequest.Tenanted(tenant, CallerScope(subject), transaction: ErasureTransaction.Caller)
                );
                inner.Complete();
                return erased;
            });

            // The outer scope is disposed here without Complete().
        }

        result.EntityFailures.Should().BeEmpty();
        observer.Deliveries.Should().BeGreaterThan(0);
        await using var verify = Host.CreateDbContext();
        (await verify.Notes.AnyAsync(note => note.Id == erasedId)).Should().BeFalse();
        // Every observer write survived the outer rollback, so none was made inside it.
        (await verify.Notes.CountAsync(note => note.Body == ObserverNoteBody)).Should().Be(observer.Deliveries);
    }

    // The host disposes the scope on a thread with a single-threaded SynchronizationContext
    // (Blazor Server, WPF, WinForms), which is blocked while Cohort checks the commit and tells the
    // observers. Their awaits must not wait for that thread.
    [Fact]
    public async Task Completing_A_Scope_On_A_Single_Threaded_Synchronization_Context_Does_Not_Deadlock()
    {
        var tenant = new TenantContext(Guid.NewGuid(), "uk", new Dictionary<string, string>());
        var subject = Guid.NewGuid();
        var erasedId = await SeedNoteForErasureAsync(tenant.Id, subject);
        var observer = new RecordingObserver();
        using var host = new CohortTestHost(
            ConnectionString,
            configureServices: services => services.AddSingleton<IRetentionAuditObserver>(observer)
        );

        ErasureResult? result = null;
        var finished = RunOnSingleThreadedContext(
            async () =>
            {
                result = await host.RunWithServicesAsync(async services =>
                {
                    using var scope = new TransactionScope(
                        TransactionScopeOption.Required,
                        ReadCommitted,
                        TransactionScopeAsyncFlowOption.Enabled
                    );
                    var erased = await services.GetRequiredService<IRetentionErasureService>().ExecuteAsync(
                        RetentionErasureRequest.Tenanted(tenant, CallerScope(subject), transaction: ErasureTransaction.Caller)
                    );
                    scope.Complete();
                    return erased;
                });
            },
            TimeSpan.FromSeconds(30)
        );

        finished.Should().BeTrue("disposing the completed scope must not wait for the caller's own thread");
        result!.EntityFailures.Should().BeEmpty();
        observer.Events.Should().NotBeEmpty();
        await using var verify = Host.CreateDbContext();
        (await verify.Notes.AnyAsync(note => note.Id == erasedId)).Should().BeFalse();
    }

    // The caller's DI scope is gone by the time the transaction completes, and a scoped observer
    // is disposed with it. The events must reach an observer of the scope Cohort opens for the
    // telling, not the caller scope's disposed one.
    [Fact]
    public async Task A_Scoped_Observer_Of_A_Disposed_Caller_Scope_Is_Not_The_One_Told()
    {
        var tenant = new TenantContext(Guid.NewGuid(), "uk", new Dictionary<string, string>());
        var subject = Guid.NewGuid();
        await SeedNoteForErasureAsync(tenant.Id, subject);
        var heard = new ScopedObserverLog();
        using var host = new CohortTestHost(
            ConnectionString,
            configureServices: services => services
                .AddSingleton(heard)
                .AddScoped<IRetentionAuditObserver, DisposableScopedObserver>()
        );

        using (var scope = new TransactionScope(TransactionScopeOption.Required, ReadCommitted, TransactionScopeAsyncFlowOption.Enabled))
        {
            await host.RunWithServicesAsync(services => EraseAsync(services, tenant, subject));
            heard.Entries.Should().BeEmpty();
            scope.Complete();
        }

        var events = heard.Entries.Select(entry => entry.Event).ToList();
        events.Should().StartWith(events.OfType<SweepEvent.Started>().Single())
            .And.EndWith(events.OfType<SweepEvent.Completed>().Single());
        heard.Entries.Should().OnlyContain(entry => !entry.InstanceWasDisposed);
    }

    // The check and the telling run in a scope Cohort creates through the host's execution-scope
    // hook, as the run's tenant: the host's tenant isolation covers them.
    [Fact]
    public async Task The_Check_And_The_Telling_After_The_Commit_Run_In_A_Scope_Opened_Through_The_Hook_For_The_Runs_Tenant()
    {
        var tenant = new TenantContext(Guid.NewGuid(), "uk", new Dictionary<string, string>());
        var subject = Guid.NewGuid();
        await SeedNoteForErasureAsync(tenant.Id, subject);
        var hook = new RecordingExecutionScope();
        var observer = new RecordingObserver();
        using var host = new CohortTestHost(
            ConnectionString,
            configureServices: services => services
                .AddSingleton<IRetentionExecutionScope>(hook)
                .AddSingleton<IRetentionAuditObserver>(observer)
        );
        var runsAsTenant = () => hook.Ran.Count(context => context == new RetentionExecutionContext.Tenant(tenant.Id));

        int whenTheErasureReturned;
        using (var scope = new TransactionScope(TransactionScopeOption.Required, ReadCommitted, TransactionScopeAsyncFlowOption.Enabled))
        {
            await host.RunWithServicesAsync(services => EraseAsync(services, tenant, subject));
            whenTheErasureReturned = runsAsTenant();
            scope.Complete();
        }

        runsAsTenant().Should().Be(whenTheErasureReturned + 1);
        observer.Events.Should().NotBeEmpty();
    }

    // A hook that fails before the check was answered leaves the commit unconfirmed: the events
    // are dropped and a warning says so. The commit itself has happened and stays.
    [Fact]
    public async Task A_Hook_That_Fails_Before_The_Check_Leaves_The_Commit_Unconfirmed()
    {
        var tenant = new TenantContext(Guid.NewGuid(), "uk", new Dictionary<string, string>());
        var subject = Guid.NewGuid();
        var erasedId = await SeedNoteForErasureAsync(tenant.Id, subject);
        var hook = new FailingTenantExecutionScope(runsWorkFirst: false);
        var observer = new RecordingObserver();
        var logs = new RecordingLogProvider();
        using var host = new CohortTestHost(
            ConnectionString,
            configureServices: services => services
                .AddSingleton<IRetentionExecutionScope>(hook)
                .AddSingleton<IRetentionAuditObserver>(observer)
                .AddSingleton<ILoggerProvider>(logs)
        );

        var scope = new TransactionScope(TransactionScopeOption.Required, ReadCommitted, TransactionScopeAsyncFlowOption.Enabled);
        var result = await host.RunWithServicesAsync(services => EraseAsync(services, tenant, subject));
        scope.Complete();
        hook.Armed = true;
        scope.Dispose();

        observer.Events.Should().BeEmpty();
        logs.Entries.Should().ContainSingle(entry =>
            entry.Level == LogLevel.Warning && entry.Message.Contains("could not confirm") && entry.Message.Contains(result.SweepId.ToString()));
        await using var verify = Host.CreateDbContext();
        (await verify.Notes.AnyAsync(note => note.Id == erasedId)).Should().BeFalse();
        (await verify.Database.SqlQuery<int>($"""SELECT "Status" AS "Value" FROM "sweep_run" WHERE "SweepId" = {result.SweepId}""").SingleAsync())
            .Should().Be((int)SweepRunStatus.Succeeded);
    }

    // A hook that fails after it ran the work: the check was answered and the events were told, so
    // the failure is only logged, at Error, and nothing is reported as unconfirmed.
    [Fact]
    public async Task A_Hook_That_Fails_After_The_Check_Was_Answered_Is_Only_Logged()
    {
        var tenant = new TenantContext(Guid.NewGuid(), "uk", new Dictionary<string, string>());
        var subject = Guid.NewGuid();
        await SeedNoteForErasureAsync(tenant.Id, subject);
        var hook = new FailingTenantExecutionScope(runsWorkFirst: true);
        var observer = new RecordingObserver();
        var logs = new RecordingLogProvider();
        using var host = new CohortTestHost(
            ConnectionString,
            configureServices: services => services
                .AddSingleton<IRetentionExecutionScope>(hook)
                .AddSingleton<IRetentionAuditObserver>(observer)
                .AddSingleton<ILoggerProvider>(logs)
        );

        var scope = new TransactionScope(TransactionScopeOption.Required, ReadCommitted, TransactionScopeAsyncFlowOption.Enabled);
        await host.RunWithServicesAsync(services => EraseAsync(services, tenant, subject));
        scope.Complete();
        hook.Armed = true;
        scope.Dispose();

        observer.Events.Should().StartWith(observer.Events.OfType<SweepEvent.Started>().Single())
            .And.EndWith(observer.Events.OfType<SweepEvent.Completed>().Single());
        logs.Entries.Should().NotContain(entry => entry.Message.Contains("could not confirm"));
        logs.Entries.Should().ContainSingle(entry => entry.Level == LogLevel.Error);
    }

    // Cohort's handler is subscribed first, and a handler that throws stops every one the host
    // subscribed after it (and, with a connection enlisted, only a quiet handler lets a
    // BeginCommit commit finish). So nothing Cohort's handler does, however the hook fails, may
    // escape it: the host's own handler, subscribed once the erasure returned, must still run.
    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task A_Failing_Hook_Does_Not_Stop_The_Hosts_Own_Completion_Handler(bool hookRunsTheWorkFirst)
    {
        var tenant = new TenantContext(Guid.NewGuid(), "uk", new Dictionary<string, string>());
        var subject = Guid.NewGuid();
        await SeedNoteForErasureAsync(tenant.Id, subject);
        var hook = new FailingTenantExecutionScope(hookRunsTheWorkFirst);
        using var host = new CohortTestHost(
            ConnectionString,
            configureServices: services => services
                .AddSingleton<IRetentionExecutionScope>(hook)
                .AddSingleton<IRetentionAuditObserver>(new RecordingObserver())
        );

        var scope = new TransactionScope(TransactionScopeOption.Required, ReadCommitted, TransactionScopeAsyncFlowOption.Enabled);
        await host.RunWithServicesAsync(services => EraseAsync(services, tenant, subject));
        var hostHandlerRan = false;
        Transaction.Current!.TransactionCompleted += (_, _) => hostHandlerRan = true;
        scope.Complete();
        hook.Armed = true;
        scope.Dispose();

        hostHandlerRan.Should().BeTrue();
    }

    private static Task<ErasureResult> EraseAsync(IServiceProvider services, TenantContext tenant, Guid subject) =>
        services.GetRequiredService<IRetentionErasureService>().ExecuteAsync(
            RetentionErasureRequest.Tenanted(tenant, CallerScope(subject), transaction: ErasureTransaction.Caller)
        );

    private static TransactionOptions ReadCommitted =>
        new() { IsolationLevel = IsolationLevel.ReadCommitted };

    private static ErasureScope CallerScope(Guid subject) =>
        new("user", subject, allowSoftDeleteAsErasure: true);

    private async Task<Guid> SeedNoteForErasureAsync(Guid tenantId, Guid subject)
    {
        var erasedId = Guid.NewGuid();
        await using var db = Host.CreateDbContext();
        db.Notes.Add(
            new Note
            {
                Id = erasedId,
                TenantId = tenantId,
                SubjectId = subject,
                CreatedAt = new DateTimeOffset(2026, 4, 12, 12, 0, 0, TimeSpan.Zero).AddDays(-45),
                Body = "erase-me",
            }
        );
        await db.SaveChangesAsync();
        return erasedId;
    }

    /// <summary>
    /// Runs <paramref name="work"/> on a thread whose SynchronizationContext runs continuations
    /// on that thread only, between its own blocking calls, as a UI thread does. False when it
    /// did not finish in time.
    /// </summary>
    private static bool RunOnSingleThreadedContext(Func<Task> work, TimeSpan timeout)
    {
        Exception? failure = null;
        var thread = new Thread(() =>
        {
            var context = new PumpContext();
            SynchronizationContext.SetSynchronizationContext(context);
            var task = work();
            _ = task.ContinueWith(_ => context.Complete(), TaskScheduler.Default);
            context.Run();
            try
            {
                task.GetAwaiter().GetResult();
            }
            catch (Exception ex)
            {
                failure = ex;
            }
        })
        {
            IsBackground = true,
        };
        thread.Start();
        var finished = thread.Join(timeout);
        if (failure is not null)
        {
            throw new InvalidOperationException("The work on the single-threaded context failed.", failure);
        }

        return finished;
    }

    private sealed class PumpContext : SynchronizationContext
    {
        private readonly BlockingCollection<(SendOrPostCallback Callback, object? State)> queue = new();

        public override void Post(SendOrPostCallback d, object? state)
        {
            if (!queue.IsAddingCompleted)
            {
                queue.Add((d, state));
            }
        }

        public void Complete() => queue.CompleteAdding();

        public void Run()
        {
            foreach (var (callback, state) in queue.GetConsumingEnumerable())
            {
                callback(state);
            }
        }
    }

    /// <summary>Writes a note of its own through its own context for every event, as an observer that keeps its own records would.</summary>
    private sealed class WritingObserver(string connectionString) : IRetentionAuditObserver
    {
        private int deliveries;

        public int Deliveries => Volatile.Read(ref deliveries);

        public async Task OnCommittedAsync(SweepEvent evt, CancellationToken ct)
        {
            await using var db = new SampleDbContext(
                new DbContextOptionsBuilder<SampleDbContext>().UseNpgsql(connectionString).Options
            );
            db.Notes.Add(
                new Note
                {
                    Id = Guid.NewGuid(),
                    TenantId = Guid.NewGuid(),
                    CreatedAt = DateTimeOffset.UtcNow,
                    Body = ObserverNoteBody,
                }
            );
            await db.SaveChangesAsync(ct);
            Interlocked.Increment(ref deliveries);
        }
    }

    /// <summary>Records which observer instance heard each event, and whether its DI scope had disposed it by then.</summary>
    private sealed class ScopedObserverLog
    {
        private readonly ConcurrentQueue<(SweepEvent Event, bool InstanceWasDisposed)> entries = new();

        public IReadOnlyList<(SweepEvent Event, bool InstanceWasDisposed)> Entries => [.. entries];

        public void Heard(SweepEvent evt, bool instanceWasDisposed) => entries.Enqueue((evt, instanceWasDisposed));
    }

    private sealed class DisposableScopedObserver(ScopedObserverLog log) : IRetentionAuditObserver, IDisposable
    {
        private volatile bool disposed;

        public Task OnCommittedAsync(SweepEvent evt, CancellationToken ct)
        {
            log.Heard(evt, disposed);
            return Task.CompletedTask;
        }

        public void Dispose() => disposed = true;
    }

    /// <summary>Once armed, fails Cohort's work for a tenant: instead of running it, or after running it.</summary>
    private sealed class FailingTenantExecutionScope(bool runsWorkFirst) : IRetentionExecutionScope
    {
        public volatile bool Armed;

        public async Task RunAsync(RetentionExecutionContext context, Func<Task> work, CancellationToken ct)
        {
            if (!Armed || context is not RetentionExecutionContext.Tenant)
            {
                await work();
                return;
            }

            if (runsWorkFirst)
            {
                await work();
            }

            throw new InvalidOperationException("Simulated execution-scope failure.");
        }
    }

    private sealed class RecordingLogProvider : ILoggerProvider
    {
        private readonly ConcurrentQueue<(LogLevel Level, string Message)> entries = new();

        public IReadOnlyList<(LogLevel Level, string Message)> Entries => [.. entries];

        public ILogger CreateLogger(string categoryName) => new RecordingLogger(entries);

        public void Dispose() { }

        private sealed class RecordingLogger(ConcurrentQueue<(LogLevel Level, string Message)> entries) : ILogger
        {
            public IDisposable? BeginScope<TState>(TState state)
                where TState : notnull => null;

            public bool IsEnabled(LogLevel logLevel) => true;

            public void Log<TState>(LogLevel logLevel, EventId eventId, TState state, Exception? exception, Func<TState, Exception?, string> formatter) =>
                entries.Enqueue((logLevel, formatter(state, exception)));
        }
    }

    private sealed class RecordingObserver : IRetentionAuditObserver
    {
        private readonly ConcurrentQueue<SweepEvent> events = new();

        public IReadOnlyList<SweepEvent> Events => [.. events];

        public Task OnCommittedAsync(SweepEvent evt, CancellationToken ct)
        {
            events.Enqueue(evt);
            return Task.CompletedTask;
        }
    }
}
