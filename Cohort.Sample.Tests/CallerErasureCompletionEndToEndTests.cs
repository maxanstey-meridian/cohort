using System.Collections.Concurrent;
using System.Transactions;

using Cohort.Application;
using Cohort.Domain;
using Cohort.Sample.Entities;

using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.DependencyInjection;

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
