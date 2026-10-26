using System.Reflection;
using System.Text.Json;

using Cohort.Application;
using Cohort.Domain;
using Cohort.Hosting;
using Cohort.Infrastructure;
using Cohort.Infrastructure.Migrations;
using Cohort.Sample.Entities;

using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;

using Npgsql;

namespace Cohort.Sample.Tests;

public sealed class RetentionHandlerEndToEndTests(PostgresFixture fixture)
    : IntegrationTestBase(fixture)
{
    private const int PendingState = 0;
    private const int InFlightState = 1;
    private const int SucceededState = 2;
    private const int DeadLetteredState = 3;

    [Fact]
    public async Task FlushAsync_Scrubs_Expired_Payload_While_Handler_Work_Is_Queued()
    {
        var tenantId = Guid.NewGuid();
        var fileId = Guid.NewGuid();
        var asOf = new DateTimeOffset(2026, 4, 13, 12, 0, 0, TimeSpan.Zero);
        var cleanupStore = new BlobCleanupStoreSpy();

        await using (var db = Host.CreateDbContext())
        {
            db.BlobBackedFiles.Add(
                new BlobBackedFile
                {
                    Id = fileId,
                    TenantId = tenantId,
                    CreatedAt = asOf.AddDays(-120),
                    StoragePath = "blob://tenant-a/scrubbed/orphan.pdf",
                    OriginalFileName = "orphan.pdf",
                    ContentType = "application/pdf",
                }
            );
            await db.SaveChangesAsync();
        }

        using var handlerHost = new CohortTestHost(
            GetConnectionString(),
            configurationOverrides: new Dictionary<string, string?>
            {
                ["Cohort:RowHandlerDispatch:PayloadRetention"] = "01:00:00",
                // A retry schedule must fit inside PayloadRetention.
                ["Cohort:RowHandlerDispatch:MaxAttempts"] = "1",
            },
            configureServices: services =>
            {
                services.AddSingleton(cleanupStore);
                services.AddRowHandler<BlobBackedFile, BlobBackedFileCleanupHandler>();
            }
        );

        var result = await handlerHost.RunSweepAsync(
            new TenantContext(tenantId, "uk", new Dictionary<string, string>()),
            asOf
        );

        // The queued work sits undrained past the payload retention window.
        await BackdateRowDetailsAsync(result.SweepId, DateTimeOffset.UtcNow.AddHours(-2));

        await handlerHost.RunWithServicesAsync(async serviceProvider =>
        {
            var dispatcher = serviceProvider.GetRequiredService<IRetentionRowDispatcher>();
            var flush = await dispatcher.FlushAsync();
            // Without its snapshot the work can't be requeued: it would only fail again.
            flush.DeadLetteredRequeueable.Should().Be(0);
            (await dispatcher.RequeueDeadLetteredAsync()).Should().Be(0);
        });

        cleanupStore.DeletedPaths.Should().BeEmpty();

        var statuses = await LoadHandlerStatusesAsync(result.SweepId);
        statuses.Should().ContainSingle();
        statuses[0].State.Should().Be(DeadLetteredState);
        statuses[0].Attempt.Should().Be(1);
        statuses[0].LastError.Should().Contain("payload-retention backstop");

        (await CountRemainingCapturedPayloadsAsync(result.SweepId)).Should().Be(0);
    }

    [Fact]
    public async Task FlushAsync_Delivers_Queued_Work_To_The_Handler_Carrying_Its_Id_After_A_Rename()
    {
        // The handler class is renamed between the sweep that queued its work and the flush
        // that delivers it. The persisted id is the [RowHandlerId], not the class name, so
        // the renamed class still receives the work.
        var tenantId = Guid.NewGuid();
        var asOf = new DateTimeOffset(2026, 4, 13, 12, 0, 0, TimeSpan.Zero);
        var recorder = new HandlerExecutionSink();
        await using (var db = Host.CreateDbContext())
        {
            db.Notes.Add(
                new Note
                {
                    Id = Guid.NewGuid(),
                    TenantId = tenantId,
                    CreatedAt = asOf.AddDays(-120),
                    Body = "queued-before-rename",
                }
            );
            await db.SaveChangesAsync();
        }

        RetentionSweepResult result;
        using (
            var beforeRename = new CohortTestHost(
                GetConnectionString(),
                configureServices: services =>
                {
                    services.AddSingleton(recorder);
                    services.AddRowHandler<Note, NoteHandlerBeforeRename>();
                }
            )
        )
        {
            result = await beforeRename.RunSweepAsync(
                new TenantContext(tenantId, "uk", new Dictionary<string, string>()),
                asOf
            );
        }

        (await LoadHandlerStatusesAsync(result.SweepId))
            .Should()
            .ContainSingle()
            .Which.HandlerType.Should()
            .Be(NoteHandlerBeforeRename.Id);
        recorder.AfterCalls.Should().BeEmpty();

        using var afterRename = new CohortTestHost(
            GetConnectionString(),
            configureServices: services =>
            {
                services.AddSingleton(recorder);
                services.AddRowHandler<Note, NoteHandlerAfterRename>();
            }
        );
        await afterRename.RunWithServicesAsync(async serviceProvider =>
            await serviceProvider.GetRequiredService<IRetentionRowDispatcher>().FlushAsync()
        );

        recorder.AfterCalls.Should().Equal("renamed:queued-before-rename:1");
        var status = (await LoadHandlerStatusesAsync(result.SweepId)).Should().ContainSingle().Subject;
        status.HandlerType.Should().Be(NoteHandlerBeforeRename.Id);
        status.CompletedAt.Should().NotBeNull();
        status.LastError.Should().BeNull();
    }

    [Fact]
    public async Task FlushAsync_Deferred_Row_Handler_Waits_For_Sweep_Completion_But_Immediate_Does_Not()
    {
        var immediateTenantId = Guid.NewGuid();
        var deferredTenantId = Guid.NewGuid();
        var asOf = new DateTimeOffset(2026, 4, 13, 12, 0, 0, TimeSpan.Zero);
        var immediateNoteId = Guid.NewGuid();
        var deferredNoteId = Guid.NewGuid();
        var recorder = new HandlerExecutionSink();

        await using (var db = Host.CreateDbContext())
        {
            db.Notes.Add(
                new Note
                {
                    Id = immediateNoteId,
                    TenantId = immediateTenantId,
                    CreatedAt = asOf.AddDays(-120),
                    Body = "immediate-gate",
                }
            );
            await db.SaveChangesAsync();
        }

        using var immediateHost = new CohortTestHost(
            GetConnectionString(),
            configurationOverrides: new Dictionary<string, string?>
            {
                [$"{CohortOptions.SectionName}:RowHandlerDispatch:PollInterval"] = "1.00:00:00",
            },
            configureServices: services =>
            {
                services.AddSingleton(recorder);
                services.AddRowHandler<Note, DispatchRecordingNoteHandler>();
            }
        );

        var immediateResult = await immediateHost.RunSweepAsync(
            new TenantContext(immediateTenantId, "uk", new Dictionary<string, string>()),
            asOf
        );

        await SetSweepCompletedAtAsync(immediateResult.SweepId, null);
        await immediateHost.RunWithServicesAsync(async serviceProvider =>
        {
            var dispatcher = serviceProvider.GetRequiredService<IRetentionRowDispatcher>();
            await dispatcher.FlushAsync();
        });

        recorder.AfterCalls.Should().ContainSingle(call => call == "after:immediate-gate:1");

        var immediateStatuses = await LoadHandlerStatusesAsync(immediateResult.SweepId);
        immediateStatuses.Should().ContainSingle();
        immediateStatuses[0].DispatchPhase.Should().Be((int)RowHandlerDispatchPhase.Immediate);
        immediateStatuses[0].CompletedAt.Should().NotBeNull();

        recorder.AfterCalls.Clear();

        await using (var db = Host.CreateDbContext())
        {
            db.Notes.Add(
                new Note
                {
                    Id = deferredNoteId,
                    TenantId = deferredTenantId,
                    CreatedAt = asOf.AddDays(-121),
                    Body = "deferred-gate",
                }
            );
            await db.SaveChangesAsync();
        }

        using var deferredHost = new CohortTestHost(
            GetConnectionString(),
            configurationOverrides: new Dictionary<string, string?>
            {
                [$"{CohortOptions.SectionName}:RowHandlerDispatch:PollInterval"] = "1.00:00:00",
            },
            configureServices: services =>
            {
                services.AddSingleton(recorder);
                services.AddRowHandler<Note, DispatchRecordingNoteHandler>(
                    RowHandlerDispatchPhase.AfterSweepSettled
                );
            }
        );

        var deferredResult = await deferredHost.RunSweepAsync(
            new TenantContext(deferredTenantId, "uk", new Dictionary<string, string>()),
            asOf
        );

        await SetSweepCompletedAtAsync(deferredResult.SweepId, null);
        RowDispatcherFlushResult? midSweepFlushResult = null;
        await deferredHost.RunWithServicesAsync(async serviceProvider =>
        {
            var dispatcher = serviceProvider.GetRequiredService<IRetentionRowDispatcher>();
            midSweepFlushResult = await dispatcher.FlushAsync();
        });

        recorder.AfterCalls.Should().BeEmpty();
        midSweepFlushResult.Should().NotBeNull();
        midSweepFlushResult!.Settled.Should().BeFalse();
        midSweepFlushResult.InFlightRemaining.Should().Be(0);
        midSweepFlushResult.PendingRemaining.Should().Be(1);

        var pendingStatuses = await LoadHandlerStatusesAsync(deferredResult.SweepId);
        pendingStatuses.Should().ContainSingle();
        pendingStatuses[0]
            .DispatchPhase.Should()
            .Be((int)RowHandlerDispatchPhase.AfterSweepSettled);
        pendingStatuses[0].Attempt.Should().Be(0);
        pendingStatuses[0].ClaimedAt.Should().BeNull();
        pendingStatuses[0].CompletedAt.Should().BeNull();

        await SetSweepCompletedAtAsync(deferredResult.SweepId, deferredResult.CompletedAt);
        RowDispatcherFlushResult? settledFlushResult = null;
        await deferredHost.RunWithServicesAsync(async serviceProvider =>
        {
            var dispatcher = serviceProvider.GetRequiredService<IRetentionRowDispatcher>();
            settledFlushResult = await dispatcher.FlushAsync();
        });

        recorder.AfterCalls.Should().ContainSingle(call => call == "after:deferred-gate:1");
        settledFlushResult.Should().NotBeNull();
        settledFlushResult!.Settled.Should().BeTrue();

        var completedStatuses = await LoadHandlerStatusesAsync(deferredResult.SweepId);
        completedStatuses.Should().ContainSingle();
        completedStatuses[0].CompletedAt.Should().NotBeNull();
        completedStatuses[0].Attempt.Should().Be(1);
    }

    [Fact]
    public async Task FlushAsync_Recovers_Abandoned_Run_And_Releases_Deferred_Work()
    {
        var tenantId = Guid.NewGuid();
        var asOf = new DateTimeOffset(2026, 4, 13, 12, 0, 0, TimeSpan.Zero);
        var recorder = new HandlerExecutionSink();

        await using (var db = Host.CreateDbContext())
        {
            db.Notes.Add(
                new Note
                {
                    Id = Guid.NewGuid(),
                    TenantId = tenantId,
                    CreatedAt = asOf.AddDays(-120),
                    Body = "deferred-settle-timeout",
                }
            );
            await db.SaveChangesAsync();
        }

        using var handlerHost = new CohortTestHost(
            GetConnectionString(),
            configurationOverrides: new Dictionary<string, string?>
            {
                [$"{CohortOptions.SectionName}:RowHandlerDispatch:PollInterval"] = "1.00:00:00",
            },
            configureServices: services =>
            {
                services.AddSingleton(recorder);
                services.AddRowHandler<Note, DispatchRecordingNoteHandler>(
                    RowHandlerDispatchPhase.AfterSweepSettled
                );
            }
        );

        var result = await handlerHost.RunSweepAsync(
            new TenantContext(tenantId, "uk", new Dictionary<string, string>()),
            asOf
        );

        await SetSweepStartedAtAsync(result.SweepId, DateTimeOffset.UtcNow.AddDays(-1));
        await using var ownerConnection = new NpgsqlConnection(GetConnectionString());
        await ownerConnection.OpenAsync();
        await RetentionRunAdvisoryLock.AcquireAsync(ownerConnection, RetentionRunAdvisoryLock.KeyFor(result.SweepId), default);
        await handlerHost.RunWithServicesAsync(async serviceProvider =>
        {
            var activeFlush = await serviceProvider
                .GetRequiredService<IRetentionRowDispatcher>()
                .FlushAsync();
            activeFlush.PendingRemaining.Should().Be(1);
        });
        recorder.AfterCalls.Should().BeEmpty();
        await RetentionRunAdvisoryLock.ReleaseAsync(ownerConnection, RetentionRunAdvisoryLock.KeyFor(result.SweepId), default);

        RowDispatcherFlushResult? flushResult = null;
        await handlerHost.RunWithServicesAsync(async serviceProvider =>
        {
            flushResult = await serviceProvider
                .GetRequiredService<IRetentionRowDispatcher>()
                .FlushAsync();
        });

        recorder
            .AfterCalls.Should()
            .ContainSingle(call => call == "after:deferred-settle-timeout:1");
        flushResult.Should().NotBeNull();
        flushResult!.Settled.Should().BeTrue();
        flushResult.PendingRemaining.Should().Be(0);
        var completedStatus = (await LoadHandlerStatusesAsync(result.SweepId))
            .Should()
            .ContainSingle()
            .Subject;
        completedStatus.State.Should().Be(SucceededState);
        completedStatus.Attempt.Should().Be(1);
        completedStatus.CompletedAt.Should().NotBeNull();
        await using var runStatusCommand = ownerConnection.CreateCommand();
        runStatusCommand.CommandText =
            "SELECT \"Status\" FROM \"sweep_run\" WHERE \"SweepId\" = @sweepId";
        runStatusCommand.Parameters.AddWithValue("sweepId", result.SweepId);
        ((SweepRunStatus)(int)(await runStatusCommand.ExecuteScalarAsync())!)
            .Should()
            .Be(SweepRunStatus.Failed);
    }

    [Fact]
    public async Task Scheduled_Sweep_With_Handlers_Runs_OnBefore_In_Priority_Order_And_Queues_PostCommit_Work()
    {
        var tenantId = Guid.NewGuid();
        var asOf = new DateTimeOffset(2026, 4, 13, 12, 0, 0, TimeSpan.Zero);
        var noteId = Guid.NewGuid();
        var sink = new HandlerExecutionSink();

        await using (var db = Host.CreateDbContext())
        {
            db.Notes.Add(
                new Note
                {
                    Id = noteId,
                    TenantId = tenantId,
                    CreatedAt = asOf.AddDays(-120),
                    Body = "handler-target",
                }
            );
            await db.SaveChangesAsync();
        }

        using var handlerHost = new CohortTestHost(
            GetConnectionString(),
            configureServices: services =>
            {
                services.AddSingleton(sink);
                services.AddRowHandler<Note, LowPriorityNoteHandler>();
                services.AddRowHandler<Note, HighPriorityNoteHandler>();
            }
        );

        var result = await handlerHost.RunSweepAsync(
            new TenantContext(tenantId, "uk", new Dictionary<string, string>()),
            asOf
        );

        result
            .Counts.Should()
            .Contain(
                new EntitySweepCount(
                    typeof(Note),
                    "short-lived",
                    tenantId,
                    Strategy.Purge,
                    1,
                    HeldCount: 0,
                    SkippedCount: 0,
                    NullAnchorCount: 0,
                    RetainedCount: 0,
                    DeferredCount: 0,
                    BlockedCount: 0
                )
            );
        sink.BeforeCalls.Should().Equal("high", "low");

        await using (var verify = Host.CreateDbContext())
        {
            (await verify.Notes.AnyAsync(note => note.Id == noteId)).Should().BeFalse();
        }

        var rowDetails = await LoadCapturedRowsAsync(result.SweepId);
        rowDetails.Should().ContainSingle();
        rowDetails[0].EntityType.Should().Be(typeof(Note).FullName);
        rowDetails[0].RecordId.Should().Be(noteId.ToString());

        using (var payload = JsonDocument.Parse(rowDetails[0].CapturedPayload))
        {
            payload.RootElement.GetProperty("body").GetString().Should().Be("handler-target");
            payload.RootElement.GetProperty("priority").GetString().Should().Be("high-first");
        }

        var statuses = await LoadHandlerStatusesAsync(result.SweepId);
        statuses.Should().HaveCount(2);
        statuses
            .Select(status => status.HandlerType)
            .Should()
            .Contain(type =>
                type == HandlerIdOf<HighPriorityNoteHandler>()
            );
        statuses
            .Select(status => status.HandlerType)
            .Should()
            .Contain(type =>
                type == HandlerIdOf<LowPriorityNoteHandler>()
            );
        statuses.All(status => status.State == 0 && status.Attempt == 0).Should().BeTrue();
    }

    [Fact]
    public async Task Scheduled_Sweep_Flush_With_MaxParallelism_Greater_Than_One_Processes_One_Handler_At_A_Time_Per_Row()
    {
        var tenantId = Guid.NewGuid();
        var asOf = new DateTimeOffset(2026, 4, 13, 12, 0, 0, TimeSpan.Zero);
        var noteId = Guid.NewGuid();
        var sink = new HandlerExecutionSink();
        var gate = new DispatchBlockGate();

        await using (var db = Host.CreateDbContext())
        {
            db.Notes.Add(
                new Note
                {
                    Id = noteId,
                    TenantId = tenantId,
                    CreatedAt = asOf.AddDays(-120),
                    Body = "same-row-serial-dispatch",
                }
            );
            await db.SaveChangesAsync();
        }

        using var handlerHost = new CohortTestHost(
            GetConnectionString(),
            configurationOverrides: new Dictionary<string, string?>
            {
                [$"{CohortOptions.SectionName}:RowHandlerDispatch:MaxParallelism"] = "4",
            },
            configureServices: services =>
            {
                services.AddSingleton(sink);
                services.AddSingleton(gate);
                services.AddRowHandler<Note, LowPriorityAfterNoteHandler>();
                services.AddRowHandler<Note, BlockingHighPriorityAfterNoteHandler>();
            }
        );

        var result = await handlerHost.RunSweepAsync(
            new TenantContext(tenantId, "uk", new Dictionary<string, string>()),
            asOf
        );

        var flushTask = handlerHost.RunWithServicesAsync(async serviceProvider =>
        {
            var dispatcher = serviceProvider.GetRequiredService<IRetentionRowDispatcher>();
            await dispatcher.FlushAsync();
        });

        await gate.WaitUntilBlockedAsync().WaitAsync(TimeSpan.FromSeconds(5));

        var inFlightHigh = await WaitForHandlerStatusAsync(
            result.SweepId,
            row =>
                row.HandlerType == HandlerIdOf<BlockingHighPriorityAfterNoteHandler>()
                && row.State == InFlightState,
            TimeSpan.FromSeconds(5)
        );
        inFlightHigh.Attempt.Should().Be(1);

        var interimStatuses = await LoadHandlerStatusesAsync(result.SweepId);
        interimStatuses
            .Should()
            .ContainSingle(status =>
                status.HandlerType == HandlerIdOf<BlockingHighPriorityAfterNoteHandler>()
                && status.State == InFlightState
                && status.Attempt == 1
                && status.ClaimedAt != null
                && status.CompletedAt == null
            );
        interimStatuses
            .Should()
            .ContainSingle(status =>
                status.HandlerType == HandlerIdOf<LowPriorityAfterNoteHandler>()
                && status.State == PendingState
                && status.Attempt == 0
                && status.ClaimedAt == null
                && status.CompletedAt == null
            );
        sink.AfterCalls.Should().BeEmpty();

        gate.Release();
        await flushTask.WaitAsync(TimeSpan.FromSeconds(5));

        sink.AfterCalls.Should().Equal("after-high-blocking", "after-low");

        var completedStatuses = await LoadHandlerStatusesAsync(result.SweepId);
        completedStatuses.Should().HaveCount(2);
        completedStatuses
            .All(status => status.State == SucceededState && status.Attempt == 1)
            .Should()
            .BeTrue();
    }

    [Fact]
    public async Task FlushAsync_Stale_Owner_Cannot_Settle_A_Newer_Claim()
    {
        var tenantId = Guid.NewGuid();
        var asOf = new DateTimeOffset(2026, 4, 13, 12, 0, 0, TimeSpan.Zero);
        var gate = new DispatchBlockGate();
        var sink = new HandlerExecutionSink();

        await using (var db = Host.CreateDbContext())
        {
            db.Notes.Add(
                new Note
                {
                    Id = Guid.NewGuid(),
                    TenantId = tenantId,
                    CreatedAt = asOf.AddDays(-120),
                    Body = "fenced-claim",
                }
            );
            await db.SaveChangesAsync();
        }

        using var handlerHost = new CohortTestHost(
            GetConnectionString(),
            configureServices: services =>
            {
                services.AddSingleton(gate);
                services.AddSingleton(sink);
                services.AddRowHandler<Note, BlockingDispatchNoteHandler>();
            }
        );
        var result = await handlerHost.RunSweepAsync(
            new TenantContext(tenantId, "uk", new Dictionary<string, string>()),
            asOf
        );

        var flushTask = handlerHost.RunWithServicesAsync(async serviceProvider =>
            await serviceProvider.GetRequiredService<IRetentionRowDispatcher>().FlushAsync()
        );
        await gate.WaitUntilBlockedAsync().WaitAsync(TimeSpan.FromSeconds(5));

        // Another dispatcher reclaims the row (as it would once the lease looked expired)
        // while the first delivery is still running.
        await ReclaimInFlightAsync(result.SweepId);
        gate.Release();
        var flush = await flushTask.WaitAsync(TimeSpan.FromSeconds(5));

        // The stale delivery ran, but its success must not settle the newer claim.
        sink.AfterCalls.Should().Equal("after:fenced-claim:1");
        flush.InFlightRemaining.Should().Be(1);
        var status = (await LoadHandlerStatusesAsync(result.SweepId)).Single();
        status.State.Should().Be(InFlightState);
        status.Attempt.Should().Be(2);
        status.CompletedAt.Should().BeNull();
        status.LastError.Should().BeNull();
        (await CountRemainingCapturedPayloadsAsync(result.SweepId)).Should().Be(1);
    }

    [Fact]
    public async Task FlushAsync_Stale_Owner_Failing_Its_Last_Attempt_Cannot_Dead_Letter_The_Newer_Claim_Or_Its_Siblings()
    {
        var tenantId = Guid.NewGuid();
        var asOf = new DateTimeOffset(2026, 4, 13, 12, 0, 0, TimeSpan.Zero);
        var gate = new DispatchBlockGate();
        var sink = new HandlerExecutionSink();

        await using (var db = Host.CreateDbContext())
        {
            db.Notes.Add(
                new Note
                {
                    Id = Guid.NewGuid(),
                    TenantId = tenantId,
                    CreatedAt = asOf.AddDays(-120),
                    Body = "fenced-failure",
                }
            );
            await db.SaveChangesAsync();
        }

        using var handlerHost = new CohortTestHost(
            GetConnectionString(),
            configurationOverrides: new Dictionary<string, string?>
            {
                [$"{CohortOptions.SectionName}:RowHandlerDispatch:MaxAttempts"] = "1",
            },
            configureServices: services =>
            {
                services.AddSingleton(gate);
                services.AddSingleton(sink);
                services.AddRowHandler<Note, BlockingThenFailingHighPriorityNoteHandler>();
                services.AddRowHandler<Note, LowPriorityAfterNoteHandler>();
            }
        );
        var result = await handlerHost.RunSweepAsync(
            new TenantContext(tenantId, "uk", new Dictionary<string, string>()),
            asOf
        );

        var flushTask = handlerHost.RunWithServicesAsync(async serviceProvider =>
            await serviceProvider.GetRequiredService<IRetentionRowDispatcher>().FlushAsync()
        );
        await gate.WaitUntilBlockedAsync().WaitAsync(TimeSpan.FromSeconds(5));

        await ReclaimInFlightAsync(result.SweepId);
        gate.Release();
        await flushTask.WaitAsync(TimeSpan.FromSeconds(5));

        // The stale delivery failed on what was its last attempt, but the row belongs to the
        // newer claim now: neither it nor the queued sibling may be dead-lettered for it.
        var statuses = await LoadHandlerStatusesAsync(result.SweepId);
        statuses.Should().HaveCount(2);
        statuses[0].State.Should().Be(InFlightState);
        statuses[0].Attempt.Should().Be(2);
        statuses[0].LastError.Should().BeNull();
        statuses[1].State.Should().Be(PendingState);
        statuses[1].CompletedAt.Should().BeNull();
        sink.AfterCalls.Should().BeEmpty();
    }

    [Fact]
    public async Task FlushAsync_Cancels_A_Handler_That_Outlives_ClaimTimeout_And_Counts_It_As_A_Failed_Attempt()
    {
        var tenantId = Guid.NewGuid();
        var asOf = new DateTimeOffset(2026, 4, 13, 12, 0, 0, TimeSpan.Zero);
        var gate = new DispatchBlockGate();
        var sink = new HandlerExecutionSink();

        await using (var db = Host.CreateDbContext())
        {
            db.Notes.Add(
                new Note
                {
                    Id = Guid.NewGuid(),
                    TenantId = tenantId,
                    CreatedAt = asOf.AddDays(-120),
                    Body = "outlives-claim",
                }
            );
            await db.SaveChangesAsync();
        }

        using var handlerHost = new CohortTestHost(
            GetConnectionString(),
            configurationOverrides: new Dictionary<string, string?>
            {
                [$"{CohortOptions.SectionName}:RowHandlerDispatch:ClaimTimeout"] = "00:00:30",
                [$"{CohortOptions.SectionName}:RowHandlerDispatch:MaxAttempts"] = "1",
            },
            configureServices: services =>
            {
                services.AddSingleton(gate);
                services.AddSingleton(sink);
                services.AddRowHandler<Note, BlockingDispatchNoteHandler>();
            }
        );
        var result = await handlerHost.RunSweepAsync(
            new TenantContext(tenantId, "uk", new Dictionary<string, string>()),
            asOf
        );

        // The gate is never released: only the claim timeout can end the handler.
        var flushTask = handlerHost.RunWithServicesAsync(async serviceProvider =>
            await serviceProvider.GetRequiredService<IRetentionRowDispatcher>().FlushAsync()
        );
        await gate.WaitUntilBlockedAsync().WaitAsync(TimeSpan.FromSeconds(5));
        var claimedAt = (await LoadHandlerStatusesAsync(result.SweepId)).Single().ClaimedAt!.Value;
        var flush = await flushTask.WaitAsync(TimeSpan.FromSeconds(60));

        // The delivery gives up before its lease lapses, so no other dispatcher can reclaim
        // the row while this handler may still be running.
        DateTimeOffset.UtcNow.Should().BeBefore(claimedAt.AddSeconds(30));

        sink.AfterCalls.Should().BeEmpty();
        flush.InFlightRemaining.Should().Be(0);
        var status = (await LoadHandlerStatusesAsync(result.SweepId)).Single();
        status.State.Should().Be(DeadLetteredState);
        status.Attempt.Should().Be(1);
        status.ClaimedAt.Should().BeNull();
        status.LastError.Should().Contain("ClaimTimeout");
    }

    [Theory]
    [InlineData(1, SucceededState, 2)]
    [InlineData(10, DeadLetteredState, 10)]
    public async Task FlushAsync_Reclaims_A_Claim_Its_Crashed_Owner_Left_Behind_Until_Attempts_Run_Out(
        int attemptsSpent,
        int expectedState,
        int expectedAttempt
    )
    {
        // A crashed owner holds no session lock, so its expired claim is reclaimed.
        var tenantId = Guid.NewGuid();
        var asOf = new DateTimeOffset(2026, 4, 13, 12, 0, 0, TimeSpan.Zero);
        var sink = new HandlerExecutionSink();

        await using (var db = Host.CreateDbContext())
        {
            db.Notes.Add(
                new Note
                {
                    Id = Guid.NewGuid(),
                    TenantId = tenantId,
                    CreatedAt = asOf.AddDays(-120),
                    Body = "crashed-owner",
                }
            );
            await db.SaveChangesAsync();
        }

        using var handlerHost = new CohortTestHost(
            GetConnectionString(),
            configurationOverrides: new Dictionary<string, string?>
            {
                [$"{CohortOptions.SectionName}:RowHandlerDispatch:MaxAttempts"] = "10",
            },
            configureServices: services =>
            {
                services.AddSingleton(sink);
                services.AddRowHandler<Note, DispatchRecordingNoteHandler>();
            }
        );
        var result = await handlerHost.RunSweepAsync(
            new TenantContext(tenantId, "uk", new Dictionary<string, string>()),
            asOf
        );

        // A dispatcher claimed the row and died: nothing will ever settle it.
        await AbandonClaimAsync(result.SweepId, attemptsSpent, DateTimeOffset.UtcNow.AddMinutes(-6));

        await handlerHost.RunWithServicesAsync(async serviceProvider =>
            await serviceProvider.GetRequiredService<IRetentionRowDispatcher>().FlushAsync()
        );

        var status = (await LoadHandlerStatusesAsync(result.SweepId)).Single();
        status.State.Should().Be(expectedState);
        status.Attempt.Should().Be(expectedAttempt);
        status.ClaimedAt.Should().BeNull();
        sink.AfterCalls.Should().HaveCount(expectedState == SucceededState ? 1 : 0);
    }

    [Fact]
    public async Task Sweep_Continues_Past_A_Batch_Of_Only_Failing_OnBefore_Rows()
    {
        // Regression: with batch size 1, the oldest row failing OnBefore produced a
        // zero-affected batch, which used to stop the entity's loop — every younger row
        // was starved on every sweep, forever.
        var tenantId = Guid.NewGuid();
        var asOf = new DateTimeOffset(2026, 4, 13, 12, 0, 0, TimeSpan.Zero);
        var failingNoteId = Guid.NewGuid();
        var healthyNoteId = Guid.NewGuid();

        await using (var db = Host.CreateDbContext())
        {
            db.Notes.AddRange(
                new Note
                {
                    Id = failingNoteId,
                    TenantId = tenantId,
                    CreatedAt = asOf.AddDays(-300), // oldest: fills the whole first batch
                    Body = SelectivelyFailingNoteHandler.FailingBody,
                },
                new Note
                {
                    Id = healthyNoteId,
                    TenantId = tenantId,
                    CreatedAt = asOf.AddDays(-100),
                    Body = "behind-the-failure",
                }
            );
            await db.SaveChangesAsync();
        }

        using var handlerHost = new CohortTestHost(
            GetConnectionString(),
            configurationOverrides: new Dictionary<string, string?>
            {
                [$"{CohortOptions.SectionName}:SweepBatchSize"] = "1",
            },
            configureServices: services =>
                services.AddRowHandler<Note, SelectivelyFailingNoteHandler>()
        );

        var result = await handlerHost.RunSweepAsync(
            new TenantContext(tenantId, "uk", new Dictionary<string, string>()),
            asOf
        );

        // The skipped row makes the run PartiallyFailed, with a plain reason naming the entity.
        result.EntityFailures.Should().ContainSingle().Which.Should().Be("reason=rows-skipped;entityType=Cohort.Sample.Entities.Note;count=1");
        (await LoadRunStatusAndErrorAsync(result.SweepId))
            .Should()
            .Be((SweepRunStatus.PartiallyFailed, "reason=rows-skipped;entityType=Cohort.Sample.Entities.Note;count=1"));
        result
            .Counts.Should()
            .Contain(
                new EntitySweepCount(
                    typeof(Note),
                    "short-lived",
                    tenantId,
                    Strategy.Purge,
                    1,
                    HeldCount: 0,
                    SkippedCount: 1,
                    NullAnchorCount: 0,
                    RetainedCount: 0,
                    DeferredCount: 0,
                    BlockedCount: 0
                )
            );

        await using (var verify = Host.CreateDbContext())
        {
            (await verify.Notes.AnyAsync(note => note.Id == healthyNoteId)).Should().BeFalse();
            (await verify.Notes.AnyAsync(note => note.Id == failingNoteId)).Should().BeTrue();
        }
    }

    [Fact]
    public async Task Sweep_Skips_Only_The_Row_Whose_OnBefore_Hits_A_Database_Error()
    {
        var tenantId = Guid.NewGuid();
        var asOf = new DateTimeOffset(2026, 4, 13, 12, 0, 0, TimeSpan.Zero);
        var failingNoteId = Guid.NewGuid();
        var healthyNoteId = Guid.NewGuid();

        await using (var db = Host.CreateDbContext())
        {
            db.Notes.AddRange(
                new Note
                {
                    Id = failingNoteId,
                    TenantId = tenantId,
                    CreatedAt = asOf.AddDays(-300),
                    Body = DatabaseTouchingNoteHandler.DivideByZeroBody,
                },
                new Note
                {
                    Id = healthyNoteId,
                    TenantId = tenantId,
                    CreatedAt = asOf.AddDays(-100),
                    Body = "after-the-database-error",
                }
            );
            await db.SaveChangesAsync();
        }

        using var handlerHost = new CohortTestHost(
            GetConnectionString(),
            configureServices: services =>
                services.AddSingleton(new WriteTarget(Guid.Empty)).AddRowHandler<Note, DatabaseTouchingNoteHandler>()
        );

        // Both rows share one batch: the failing statement must not abort it.
        var result = await handlerHost.RunSweepAsync(
            new TenantContext(tenantId, "uk", new Dictionary<string, string>()),
            asOf
        );

        result.EntityFailures.Should().ContainSingle().Which.Should().Be("reason=rows-skipped;entityType=Cohort.Sample.Entities.Note;count=1");
        result.Counts.Should().ContainSingle(count => count.EntityType == typeof(Note))
            .Which.Should().Match<EntitySweepCount>(count => count.Affected == 1 && count.SkippedCount == 1);
        var deadLettered = (await LoadHandlerStatusesAsync(result.SweepId)).Single(status => status.State == DeadLetteredState);
        deadLettered.LastError.Should().MatchRegex("^type=Npgsql\\.PostgresException;code=sqlstate:22012;diagnosticId=[0-9a-f]{32}$");

        await using var verify = Host.CreateDbContext();
        (await verify.Notes.AnyAsync(note => note.Id == healthyNoteId)).Should().BeFalse();
        (await verify.Notes.AnyAsync(note => note.Id == failingNoteId)).Should().BeTrue();
    }

    [Fact]
    public async Task Sweep_Refuses_An_OnBefore_That_Writes_Through_The_Sweep_Transaction()
    {
        var tenantId = Guid.NewGuid();
        var asOf = new DateTimeOffset(2026, 4, 13, 12, 0, 0, TimeSpan.Zero);
        var healthyNoteId = Guid.NewGuid();
        var writingNoteId = Guid.NewGuid();
        var bystanderNoteId = Guid.NewGuid();

        await using (var db = Host.CreateDbContext())
        {
            db.Notes.AddRange(
                // Oldest, so the batch has already deleted it and written its row detail
                // when the writing handler runs: read-only must still be accepted then.
                new Note
                {
                    Id = healthyNoteId,
                    TenantId = tenantId,
                    CreatedAt = asOf.AddDays(-300),
                    Body = "before-the-write",
                },
                new Note
                {
                    Id = writingNoteId,
                    TenantId = tenantId,
                    CreatedAt = asOf.AddDays(-100),
                    Body = DatabaseTouchingNoteHandler.WritingBody,
                },
                // Out of scope (another tenant): only the handler's write could change it.
                new Note
                {
                    Id = bystanderNoteId,
                    TenantId = Guid.NewGuid(),
                    CreatedAt = asOf.AddDays(-1),
                    Body = "bystander",
                }
            );
            await db.SaveChangesAsync();
        }

        using var handlerHost = new CohortTestHost(
            GetConnectionString(),
            configureServices: services =>
                services.AddSingleton(new WriteTarget(bystanderNoteId)).AddRowHandler<Note, DatabaseTouchingNoteHandler>()
        );

        var result = await handlerHost.RunSweepAsync(
            new TenantContext(tenantId, "uk", new Dictionary<string, string>()),
            asOf
        );

        result.EntityFailures.Should().ContainSingle().Which.Should().Be("reason=rows-skipped;entityType=Cohort.Sample.Entities.Note;count=1");
        result.Counts.Should().ContainSingle(count => count.EntityType == typeof(Note))
            .Which.Should().Match<EntitySweepCount>(count => count.Affected == 1 && count.SkippedCount == 1);
        var deadLettered = (await LoadHandlerStatusesAsync(result.SweepId)).Single(status => status.State == DeadLetteredState);
        deadLettered.LastError.Should().MatchRegex("^type=Npgsql\\.PostgresException;code=sqlstate:25006;diagnosticId=[0-9a-f]{32}$");

        await using var verify = Host.CreateDbContext();
        (await verify.Notes.AnyAsync(note => note.Id == healthyNoteId)).Should().BeFalse();
        (await verify.Notes.AnyAsync(note => note.Id == writingNoteId)).Should().BeTrue();
        (await verify.Notes.SingleAsync(note => note.Id == bystanderNoteId)).Body.Should().Be("bystander");
    }

    private const string UnsavedChanges = "^reason=on-before-unsaved-changes$";
    private const string ReadOnlyRefusal = "^type=Npgsql\\.PostgresException;code=sqlstate:25006;diagnosticId=[0-9a-f]{32}$";

    [Theory]
    [InlineData(TrackedGraphHandler.StagingBody, UnsavedChanges, false)]
    [InlineData(TrackedGraphHandler.StagingBody, UnsavedChanges, true)]
    // A write fails read-only, and later rows of the batch still run.
    [InlineData(TrackedGraphHandler.FailedSaveBody, ReadOnlyRefusal, false)]
    [InlineData(TrackedGraphHandler.FailedSaveBody, ReadOnlyRefusal, true)]
    // Change detection itself throws on a changed key: still the handler's unsaved change.
    [InlineData(TrackedGraphHandler.KeyChangingBody, UnsavedChanges, false)]
    [InlineData(TrackedGraphHandler.KeyChangingBody, UnsavedChanges, true)]
    [InlineData(TrackedGraphHandler.EditingBody, UnsavedChanges, false)]
    [InlineData(TrackedGraphHandler.EditingBody, UnsavedChanges, true)]
    // With automatic detection off, only an explicit DetectChanges sees the handler's edit.
    [InlineData(TrackedGraphHandler.UndetectedEditBody, UnsavedChanges, false)]
    [InlineData(TrackedGraphHandler.UndetectedEditBody, UnsavedChanges, true)]
    [InlineData(TrackedGraphHandler.AddingBody, UnsavedChanges, false)]
    [InlineData(TrackedGraphHandler.AddingBody, UnsavedChanges, true)]
    [InlineData(TrackedGraphHandler.ManyToManyBody, UnsavedChanges, false)]
    [InlineData(TrackedGraphHandler.ManyToManyBody, UnsavedChanges, true)]
    // Nothing unsaved: the row is erased.
    [InlineData(TrackedGraphHandler.TrackingReadBody, null, false)]
    [InlineData(TrackedGraphHandler.TrackingReadBody, null, true)]
    [InlineData(TrackedGraphHandler.ClearingBody, null, false)]
    [InlineData(TrackedGraphHandler.ClearingBody, null, true)]
    public async Task A_Caller_Transaction_Erasure_Leaves_The_Callers_Context_As_It_Was_Whatever_An_OnBefore_Does(
        string body,
        string? expectedError,
        bool callerTracks
    )
    {
        await using var database = await TemporaryDatabase.CreateAsync(ConnectionString);
        await using var services = BuildTrackedGraphServices(database.ConnectionString);
        var tenantId = Guid.NewGuid();
        var subjectId = Guid.NewGuid();
        var asOf = new DateTimeOffset(2026, 4, 13, 12, 0, 0, TimeSpan.Zero);
        await SeedTrackedGraphAsync(
            services,
            tenantId,
            subjectId,
            (body, asOf.AddDays(-2)),
            // Later in the batch than the handler's row: it still runs.
            ("healthy", asOf.AddDays(-1))
        );
        var bystanderId = await SeedTrackedGraphBystandersAsync(services, tenantId);

        ErasureResult result;
        await using (var scope = services.CreateAsyncScope())
        {
            var db = scope.ServiceProvider.GetRequiredService<TrackedGraphDbContext>();
            TrackedGraphRecord? bystander = null;
            TrackedGraphFolder? folder = null;
            if (callerTracks)
            {
                bystander = await db.Records.SingleAsync(record => record.Id == bystanderId);
                // Its comments not loaded, so a handler's tracking read of one would fix up into it.
                folder = await db.Folders.Include(item => item.Tags).SingleAsync();
            }

            var trackedBefore = db.ChangeTracker.Entries().Select(entry => entry.Entity).ToArray();
            await using var transaction = await db.Database.BeginTransactionAsync();

            result = await scope.ServiceProvider.GetRequiredService<IRetentionErasureService>().ExecuteAsync(
                RetentionErasureRequest.Tenanted(
                    new TenantContext(tenantId, "uk", new Dictionary<string, string>()),
                    new ErasureScope("person", subjectId),
                    asOf,
                    transaction: ErasureTransaction.Caller
                )
            );

            // The caller's context is exactly as it was: the same entities, unchanged.
            db.ChangeTracker.DetectChanges();
            db.ChangeTracker.HasChanges().Should().BeFalse();
            db.ChangeTracker.Entries().Select(entry => entry.Entity).ToHashSet(ReferenceEqualityComparer.Instance)
                .SetEquals(trackedBefore).Should().BeTrue();
            db.ChangeTracker.Entries().Where(entry => entry.State != EntityState.Unchanged).Should().BeEmpty();
            if (callerTracks)
            {
                bystander!.Id.Should().Be(bystanderId);
                bystander.Body.Should().Be(TrackedGraphHandler.BystanderBody);
                folder!.Comments.Should().BeEmpty();
                folder.Tags.Should().BeEmpty();
            }

            // The caller's own save and commit carry nothing of the handler's.
            await db.SaveChangesAsync();
            await transaction.CommitAsync();
        }

        var skipped = expectedError is null ? 0 : 1;
        result.Counts.Should().ContainSingle(count => count.EntityType == typeof(TrackedGraphRecord))
            .Which.Should().Match<EntitySweepCount>(count => count.Affected == 2 - skipped && count.SkippedCount == skipped);
        if (expectedError is null)
        {
            result.EntityFailures.Should().BeEmpty();
        }
        else
        {
            result.EntityFailures.Should().ContainSingle()
                .Which.Should().Be($"reason=rows-skipped;entityType={typeof(TrackedGraphRecord).FullName};count=1");
            (await LoadHandlerStatusesAsync(result.SweepId, database.ConnectionString))
                .Single(status => status.State == DeadLetteredState)
                .LastError.Should().MatchRegex(expectedError);
        }

        await using var verifyScope = services.CreateAsyncScope();
        var verify = verifyScope.ServiceProvider.GetRequiredService<TrackedGraphDbContext>();
        (await verify.Records.Select(record => record.Body).ToListAsync()).Should().BeEquivalentTo(
            expectedError is null ? [TrackedGraphHandler.BystanderBody] : [body, TrackedGraphHandler.BystanderBody]
        );
        (await verify.Records.AnyAsync(record => record.Id == bystanderId)).Should().BeTrue();
        (await verify.Comments.CountAsync()).Should().Be(1);
        (await verify.Folders.SelectMany(item => item.Tags).CountAsync()).Should().Be(0);
    }

    [Fact]
    public async Task A_Caller_Transaction_Erasures_OnBefore_Reads_Through_Its_Own_Context_Inside_The_Callers_Transaction()
    {
        await using var database = await TemporaryDatabase.CreateAsync(ConnectionString);
        await using var services = BuildTrackedGraphServices(database.ConnectionString);
        var tenantId = Guid.NewGuid();
        var subjectId = Guid.NewGuid();
        var asOf = new DateTimeOffset(2026, 4, 13, 12, 0, 0, TimeSpan.Zero);
        await SeedTrackedGraphAsync(services, tenantId, subjectId);
        var probe = services.GetRequiredService<TrackedGraphProbe>();

        await using (var scope = services.CreateAsyncScope())
        {
            var db = scope.ServiceProvider.GetRequiredService<TrackedGraphDbContext>();
            await using var transaction = await db.Database.BeginTransactionAsync();
            // Saved in the caller's transaction only: a handler outside it couldn't see it.
            db.Records.Add(new TrackedGraphRecord
            {
                Id = Guid.NewGuid(),
                TenantId = tenantId,
                SubjectId = subjectId,
                CreatedAt = asOf.AddDays(-1),
                Body = TrackedGraphHandler.ReadingOwnRowBody,
            });
            await db.SaveChangesAsync();

            var result = await scope.ServiceProvider.GetRequiredService<IRetentionErasureService>().ExecuteAsync(
                RetentionErasureRequest.Tenanted(
                    new TenantContext(tenantId, "uk", new Dictionary<string, string>()),
                    new ErasureScope("person", subjectId),
                    asOf,
                    transaction: ErasureTransaction.Caller
                )
            );

            result.EntityFailures.Should().BeEmpty();
            probe.Contexts.Should().ContainSingle().Which.Should().NotBeSameAs(db);
            await transaction.CommitAsync();
        }

        probe.SawOwnRow.Should().Equal(true);
        await using var verifyScope = services.CreateAsyncScope();
        (await verifyScope.ServiceProvider.GetRequiredService<TrackedGraphDbContext>().Records.CountAsync()).Should().Be(0);
    }

    [Fact]
    public async Task An_Owned_Sweep_Fails_Only_The_Row_Whose_OnBefore_Left_A_Navigation_Change()
    {
        await using var database = await TemporaryDatabase.CreateAsync(ConnectionString);
        await using var services = BuildTrackedGraphServices(database.ConnectionString);
        var tenantId = Guid.NewGuid();
        var asOf = new DateTimeOffset(2026, 4, 13, 12, 0, 0, TimeSpan.Zero);
        await SeedTrackedGraphAsync(
            services,
            tenantId,
            subjectId: null,
            (TrackedGraphHandler.AddingBody, asOf.AddDays(-300)),
            ("healthy-1", asOf.AddDays(-200)),
            ("healthy-2", asOf.AddDays(-100))
        );

        var result = await services.GetRequiredService<IRetentionSweep>().ExecuteAsync(
            RetentionSweepRequest.Tenanted(new TenantContext(tenantId, "uk", new Dictionary<string, string>()), asOf)
        );

        result.EntityFailures.Should().ContainSingle()
            .Which.Should().Be($"reason=rows-skipped;entityType={typeof(TrackedGraphRecord).FullName};count=1");
        result.Counts.Should().ContainSingle(count => count.EntityType == typeof(TrackedGraphRecord))
            .Which.Should().Match<EntitySweepCount>(count => count.Affected == 2 && count.SkippedCount == 1);

        await using var verifyScope = services.CreateAsyncScope();
        var verify = verifyScope.ServiceProvider.GetRequiredService<TrackedGraphDbContext>();
        (await verify.Comments.CountAsync()).Should().Be(0);
        (await verify.Records.Select(record => record.Body).ToListAsync())
            .Should().BeEquivalentTo([TrackedGraphHandler.AddingBody]);
    }

    [Fact]
    public async Task A_Handlers_Own_Cancellation_Skips_Its_Row_Instead_Of_Failing_The_Run()
    {
        var tenantId = Guid.NewGuid();
        var asOf = new DateTimeOffset(2026, 4, 13, 12, 0, 0, TimeSpan.Zero);
        var synchronousId = Guid.NewGuid();
        var asynchronousId = Guid.NewGuid();
        var healthyId = Guid.NewGuid();

        await using (var db = Host.CreateDbContext())
        {
            db.Notes.AddRange(
                new Note { Id = synchronousId, TenantId = tenantId, CreatedAt = asOf.AddDays(-300), Body = TimingOutNoteHandler.SynchronousBody },
                new Note { Id = asynchronousId, TenantId = tenantId, CreatedAt = asOf.AddDays(-200), Body = TimingOutNoteHandler.AsynchronousBody },
                new Note { Id = healthyId, TenantId = tenantId, CreatedAt = asOf.AddDays(-100), Body = "healthy" }
            );
            await db.SaveChangesAsync();
        }

        using var handlerHost = new CohortTestHost(
            GetConnectionString(),
            configureServices: services => services.AddRowHandler<Note, TimingOutNoteHandler>()
        );

        // An HttpClient timeout is a TaskCanceledException, but the sweep was not cancelled.
        var result = await handlerHost.RunSweepAsync(
            new TenantContext(tenantId, "uk", new Dictionary<string, string>()),
            asOf
        );

        result.EntityFailures.Should().ContainSingle().Which.Should().Be("reason=rows-skipped;entityType=Cohort.Sample.Entities.Note;count=2");
        result.Counts.Should().ContainSingle(count => count.EntityType == typeof(Note))
            .Which.Should().Match<EntitySweepCount>(count => count.Affected == 1 && count.SkippedCount == 2);
        (await LoadHandlerStatusesAsync(result.SweepId)).Where(status => status.State == DeadLetteredState)
            .Should().HaveCount(2)
            .And.OnlyContain(status => status.LastError!.StartsWith("type=System.Threading.Tasks.TaskCanceledException;", StringComparison.Ordinal));

        await using var verify = Host.CreateDbContext();
        (await verify.Notes.Where(note => note.TenantId == tenantId).Select(note => note.Id).ToListAsync())
            .Should().BeEquivalentTo([synchronousId, asynchronousId]);
    }

    [Fact]
    public async Task OnBefore_Can_Read_Related_Rows_On_The_Sweep_Transaction()
    {
        var tenantId = Guid.NewGuid();
        var asOf = new DateTimeOffset(2026, 4, 13, 12, 0, 0, TimeSpan.Zero);
        var sweptNoteId = Guid.NewGuid();
        var relatedNoteId = Guid.NewGuid();
        var recorder = new CollectionSnapshotRecorder();

        await using (var db = Host.CreateDbContext())
        {
            db.Notes.AddRange(
                new Note
                {
                    Id = sweptNoteId,
                    TenantId = tenantId,
                    CreatedAt = asOf.AddDays(-100),
                    Body = DatabaseTouchingNoteHandler.ReadingBody,
                },
                // Too young to sweep: the handler looks it up while the swept row is locked.
                new Note
                {
                    Id = relatedNoteId,
                    TenantId = tenantId,
                    CreatedAt = asOf.AddDays(-1),
                    Body = "related-body",
                }
            );
            await db.SaveChangesAsync();
        }

        using var handlerHost = new CohortTestHost(
            GetConnectionString(),
            configureServices: services =>
                services
                    .AddSingleton(new WriteTarget(relatedNoteId))
                    .AddSingleton(recorder)
                    .AddRowHandler<Note, DatabaseTouchingNoteHandler>()
        );

        var result = await handlerHost.RunSweepAsync(
            new TenantContext(tenantId, "uk", new Dictionary<string, string>()),
            asOf
        );
        await handlerHost.RunWithServicesAsync(async services =>
            await services.GetRequiredService<IRetentionRowDispatcher>().FlushAsync()
        );

        result.EntityFailures.Should().BeEmpty();
        recorder.Snapshot.Should().NotBeNull();
        recorder.Snapshot!["related"].Should().Be("related-body");
        await using var verify = Host.CreateDbContext();
        (await verify.Notes.AnyAsync(note => note.Id == sweptNoteId)).Should().BeFalse();
    }

    [Fact]
    public async Task A_Live_Erasure_Whose_OnBefore_Skips_A_Row_Is_Partially_Failed_And_Not_Complete()
    {
        var tenantId = Guid.NewGuid();
        var subjectId = Guid.NewGuid();
        var asOf = new DateTimeOffset(2026, 4, 13, 12, 0, 0, TimeSpan.Zero);
        var failingNoteId = Guid.NewGuid();
        var erasedNoteId = Guid.NewGuid();

        await using (var db = Host.CreateDbContext())
        {
            db.Notes.AddRange(
                new Note
                {
                    Id = failingNoteId,
                    TenantId = tenantId,
                    SubjectId = subjectId,
                    CreatedAt = asOf.AddDays(-2),
                    Body = SelectivelyFailingNoteHandler.FailingBody,
                },
                new Note
                {
                    Id = erasedNoteId,
                    TenantId = tenantId,
                    SubjectId = subjectId,
                    CreatedAt = asOf.AddDays(-1),
                    Body = "erase-me",
                }
            );
            await db.SaveChangesAsync();
        }

        using var handlerHost = new CohortTestHost(
            GetConnectionString(),
            configureServices: services =>
                services.AddRowHandler<Note, SelectivelyFailingNoteHandler>()
        );

        var result = await handlerHost.RunErasureAsync(
            new TenantContext(tenantId, "uk", new Dictionary<string, string>()),
            new ErasureScope("user", subjectId, allowSoftDeleteAsErasure: true),
            asOf
        );

        const string reason = "reason=rows-skipped;entityType=Cohort.Sample.Entities.Note;count=1";
        result.EntityFailures.Should().ContainSingle().Which.Should().Be(reason);
        result.Complete.Should().BeFalse();
        result.Counts.Should().ContainSingle(count => count.EntityType == typeof(Note))
            .Which.Should().Match<EntitySweepCount>(count => count.Affected == 1 && count.SkippedCount == 1 && count.DeferredCount == 0);
        (await LoadRunStatusAndErrorAsync(result.SweepId))
            .Should()
            .Be((SweepRunStatus.PartiallyFailed, reason));

        await using var verify = Host.CreateDbContext();
        (await verify.Notes.Where(note => note.SubjectId == subjectId).Select(note => note.Id).ToListAsync())
            .Should()
            .Equal(failingNoteId);
    }

    [Fact]
    public async Task Live_Erasure_With_Handlers_Captures_Target_Rows_And_Skips_Held_And_Exempt_Work()
    {
        var tenantId = Guid.NewGuid();
        var subjectId = Guid.NewGuid();
        var asOf = new DateTimeOffset(2026, 4, 13, 12, 0, 0, TimeSpan.Zero);
        var noteId = Guid.NewGuid();
        var heldNoteId = Guid.NewGuid();
        var softDeleteId = Guid.NewGuid();
        var heldSoftDeleteId = Guid.NewGuid();
        var contactId = Guid.NewGuid();
        var heldContactId = Guid.NewGuid();
        var exemptRecordId = Guid.NewGuid();
        var sink = new HandlerExecutionSink();

        await using (var db = Host.CreateDbContext())
        {
            db.Notes.AddRange(
                new Note
                {
                    Id = noteId,
                    TenantId = tenantId,
                    SubjectId = subjectId,
                    CreatedAt = asOf.AddDays(-1),
                    Body = "erase-note",
                },
                new Note
                {
                    Id = heldNoteId,
                    TenantId = tenantId,
                    SubjectId = subjectId,
                    CreatedAt = asOf.AddDays(-1),
                    Body = "held-note",
                }
            );
            db.SoftDeleteRecords.AddRange(
                new SoftDeleteRecord
                {
                    Id = softDeleteId,
                    TenantId = tenantId,
                    SubjectId = subjectId,
                    CreatedAt = asOf.AddDays(-1),
                    Body = "erase-soft-delete",
                    IsDeleted = false,
                },
                new SoftDeleteRecord
                {
                    Id = heldSoftDeleteId,
                    TenantId = tenantId,
                    SubjectId = subjectId,
                    CreatedAt = asOf.AddDays(-1),
                    Body = "held-soft-delete",
                    IsDeleted = false,
                }
            );
            db.AnonymisedContacts.AddRange(
                new AnonymisedContact
                {
                    Id = contactId,
                    TenantId = tenantId,
                    SubjectId = subjectId,
                    CreatedAt = asOf.AddDays(-1),
                    EmailAddress = "subject@example.com",
                    GivenName = "Target",
                    Surname = "Contact",
                    Notes = "keep-notes",
                },
                new AnonymisedContact
                {
                    Id = heldContactId,
                    TenantId = tenantId,
                    SubjectId = subjectId,
                    CreatedAt = asOf.AddDays(-1),
                    EmailAddress = "held@example.com",
                    GivenName = "Held",
                    Surname = "Contact",
                    Notes = "held-notes",
                }
            );
            db.ErasureSubjectRecords.Add(
                new ErasureSubjectRecord
                {
                    Id = exemptRecordId,
                    TenantId = tenantId,
                    SubjectId = subjectId,
                    CreatedAt = asOf.AddDays(-1),
                    Body = "exempt-erasure-record",
                }
            );
            await db.SaveChangesAsync();
        }

        await CreateHoldAsync("notes", heldNoteId, tenantId, asOf);
        await CreateHoldAsync("soft_delete_records", heldSoftDeleteId, tenantId, asOf);
        await CreateHoldAsync("anonymised_contacts", heldContactId, tenantId, asOf);

        using var handlerHost = new CohortTestHost(
            GetConnectionString(),
            CreateHandlerErasureCategoryRepository(),
            configurationOverrides: new Dictionary<string, string?>
            {
                [$"{CohortOptions.SectionName}:RowHandlerDispatch:MaxParallelism"] = "1",
            },
            configureServices: services =>
            {
                services.AddSingleton(sink);
                services.AddRowHandler<Note, NoteErasureTrackingHandler>();
                services.AddRowHandler<SoftDeleteRecord, SoftDeleteErasureTrackingHandler>();
                services.AddRowHandler<
                    AnonymisedContact,
                    AnonymisedContactErasureTrackingHandler
                >();
            }
        );

        var result = await handlerHost.RunErasureAsync(
            new TenantContext(tenantId, "uk", new Dictionary<string, string>()),
            new ErasureScope("user", subjectId, allowSoftDeleteAsErasure: true),
            asOf
        );

        result
            .Counts.Should()
            .Contain(
                new EntitySweepCount(
                    typeof(Note),
                    "short-lived",
                    tenantId,
                    Strategy.Purge,
                    1,
                    HeldCount: 1,
                    SkippedCount: 0,
                    NullAnchorCount: 0,
                    RetainedCount: 0,
                    DeferredCount: 0,
                    BlockedCount: 0
                )
            );
        result
            .Counts.Should()
            .Contain(
                new EntitySweepCount(
                    typeof(SoftDeleteRecord),
                    "soft-delete",
                    tenantId,
                    Strategy.SoftDelete,
                    1,
                    HeldCount: 1,
                    SkippedCount: 0,
                    NullAnchorCount: 0,
                    RetainedCount: 0,
                    DeferredCount: 0,
                    BlockedCount: 0
                )
            );
        result
            .Counts.Should()
            .Contain(
                // Held counts are measured directly (subject-matching, past cutoff, actively
                // held), so the anonymise erase path reports the held contact too.
                new EntitySweepCount(
                    typeof(AnonymisedContact),
                    "anonymise",
                    tenantId,
                    Strategy.Anonymise,
                    1,
                    HeldCount: 1,
                    SkippedCount: 0,
                    NullAnchorCount: 0,
                    RetainedCount: 0,
                    DeferredCount: 0,
                    BlockedCount: 0
                )
            );
        sink.BeforeCalls.Should()
            .BeEquivalentTo([
                "note:erase-note",
                "soft:erase-soft-delete",
                "contact:subject@example.com",
            ]);

        await using (var verify = Host.CreateDbContext())
        {
            (await verify.Notes.AnyAsync(note => note.Id == noteId)).Should().BeFalse();
            (await verify.Notes.AnyAsync(note => note.Id == heldNoteId)).Should().BeTrue();

            var softDelete = await verify.SoftDeleteRecords.SingleAsync(record =>
                record.Id == softDeleteId
            );
            softDelete.IsDeleted.Should().BeTrue();
            softDelete.DeletedAt.Should().Be(asOf);
            (await verify.SoftDeleteRecords.SingleAsync(record => record.Id == heldSoftDeleteId))
                .IsDeleted.Should()
                .BeFalse();

            var contact = await verify.AnonymisedContacts.SingleAsync(candidate =>
                candidate.Id == contactId
            );
            contact.EmailAddress.Should().BeNull();
            contact.GivenName.Should().BeEmpty();
            contact.Surname.Should().Be("[redacted]");
            (
                await verify.AnonymisedContacts.SingleAsync(candidate =>
                    candidate.Id == heldContactId
                )
            )
                .EmailAddress.Should()
                .Be("held@example.com");

            (await verify.ErasureSubjectRecords.SingleAsync(record => record.Id == exemptRecordId))
                .Body.Should()
                .Be("exempt-erasure-record");
        }

        var rowDetails = await LoadCapturedRowsAsync(result.SweepId);
        rowDetails.Should().HaveCount(3);
        rowDetails
            .Select(row => row.RecordId)
            .Should()
            .BeEquivalentTo([noteId.ToString(), softDeleteId.ToString(), contactId.ToString()]);

        var statuses = await LoadHandlerStatusesAsync(result.SweepId);
        statuses.Should().HaveCount(3);
        statuses.All(status => status.State == 0 && status.Attempt == 0).Should().BeTrue();
        statuses
            .Select(status => status.HandlerType)
            .Should()
            .Contain(type =>
                type == HandlerIdOf<NoteErasureTrackingHandler>()
            );
        statuses
            .Select(status => status.HandlerType)
            .Should()
            .Contain(type =>
                type == HandlerIdOf<SoftDeleteErasureTrackingHandler>()
            );
        statuses
            .Select(status => status.HandlerType)
            .Should()
            .Contain(type =>
                type == HandlerIdOf<AnonymisedContactErasureTrackingHandler>()
            );

        await handlerHost.RunWithServicesAsync(async serviceProvider =>
        {
            var dispatcher = serviceProvider.GetRequiredService<IRetentionRowDispatcher>();
            await dispatcher.FlushAsync();
        });

        sink.AfterCalls.Should()
            .BeEquivalentTo([
                "after-note:erase-note:1",
                "after-soft:erase-soft-delete:1",
                "after-contact:subject@example.com:1",
            ]);
        var completedStatuses = await LoadHandlerStatusesAsync(result.SweepId);
        completedStatuses.Should().HaveCount(3);
        completedStatuses.All(status => status.State == SucceededState).Should().BeTrue();
        completedStatuses.All(status => status.Attempt == 1).Should().BeTrue();
        completedStatuses.All(status => status.CompletedAt is not null).Should().BeTrue();
        completedStatuses.All(status => status.LastError is null).Should().BeTrue();

        var summaries = await LoadEntitySummariesAsync(result.SweepId);
        summaries
            .Should()
            .Contain(new EntitySummaryRow(typeof(Note).FullName!, Strategy.Purge, 1, 1, 0));
        summaries
            .Should()
            .Contain(
                new EntitySummaryRow(
                    typeof(SoftDeleteRecord).FullName!,
                    Strategy.SoftDelete,
                    1,
                    1,
                    0
                )
            );
        summaries
            .Should()
            .Contain(
                new EntitySummaryRow(
                    typeof(AnonymisedContact).FullName!,
                    Strategy.Anonymise,
                    1,
                    1,
                    0
                )
            );
    }

    [Fact]
    public async Task DryRun_Erasure_With_Handlers_Does_Not_Invoke_Handlers_Or_Persist_Handler_Work()
    {
        var tenantId = Guid.NewGuid();
        var subjectId = Guid.NewGuid();
        var asOf = new DateTimeOffset(2026, 4, 13, 12, 0, 0, TimeSpan.Zero);
        var noteId = Guid.NewGuid();
        var softDeleteId = Guid.NewGuid();
        var contactId = Guid.NewGuid();
        var exemptRecordId = Guid.NewGuid();
        var sink = new HandlerExecutionSink();

        await using (var db = Host.CreateDbContext())
        {
            db.Notes.Add(
                new Note
                {
                    Id = noteId,
                    TenantId = tenantId,
                    SubjectId = subjectId,
                    CreatedAt = EligibleErasureCreatedAt(asOf),
                    Body = "dry-run-note",
                }
            );
            db.SoftDeleteRecords.Add(
                new SoftDeleteRecord
                {
                    Id = softDeleteId,
                    TenantId = tenantId,
                    SubjectId = subjectId,
                    CreatedAt = EligibleErasureCreatedAt(asOf),
                    Body = "dry-run-soft-delete",
                    IsDeleted = false,
                }
            );
            db.AnonymisedContacts.Add(
                new AnonymisedContact
                {
                    Id = contactId,
                    TenantId = tenantId,
                    SubjectId = subjectId,
                    CreatedAt = EligibleErasureCreatedAt(asOf),
                    EmailAddress = "dry-run@example.com",
                    GivenName = "Dry",
                    Surname = "Run",
                    Notes = "keep-me",
                }
            );
            db.ErasureSubjectRecords.Add(
                new ErasureSubjectRecord
                {
                    Id = exemptRecordId,
                    TenantId = tenantId,
                    SubjectId = subjectId,
                    CreatedAt = EligibleErasureCreatedAt(asOf),
                    Body = "dry-run-exempt",
                }
            );
            await db.SaveChangesAsync();
        }

        using var handlerHost = new CohortTestHost(
            GetConnectionString(),
            CreateHandlerErasureCategoryRepository(),
            configurationOverrides: null,
            services =>
            {
                services.AddSingleton(sink);
                services.AddRowHandler<Note, NoteErasureTrackingHandler>();
                services.AddRowHandler<SoftDeleteRecord, SoftDeleteErasureTrackingHandler>();
                services.AddRowHandler<
                    AnonymisedContact,
                    AnonymisedContactErasureTrackingHandler
                >();
            }
        );

        var result = await handlerHost.RunErasureAsync(
            new TenantContext(tenantId, "uk", new Dictionary<string, string>()),
            new ErasureScope("user", subjectId, allowSoftDeleteAsErasure: true, dryRun: true),
            asOf
        );

        result
            .Counts.Should()
            .Contain(
                new EntitySweepCount(
                    typeof(Note),
                    "short-lived",
                    tenantId,
                    Strategy.Purge,
                    1,
                    HeldCount: 0,
                    SkippedCount: 0,
                    NullAnchorCount: 0,
                    RetainedCount: 0,
                    DeferredCount: 0,
                    BlockedCount: 0
                )
            );
        result
            .Counts.Should()
            .Contain(
                new EntitySweepCount(
                    typeof(SoftDeleteRecord),
                    "soft-delete",
                    tenantId,
                    Strategy.SoftDelete,
                    1,
                    HeldCount: 0,
                    SkippedCount: 0,
                    NullAnchorCount: 0,
                    RetainedCount: 0,
                    DeferredCount: 0,
                    BlockedCount: 0
                )
            );
        result
            .Counts.Should()
            .Contain(
                new EntitySweepCount(
                    typeof(AnonymisedContact),
                    "anonymise",
                    tenantId,
                    Strategy.Anonymise,
                    1,
                    HeldCount: 0,
                    SkippedCount: 0,
                    NullAnchorCount: 0,
                    RetainedCount: 0,
                    DeferredCount: 0,
                    BlockedCount: 0
                )
            );
        sink.BeforeCalls.Should().BeEmpty();
        (await LoadCapturedRowsAsync(result.SweepId)).Should().BeEmpty();
        (await LoadHandlerStatusesAsync(result.SweepId)).Should().BeEmpty();

        await handlerHost.RunWithServicesAsync(async serviceProvider =>
        {
            var dispatcher = serviceProvider.GetRequiredService<IRetentionRowDispatcher>();
            await dispatcher.FlushAsync();
        });

        sink.AfterCalls.Should().BeEmpty();

        await using var verify = Host.CreateDbContext();
        (await verify.Notes.AnyAsync(note => note.Id == noteId)).Should().BeTrue();
        (await verify.SoftDeleteRecords.SingleAsync(record => record.Id == softDeleteId))
            .IsDeleted.Should()
            .BeFalse();
        (await verify.AnonymisedContacts.SingleAsync(candidate => candidate.Id == contactId))
            .EmailAddress.Should()
            .Be("dry-run@example.com");
        (await verify.ErasureSubjectRecords.SingleAsync(record => record.Id == exemptRecordId))
            .Body.Should()
            .Be("dry-run-exempt");
    }

    [Fact]
    public async Task FlushAsync_Claims_Each_Queued_Status_At_Most_Once_When_Called_Concurrently()
    {
        var tenantId = Guid.NewGuid();
        var asOf = new DateTimeOffset(2026, 4, 13, 12, 0, 0, TimeSpan.Zero);
        var noteId = Guid.NewGuid();
        var sink = new HandlerExecutionSink();
        var gate = new DispatchBlockGate();

        await using (var db = Host.CreateDbContext())
        {
            db.Notes.Add(
                new Note
                {
                    Id = noteId,
                    TenantId = tenantId,
                    CreatedAt = asOf.AddDays(-120),
                    Body = "concurrent-dispatch",
                }
            );
            await db.SaveChangesAsync();
        }

        using var handlerHost = new CohortTestHost(
            GetConnectionString(),
            configureServices: services =>
            {
                services.AddSingleton(sink);
                services.AddSingleton(gate);
                services.AddRowHandler<Note, BlockingDispatchNoteHandler>();
            }
        );

        var result = await handlerHost.RunSweepAsync(
            new TenantContext(tenantId, "uk", new Dictionary<string, string>()),
            asOf
        );

        await handlerHost.RunWithServicesAsync(async serviceProvider =>
        {
            var dispatcher = serviceProvider.GetRequiredService<IRetentionRowDispatcher>();
            var owningFlush = dispatcher.FlushAsync();
            await gate.WaitUntilBlockedAsync().WaitAsync(TimeSpan.FromSeconds(5));

            var claimedStatus = (await LoadHandlerStatusesAsync(result.SweepId)).Single();
            claimedStatus.State.Should().Be(InFlightState);
            claimedStatus.Attempt.Should().Be(1);
            claimedStatus.ClaimedAt.Should().NotBeNull();

            var competingResult = await dispatcher.FlushAsync().WaitAsync(TimeSpan.FromSeconds(5));
            competingResult.Settled.Should().BeFalse();
            competingResult.InFlightRemaining.Should().Be(1);
            competingResult.PendingRemaining.Should().Be(0);
            sink.AfterCalls.Should().BeEmpty();

            gate.Release();
            await owningFlush.WaitAsync(TimeSpan.FromSeconds(5));
        });

        sink.AfterCalls.Should().Equal("after:concurrent-dispatch:1");

        var statuses = await LoadHandlerStatusesAsync(result.SweepId);
        statuses.Should().ContainSingle();
        statuses[0].State.Should().Be(SucceededState);
        statuses[0].Attempt.Should().Be(1);
        statuses[0].ClaimedAt.Should().BeNull();
        statuses[0].CompletedAt.Should().NotBeNull();
        statuses[0].LastError.Should().BeNull();
    }

    [Fact]
    public async Task FlushAsync_Claims_Only_What_It_Can_Run_And_Cancellation_Requeues_That_Claim()
    {
        var tenantId = Guid.NewGuid();
        var asOf = new DateTimeOffset(2026, 4, 13, 12, 0, 0, TimeSpan.Zero);
        var firstNoteId = Guid.NewGuid();
        var secondNoteId = Guid.NewGuid();
        var sink = new HandlerExecutionSink();
        var gate = new DispatchBlockGate();

        await using (var db = Host.CreateDbContext())
        {
            db.Notes.AddRange(
                new Note
                {
                    Id = firstNoteId,
                    TenantId = tenantId,
                    CreatedAt = asOf.AddDays(-120),
                    Body = "cancelled-batch-first",
                },
                new Note
                {
                    Id = secondNoteId,
                    TenantId = tenantId,
                    CreatedAt = asOf.AddDays(-120),
                    Body = "cancelled-batch-second",
                }
            );
            await db.SaveChangesAsync();
        }

        using var handlerHost = new CohortTestHost(
            GetConnectionString(),
            configurationOverrides: new Dictionary<string, string?>
            {
                [$"{CohortOptions.SectionName}:RowHandlerDispatch:BatchSize"] = "10",
                [$"{CohortOptions.SectionName}:RowHandlerDispatch:MaxParallelism"] = "1",
            },
            configureServices: services =>
            {
                services.AddSingleton(sink);
                services.AddSingleton(gate);
                services.AddRowHandler<Note, BlockingDispatchNoteHandler>();
            }
        );

        var result = await handlerHost.RunSweepAsync(
            new TenantContext(tenantId, "uk", new Dictionary<string, string>()),
            asOf
        );

        using var cancellation = new CancellationTokenSource();
        await handlerHost.RunWithServicesAsync(async serviceProvider =>
        {
            var dispatcher = serviceProvider.GetRequiredService<IRetentionRowDispatcher>();
            var flushTask = dispatcher.FlushAsync(cancellation.Token);

            await gate.WaitUntilBlockedAsync();
            cancellation.Cancel();

            await Assert.ThrowsAnyAsync<OperationCanceledException>(() => flushTask);
        });

        gate.Release();

        await handlerHost.RunWithServicesAsync(async serviceProvider =>
        {
            var dispatcher = serviceProvider.GetRequiredService<IRetentionRowDispatcher>();
            await dispatcher.FlushAsync();
        });

        // MaxParallelism 1 claims one row at a time despite BatchSize 10: only the row that
        // was running when the flush was cancelled spent an attempt; the other was never
        // claimed, so it could not sit behind it while its lease ran out.
        sink.AfterCalls.Select(call => call[call.LastIndexOf(':')..])
            .Should()
            .BeEquivalentTo([":2", ":1"]);

        var statuses = await LoadHandlerStatusesAsync(result.SweepId);
        statuses.Should().HaveCount(2);
        statuses.All(status => status.State == SucceededState).Should().BeTrue();
        statuses.Select(status => status.Attempt).Should().BeEquivalentTo([2, 1]);
        statuses.All(status => status.CompletedAt is not null).Should().BeTrue();
    }

    [Fact]
    public async Task FlushAsync_A_Failure_Recording_A_Handler_Outcome_Neither_Aborts_The_Batch_Nor_Spends_Sibling_Attempts()
    {
        var tenantId = Guid.NewGuid();
        var asOf = new DateTimeOffset(2026, 4, 13, 12, 0, 0, TimeSpan.Zero);
        var sink = new HandlerExecutionSink();
        var gate = new DispatchBlockGate();

        await using (var db = Host.CreateDbContext())
        {
            db.Notes.AddRange(
                new Note { Id = Guid.NewGuid(), TenantId = tenantId, CreatedAt = asOf.AddDays(-120), Body = "fails" },
                new Note { Id = Guid.NewGuid(), TenantId = tenantId, CreatedAt = asOf.AddDays(-120), Body = "blocks" }
            );
            await db.SaveChangesAsync();
        }

        using var handlerHost = new CohortTestHost(
            GetConnectionString(),
            configurationOverrides: new Dictionary<string, string?>
            {
                [$"{CohortOptions.SectionName}:RowHandlerDispatch:MaxParallelism"] = "2",
            },
            configureServices: services =>
            {
                services.AddSingleton(sink);
                services.AddSingleton(gate);
                services.AddRowHandler<Note, FailsWhileSiblingBlocksNoteHandler>();
            }
        );
        var result = await handlerHost.RunSweepAsync(
            new TenantContext(tenantId, "uk", new Dictionary<string, string>()),
            asOf
        );

        // A transient database failure hits the write that records the handler's failure.
        await ExecuteAsync("""
            CREATE FUNCTION public.cohort_test_fail_recording() RETURNS trigger LANGUAGE plpgsql
            AS $$ BEGIN RAISE EXCEPTION 'simulated transient failure'; END $$;
            CREATE TRIGGER cohort_test_fail_recording
            BEFORE UPDATE ON public.sweep_row_handler_status
            FOR EACH ROW WHEN (NEW."State" = 0 AND NEW."LastError" IS NOT NULL)
            EXECUTE FUNCTION public.cohort_test_fail_recording();
            """);
        try
        {
            var flushTask = handlerHost.RunWithServicesAsync(async serviceProvider =>
                await serviceProvider.GetRequiredService<IRetentionRowDispatcher>().FlushAsync()
            );
            await gate.WaitUntilBlockedAsync().WaitAsync(TimeSpan.FromSeconds(5));
            // Give the failing sibling time to finish and hit the recording failure.
            await Task.WhenAny(flushTask, Task.Delay(TimeSpan.FromSeconds(1)));
            gate.Release();
            await flushTask.WaitAsync(TimeSpan.FromSeconds(10));
        }
        finally
        {
            await ExecuteAsync("""
                DROP TRIGGER IF EXISTS cohort_test_fail_recording ON public.sweep_row_handler_status;
                DROP FUNCTION IF EXISTS public.cohort_test_fail_recording();
                """);
        }

        // The blocked sibling was not cancelled: it completed on its first attempt.
        sink.AfterCalls.Should().Equal("after:blocks:1");
        var statuses = await LoadHandlerStatusesAsync(result.SweepId);
        statuses.Should().HaveCount(2);
        statuses.Should().ContainSingle(status => status.State == SucceededState && status.Attempt == 1);
        // The failed delivery stays claimed with its attempt spent; its lease recovers it.
        statuses.Should().ContainSingle(status =>
            status.State == InFlightState && status.Attempt == 1 && status.LastError == null
        );
    }

    private async Task ExecuteAsync(string sql)
    {
        await using var connection = new NpgsqlConnection(GetConnectionString());
        await connection.OpenAsync();
        await using var command = connection.CreateCommand();
        command.CommandText = sql;
        await command.ExecuteNonQueryAsync();
    }

    [Fact]
    public async Task FlushAsync_Rebuilds_Full_AfterContext_From_Persisted_Row_Detail_Using_A_Fresh_Scope_Per_Dispatch()
    {
        var tenantId = Guid.NewGuid();
        var asOf = new DateTimeOffset(2026, 4, 13, 12, 0, 0, TimeSpan.Zero);
        var firstNoteId = Guid.NewGuid();
        var secondNoteId = Guid.NewGuid();
        var recorder = new AfterContextRecorder();

        await using (var db = Host.CreateDbContext())
        {
            db.Notes.AddRange(
                new Note
                {
                    Id = firstNoteId,
                    TenantId = tenantId,
                    CreatedAt = asOf.AddDays(-120),
                    Body = "after-context-first",
                },
                new Note
                {
                    Id = secondNoteId,
                    TenantId = tenantId,
                    CreatedAt = asOf.AddDays(-120),
                    Body = "after-context-second",
                }
            );
            await db.SaveChangesAsync();
        }

        using var handlerHost = new CohortTestHost(
            GetConnectionString(),
            configurationOverrides: new Dictionary<string, string?>
            {
                [$"{CohortOptions.SectionName}:RowHandlerDispatch:MaxParallelism"] = "1",
            },
            configureServices: services =>
            {
                services.AddSingleton(recorder);
                services.AddScoped<ScopedDispatchProbe>();
                services.AddRowHandler<Note, ContextCapturingNoteHandler>();
            }
        );

        var result = await handlerHost.RunSweepAsync(
            new TenantContext(tenantId, "uk", new Dictionary<string, string>()),
            asOf
        );

        // Captured payloads are cleared once a row's handler work settles, so the
        // persisted snapshots must be read before the flush.
        var persistedRows = await LoadCapturedRowDetailsAsync(result.SweepId);

        await handlerHost.RunWithServicesAsync(async serviceProvider =>
        {
            var dispatcher = serviceProvider.GetRequiredService<IRetentionRowDispatcher>();
            await dispatcher.FlushAsync();
        });

        (await CountRemainingCapturedPayloadsAsync(result.SweepId)).Should().Be(0);

        var observed = recorder
            .Load()
            .OrderBy(call => call.RecordId, StringComparer.Ordinal)
            .ToArray();

        observed.Should().HaveCount(2);
        observed.Select(call => call.ScopeInstanceId).Should().OnlyHaveUniqueItems();

        foreach (var call in observed)
        {
            var persisted = persistedRows.Single(row => row.RecordId == call.RecordId);
            call.SweepId.Should().Be(result.SweepId);
            call.Category.Should().Be(persisted.Category);
            call.Strategy.Should().Be(persisted.Strategy);
            call.TenantId.Should().Be(persisted.TenantId);
            call.At.Should().Be(persisted.At);
            call.Attempt.Should().Be(1);
            call.Body.Should().Be(persisted.Body);
        }
    }

    [Fact]
    public async Task FlushAsync_Honours_Retry_Backoff_And_Dead_Letters_Exhausted_Failures_Keeping_Their_Snapshot()
    {
        var tenantId = Guid.NewGuid();
        var asOf = new DateTimeOffset(2026, 4, 13, 12, 0, 0, TimeSpan.Zero);
        var noteId = Guid.NewGuid();
        var tracker = new DispatchAttemptTracker();

        await using (var db = Host.CreateDbContext())
        {
            db.Notes.Add(
                new Note
                {
                    Id = noteId,
                    TenantId = tenantId,
                    CreatedAt = asOf.AddDays(-120),
                    Body = "dead-letter-target",
                }
            );
            await db.SaveChangesAsync();
        }

        using var handlerHost = new CohortTestHost(
            GetConnectionString(),
            configurationOverrides: new Dictionary<string, string?>
            {
                [$"{CohortOptions.SectionName}:RowHandlerDispatch:BaseBackoff"] = "00:05:00",
                [$"{CohortOptions.SectionName}:RowHandlerDispatch:MaxAttempts"] = "2",
            },
            configureServices: services =>
            {
                services.AddSingleton(tracker);
                services.AddRowHandler<Note, AlwaysFailingDispatchNoteHandler>();
            }
        );

        var result = await handlerHost.RunSweepAsync(
            new TenantContext(tenantId, "uk", new Dictionary<string, string>()),
            asOf
        );

        // One flush spends one attempt: the retry waits out its backoff instead of running
        // back-to-back, so a job that flushes during an outage can't exhaust the budget.
        var firstFlush = await handlerHost.RunWithServicesAsync(serviceProvider =>
            serviceProvider.GetRequiredService<IRetentionRowDispatcher>().FlushAsync()
        );
        var failedAt = DateTimeOffset.UtcNow;

        tracker.LoadAttempt(noteId.ToString()).Should().Be(1);
        firstFlush.Should().Be(new RowDispatcherFlushResult(0, 1, 0));
        var waiting = (await LoadHandlerStatusesAsync(result.SweepId)).Should().ContainSingle().Subject;
        waiting.State.Should().Be(PendingState);
        waiting.Attempt.Should().Be(1);
        waiting.NextAttemptAt.Should().BeCloseTo(failedAt.AddMinutes(5), TimeSpan.FromSeconds(30));
        AssertInvalidOperationDiagnostic(waiting.LastError);

        await MakeHandlerWorkDueAsync(result.SweepId);
        var secondFlush = await handlerHost.RunWithServicesAsync(serviceProvider =>
            serviceProvider.GetRequiredService<IRetentionRowDispatcher>().FlushAsync()
        );

        tracker.LoadAttempt(noteId.ToString()).Should().Be(2);
        secondFlush.Should().Be(new RowDispatcherFlushResult(0, 0, 1));
        secondFlush.Settled.Should().BeTrue();
        var statuses = await LoadHandlerStatusesAsync(result.SweepId);
        statuses.Should().ContainSingle();
        statuses[0].State.Should().Be(DeadLetteredState);
        statuses[0].Attempt.Should().Be(2);
        statuses[0].CompletedAt.Should().NotBeNull();
        AssertInvalidOperationDiagnostic(statuses[0].LastError);
        // Dead-lettered work keeps its snapshot until PayloadRetention, so it can be requeued.
        (await CountRemainingCapturedPayloadsAsync(result.SweepId)).Should().Be(1);
    }

    [Fact]
    public async Task FlushAsync_Keeps_The_Snapshot_Of_Dead_Lettered_Work_And_Requeue_Redelivers_It()
    {
        var tenantId = Guid.NewGuid();
        var asOf = new DateTimeOffset(2026, 4, 13, 12, 0, 0, TimeSpan.Zero);
        var outage = new DownstreamOutage { Down = true };
        var sink = new HandlerExecutionSink();
        await using (var db = Host.CreateDbContext())
        {
            db.Notes.Add(
                new Note { Id = Guid.NewGuid(), TenantId = tenantId, CreatedAt = asOf.AddDays(-120), Body = "outage" }
            );
            await db.SaveChangesAsync();
        }

        using var handlerHost = new CohortTestHost(
            GetConnectionString(),
            configurationOverrides: new Dictionary<string, string?>
            {
                [$"{CohortOptions.SectionName}:RowHandlerDispatch:MaxAttempts"] = "1",
            },
            configureServices: services =>
            {
                services.AddSingleton(outage);
                services.AddSingleton(sink);
                services.AddRowHandler<Note, OutageSensitiveNoteHandler>();
            }
        );
        var result = await handlerHost.RunSweepAsync(
            new TenantContext(tenantId, "uk", new Dictionary<string, string>()),
            asOf
        );

        await handlerHost.RunWithServicesAsync(async serviceProvider =>
        {
            var dispatcher = serviceProvider.GetRequiredService<IRetentionRowDispatcher>();
            (await dispatcher.FlushAsync()).Should().Be(new RowDispatcherFlushResult(0, 0, 1));
            (await LoadHandlerStatusesAsync(result.SweepId)).Single().State.Should().Be(DeadLetteredState);
            (await CountRemainingCapturedPayloadsAsync(result.SweepId)).Should().Be(1);

            // The outage ends; the operator requeues the work. A filter for another run
            // touches nothing.
            outage.Down = false;
            (await dispatcher.RequeueDeadLetteredAsync(sweepId: Guid.NewGuid())).Should().Be(0);
            (await dispatcher.RequeueDeadLetteredAsync(sweepId: result.SweepId)).Should().Be(1);
            (await dispatcher.FlushAsync()).Should().Be(new RowDispatcherFlushResult(0, 0, 0));
        });

        // One more delivery, with the attempt count carried on so the handler knows the work
        // may already be done.
        sink.AfterCalls.Should().Equal("after:outage:2");
        var status = (await LoadHandlerStatusesAsync(result.SweepId)).Single();
        status.State.Should().Be(SucceededState);
        status.Attempt.Should().Be(2);
        status.LastError.Should().BeNull();
        (await CountRemainingCapturedPayloadsAsync(result.SweepId)).Should().Be(0);
    }

    [Fact]
    public async Task Requeue_Requeues_Every_Dead_Lettered_Handler_Of_A_Row_In_Order()
    {
        var tenantId = Guid.NewGuid();
        var asOf = new DateTimeOffset(2026, 4, 13, 12, 0, 0, TimeSpan.Zero);
        var outage = new DownstreamOutage { Down = true };
        var sink = new HandlerExecutionSink();
        await using (var db = Host.CreateDbContext())
        {
            db.Notes.Add(
                new Note { Id = Guid.NewGuid(), TenantId = tenantId, CreatedAt = asOf.AddDays(-120), Body = "ordered" }
            );
            await db.SaveChangesAsync();
        }

        using var handlerHost = new CohortTestHost(
            GetConnectionString(),
            configurationOverrides: new Dictionary<string, string?>
            {
                [$"{CohortOptions.SectionName}:RowHandlerDispatch:MaxAttempts"] = "1",
            },
            configureServices: services =>
            {
                services.AddSingleton(outage);
                services.AddSingleton(sink);
                services.AddRowHandler<Note, OutageSensitiveNoteHandler>();
                services.AddRowHandler<Note, LowPriorityAfterNoteHandler>();
            }
        );
        var result = await handlerHost.RunSweepAsync(
            new TenantContext(tenantId, "uk", new Dictionary<string, string>()),
            asOf
        );

        await handlerHost.RunWithServicesAsync(async serviceProvider =>
        {
            var dispatcher = serviceProvider.GetRequiredService<IRetentionRowDispatcher>();
            await dispatcher.FlushAsync();
            var deadLettered = await LoadHandlerStatusesAsync(result.SweepId);
            deadLettered.Select(status => status.State).Should().Equal(DeadLetteredState, DeadLetteredState);
            deadLettered[1].Attempt.Should().Be(0);

            // Filtering on the later handler still requeues the earlier one, so the later
            // one can't run ahead of it.
            outage.Down = false;
            (await dispatcher.RequeueDeadLetteredAsync(handlerId: Guid.Parse(HandlerIdOf<LowPriorityAfterNoteHandler>())))
                .Should()
                .Be(2);
            await dispatcher.FlushAsync();
        });

        sink.AfterCalls.Should().Equal("after:ordered:2", "after-low");
        var statuses = await LoadHandlerStatusesAsync(result.SweepId);
        statuses.Select(status => status.State).Should().Equal(SucceededState, SucceededState);
        statuses.Select(status => status.Attempt).Should().Equal(2, 1);
    }

    [Fact]
    public async Task FlushAsync_Keeps_Work_For_An_Unregistered_Handler_Id_So_A_Requeue_Delivers_It_Once_It_Is_Back()
    {
        // The upgrade path for a re-keyed or removed handler: the work dead-letters while no
        // registered handler carries its id, and a requeue delivers it once one does.
        var tenantId = Guid.NewGuid();
        var asOf = new DateTimeOffset(2026, 4, 13, 12, 0, 0, TimeSpan.Zero);
        var sink = new HandlerExecutionSink();
        await using (var db = Host.CreateDbContext())
        {
            db.Notes.Add(
                new Note { Id = Guid.NewGuid(), TenantId = tenantId, CreatedAt = asOf.AddDays(-120), Body = "orphaned" }
            );
            await db.SaveChangesAsync();
        }

        void Register(IServiceCollection services)
        {
            services.AddSingleton(sink);
            services.AddRowHandler<Note, NoteHandlerBeforeRename>();
        }

        RetentionSweepResult result;
        using (var original = new CohortTestHost(GetConnectionString(), configureServices: Register))
        {
            result = await original.RunSweepAsync(
                new TenantContext(tenantId, "uk", new Dictionary<string, string>()),
                asOf
            );
        }

        using (var withoutHandler = new CohortTestHost(GetConnectionString()))
        {
            (await withoutHandler.RunWithServicesAsync(serviceProvider =>
                serviceProvider.GetRequiredService<IRetentionRowDispatcher>().FlushAsync()
            )).DeadLetteredRequeueable.Should().Be(1);
        }

        var orphaned = (await LoadHandlerStatusesAsync(result.SweepId)).Single();
        orphaned.State.Should().Be(DeadLetteredState);
        orphaned.LastError.Should().Be($"handler id {NoteHandlerBeforeRename.Id} is not registered; re-register it and requeue");

        using var restored = new CohortTestHost(GetConnectionString(), configureServices: Register);
        await restored.RunWithServicesAsync(async serviceProvider =>
        {
            var dispatcher = serviceProvider.GetRequiredService<IRetentionRowDispatcher>();
            (await dispatcher.RequeueDeadLetteredAsync()).Should().Be(1);
            await dispatcher.FlushAsync();
        });

        sink.AfterCalls.Should().Equal("original:orphaned:2");
        (await LoadHandlerStatusesAsync(result.SweepId)).Single().State.Should().Be(SucceededState);
        (await CountRemainingCapturedPayloadsAsync(result.SweepId)).Should().Be(0);
    }

    [Fact]
    public async Task FlushAsync_Does_Not_Reclaim_An_Expired_Claim_Or_Start_The_Rows_Next_Handler_While_Its_Owner_Still_Runs_It()
    {
        var tenantId = Guid.NewGuid();
        var asOf = new DateTimeOffset(2026, 4, 13, 12, 0, 0, TimeSpan.Zero);
        var gate = new DispatchBlockGate();
        var sink = new HandlerExecutionSink();
        await using (var db = Host.CreateDbContext())
        {
            db.Notes.Add(
                new Note { Id = Guid.NewGuid(), TenantId = tenantId, CreatedAt = asOf.AddDays(-120), Body = "zombie" }
            );
            await db.SaveChangesAsync();
        }

        void Register(IServiceCollection services)
        {
            services.AddSingleton(gate);
            services.AddSingleton(sink);
            services.AddRowHandler<Note, TokenIgnoringBlockingNoteHandler>();
            services.AddRowHandler<Note, LowPriorityAfterNoteHandler>();
        }

        using var owner = new CohortTestHost(GetConnectionString(), configureServices: Register);
        using var replica = new CohortTestHost(GetConnectionString(), configureServices: Register);
        var result = await owner.RunSweepAsync(
            new TenantContext(tenantId, "uk", new Dictionary<string, string>()),
            asOf
        );

        var ownerFlush = owner.RunWithServicesAsync(serviceProvider =>
            serviceProvider.GetRequiredService<IRetentionRowDispatcher>().FlushAsync()
        );
        await gate.WaitUntilBlockedAsync().WaitAsync(TimeSpan.FromSeconds(5));
        var ownerToken = await LoadInFlightClaimTokenAsync(result.SweepId);

        // The claim looks expired, but the handler ignoring its token is still running: the
        // replica must neither run it again nor start the row's next handler.
        await BackdateClaimAsync(result.SweepId, DateTimeOffset.UtcNow.AddMinutes(-6));
        var replicaFlush = await replica.RunWithServicesAsync(serviceProvider =>
            serviceProvider.GetRequiredService<IRetentionRowDispatcher>().FlushAsync()
        );

        replicaFlush.InFlightRemaining.Should().Be(1);
        sink.AfterCalls.Should().BeEmpty();
        var held = await LoadHandlerStatusesAsync(result.SweepId);
        held[0].State.Should().Be(InFlightState);
        held[0].Attempt.Should().Be(1);
        held[1].State.Should().Be(PendingState);
        held[1].Attempt.Should().Be(0);
        (await LoadInFlightClaimTokenAsync(result.SweepId)).Should().Be(ownerToken);

        gate.Release();
        await ownerFlush.WaitAsync(TimeSpan.FromSeconds(10));

        sink.AfterCalls.Should().Equal("after-zombie:1", "after-low");
        var settled = await LoadHandlerStatusesAsync(result.SweepId);
        settled.Select(status => status.State).Should().Equal(SucceededState, SucceededState);
        settled.Select(status => status.Attempt).Should().Equal(1, 1);

        // The owner released each delivery lock once it recorded the outcome.
        await using var connection = new NpgsqlConnection(GetConnectionString());
        await connection.OpenAsync();
        foreach (var statusId in await LoadHandlerStatusIdsAsync(result.SweepId))
        {
            var key = RetentionRunAdvisoryLock.RowHandlerKeyFor(
                new RelationalObjectName("public", "sweep_row_handler_status"),
                statusId
            );
            (await RetentionRunAdvisoryLock.TryAcquireAsync(connection, key, CancellationToken.None)).Should().BeTrue();
            await RetentionRunAdvisoryLock.ReleaseAsync(connection, key, CancellationToken.None);
        }
    }

    [Fact]
    public async Task Kill_Switch_Stops_The_Background_Poller_But_Not_An_Explicit_Flush_Or_The_Payload_Scrub()
    {
        var pausedTenantId = Guid.NewGuid();
        var expiredTenantId = Guid.NewGuid();
        var asOf = new DateTimeOffset(2026, 4, 13, 12, 0, 0, TimeSpan.Zero);
        var sink = new HandlerExecutionSink();
        await using (var db = Host.CreateDbContext())
        {
            db.Notes.Add(
                new Note { Id = Guid.NewGuid(), TenantId = pausedTenantId, CreatedAt = asOf.AddDays(-120), Body = "paused" }
            );
            db.Notes.Add(
                new Note { Id = Guid.NewGuid(), TenantId = expiredTenantId, CreatedAt = asOf.AddDays(-120), Body = "expired" }
            );
            await db.SaveChangesAsync();
        }

        using var handlerHost = new CohortTestHost(
            GetConnectionString(),
            configurationOverrides: new Dictionary<string, string?>
            {
                [$"{CohortOptions.SectionName}:KillSwitch"] = "true",
                [$"{CohortOptions.SectionName}:RowHandlerDispatch:PayloadRetention"] = "01:00:00",
                // A retry schedule must fit inside PayloadRetention.
                [$"{CohortOptions.SectionName}:RowHandlerDispatch:MaxAttempts"] = "1",
            },
            configureServices: services =>
            {
                services.AddSingleton(sink);
                services.AddRowHandler<Note, DispatchRecordingNoteHandler>();
            }
        );
        var paused = await handlerHost.RunSweepAsync(
            new TenantContext(pausedTenantId, "uk", new Dictionary<string, string>()),
            asOf
        );
        var expired = await handlerHost.RunSweepAsync(
            new TenantContext(expiredTenantId, "uk", new Dictionary<string, string>()),
            asOf
        );
        await BackdateRowDetailsAsync(expired.SweepId, DateTimeOffset.UtcNow.AddHours(-2));

        await handlerHost.RunWithServicesAsync(serviceProvider =>
            serviceProvider.GetRequiredService<RetentionRowDispatcher>().PollAsync(CancellationToken.None)
        );

        sink.AfterCalls.Should().BeEmpty();
        (await LoadHandlerStatusesAsync(paused.SweepId)).Single().State.Should().Be(PendingState);
        // The switch pauses delivery, never the personal-data backstop.
        (await CountRemainingCapturedPayloadsAsync(expired.SweepId)).Should().Be(0);
        (await CountRemainingCapturedPayloadsAsync(paused.SweepId)).Should().Be(1);

        await handlerHost.RunWithServicesAsync(serviceProvider =>
            serviceProvider.GetRequiredService<IRetentionRowDispatcher>().FlushAsync()
        );

        sink.AfterCalls.Should().Equal("after:paused:1");
        (await LoadHandlerStatusesAsync(expired.SweepId)).Single().LastError.Should().Contain("payload-retention backstop");
    }

    [Fact]
    public async Task Kill_Switch_Turned_On_Mid_Drain_Stops_The_Poller_After_Its_Current_Batch()
    {
        var tenantId = Guid.NewGuid();
        var asOf = new DateTimeOffset(2026, 4, 13, 12, 0, 0, TimeSpan.Zero);
        var gate = new DispatchBlockGate();
        var sink = new HandlerExecutionSink();
        await using (var db = Host.CreateDbContext())
        {
            foreach (var body in new[] { "first", "second" })
            {
                db.Notes.Add(
                    new Note { Id = Guid.NewGuid(), TenantId = tenantId, CreatedAt = asOf.AddDays(-120), Body = body }
                );
            }

            await db.SaveChangesAsync();
        }

        // Reloadable, so the switch can be flipped while a delivery runs.
        var configuration = new ConfigurationBuilder()
            .AddInMemoryCollection(
                new Dictionary<string, string?>
                {
                    [$"{SampleOptions.SectionName}:{nameof(SampleOptions.ConnectionString)}"] = GetConnectionString(),
                    // One row per batch, so the second row needs a second batch.
                    [$"{CohortOptions.SectionName}:RowHandlerDispatch:MaxParallelism"] = "1",
                }
            )
            .Build();
        using var handlerHost = new CohortTestHost(
            GetConnectionString(),
            configureServices: services =>
            {
                services.AddSingleton<IConfiguration>(configuration);
                services.AddSingleton(gate);
                services.AddSingleton(sink);
                services.AddRowHandler<Note, BlockingDispatchNoteHandler>();
            }
        );
        var result = await handlerHost.RunSweepAsync(
            new TenantContext(tenantId, "uk", new Dictionary<string, string>()),
            asOf
        );

        var poll = handlerHost.RunWithServicesAsync(serviceProvider =>
            serviceProvider.GetRequiredService<RetentionRowDispatcher>().PollAsync(CancellationToken.None)
        );
        await gate.WaitUntilBlockedAsync().WaitAsync(TimeSpan.FromSeconds(10));
        configuration[$"{CohortOptions.SectionName}:KillSwitch"] = "true";
        configuration.Reload();
        gate.Release();
        await poll.WaitAsync(TimeSpan.FromSeconds(10));

        sink.AfterCalls.Should().ContainSingle();
        (await LoadHandlerStatusesAsync(result.SweepId))
            .Select(status => status.State)
            .Should()
            .BeEquivalentTo([SucceededState, PendingState]);
    }

    [Fact]
    public async Task FlushAsync_Recovers_A_Crashed_Claim_Queued_Behind_Claims_Whose_Owners_Still_Run_Them()
    {
        // Claims whose owners still run them are never reclaimed, so they stay the oldest
        // expired claims. Recovery pages past them to the crashed dispatcher's claim.
        var heldTenantId = Guid.NewGuid();
        var crashedTenantId = Guid.NewGuid();
        var asOf = new DateTimeOffset(2026, 4, 13, 12, 0, 0, TimeSpan.Zero);
        var sink = new HandlerExecutionSink();
        await using (var db = Host.CreateDbContext())
        {
            db.Notes.Add(
                new Note { Id = Guid.NewGuid(), TenantId = heldTenantId, CreatedAt = asOf.AddDays(-120), Body = "held" }
            );
            db.Notes.Add(
                new Note { Id = Guid.NewGuid(), TenantId = crashedTenantId, CreatedAt = asOf.AddDays(-120), Body = "crashed" }
            );
            await db.SaveChangesAsync();
        }

        using var handlerHost = new CohortTestHost(
            GetConnectionString(),
            configurationOverrides: new Dictionary<string, string?>
            {
                [$"{CohortOptions.SectionName}:RowHandlerDispatch:BatchSize"] = "1",
            },
            configureServices: services =>
            {
                services.AddSingleton(sink);
                services.AddRowHandler<Note, DispatchRecordingNoteHandler>();
            }
        );
        var held = await handlerHost.RunSweepAsync(
            new TenantContext(heldTenantId, "uk", new Dictionary<string, string>()),
            asOf
        );
        var crashed = await handlerHost.RunSweepAsync(
            new TenantContext(crashedTenantId, "uk", new Dictionary<string, string>()),
            asOf
        );
        await AbandonClaimAsync(held.SweepId, 1, DateTimeOffset.UtcNow.AddMinutes(-10));
        await AbandonClaimAsync(crashed.SweepId, 1, DateTimeOffset.UtcNow.AddMinutes(-6));

        // The held claim's owner is alive: it holds the delivery lock on its own session.
        await using var owner = new NpgsqlConnection(GetConnectionString());
        await owner.OpenAsync();
        var heldKey = RetentionRunAdvisoryLock.RowHandlerKeyFor(
            new RelationalObjectName("public", "sweep_row_handler_status"),
            (await LoadHandlerStatusIdsAsync(held.SweepId)).Single()
        );
        await RetentionRunAdvisoryLock.AcquireAsync(owner, heldKey, CancellationToken.None);

        await handlerHost.RunWithServicesAsync(serviceProvider =>
            serviceProvider.GetRequiredService<IRetentionRowDispatcher>().FlushAsync()
        );

        sink.AfterCalls.Should().Equal("after:crashed:2");
        (await LoadHandlerStatusesAsync(held.SweepId)).Single().State.Should().Be(InFlightState);
        (await LoadHandlerStatusesAsync(crashed.SweepId)).Single().State.Should().Be(SucceededState);
        await RetentionRunAdvisoryLock.ReleaseAsync(owner, heldKey, CancellationToken.None);
    }

    [Fact]
    public async Task FlushAsync_Keeps_Work_For_An_Unregistered_Entity_Id_So_A_Requeue_Delivers_It_Once_It_Is_Back()
    {
        // Retrying can't heal work whose entity id the model no longer registers, so it
        // dead-letters at once and keeps its snapshot instead of burning its retries.
        var tenantId = Guid.NewGuid();
        var asOf = new DateTimeOffset(2026, 4, 13, 12, 0, 0, TimeSpan.Zero);
        var sink = new HandlerExecutionSink();
        await using (var db = Host.CreateDbContext())
        {
            db.Notes.Add(
                new Note { Id = Guid.NewGuid(), TenantId = tenantId, CreatedAt = asOf.AddDays(-120), Body = "unknown-entity" }
            );
            await db.SaveChangesAsync();
        }

        using var handlerHost = new CohortTestHost(
            GetConnectionString(),
            configureServices: services =>
            {
                services.AddSingleton(sink);
                services.AddRowHandler<Note, DispatchRecordingNoteHandler>();
            }
        );
        var result = await handlerHost.RunSweepAsync(
            new TenantContext(tenantId, "uk", new Dictionary<string, string>()),
            asOf
        );
        var noteEntityId = typeof(Note).GetCustomAttribute<RetentionEntityIdAttribute>()!.Id;
        var unknownEntityId = Guid.NewGuid();
        await SetRowDetailEntityIdAsync(result.SweepId, unknownEntityId);

        (await handlerHost.RunWithServicesAsync(serviceProvider =>
            serviceProvider.GetRequiredService<IRetentionRowDispatcher>().FlushAsync()
        )).DeadLetteredRequeueable.Should().Be(1);

        sink.AfterCalls.Should().BeEmpty();
        var orphaned = (await LoadHandlerStatusesAsync(result.SweepId)).Single();
        orphaned.State.Should().Be(DeadLetteredState);
        orphaned.Attempt.Should().Be(1);
        orphaned.LastError.Should().Be(
            $"entity id {unknownEntityId} is not registered; restore its [RetentionEntityId] and requeue"
        );

        await SetRowDetailEntityIdAsync(result.SweepId, noteEntityId);
        await handlerHost.RunWithServicesAsync(async serviceProvider =>
        {
            var dispatcher = serviceProvider.GetRequiredService<IRetentionRowDispatcher>();
            (await dispatcher.RequeueDeadLetteredAsync()).Should().Be(1);
            await dispatcher.FlushAsync();
        });

        sink.AfterCalls.Should().Equal("after:unknown-entity:2");
        (await LoadHandlerStatusesAsync(result.SweepId)).Single().State.Should().Be(SucceededState);
    }

    [Fact]
    public async Task The_Documented_Upgrade_Rekey_Delivers_Work_Queued_Under_A_Type_Name()
    {
        // 0.10 persisted work under the handler's type name. The CHANGELOG's upgrade step
        // finds those keys, then re-keys them to the [RowHandlerId], whatever case or format
        // the host copies the UUID in.
        var tenantId = Guid.NewGuid();
        var asOf = new DateTimeOffset(2026, 4, 13, 12, 0, 0, TimeSpan.Zero);
        var sink = new HandlerExecutionSink();
        const string legacyKey = "Cohort.Sample.NoteSweptLogger, Cohort.Sample";
        await using (var db = Host.CreateDbContext())
        {
            db.Notes.Add(
                new Note { Id = Guid.NewGuid(), TenantId = tenantId, CreatedAt = asOf.AddDays(-120), Body = "legacy" }
            );
            await db.SaveChangesAsync();
        }

        using var handlerHost = new CohortTestHost(
            GetConnectionString(),
            configureServices: services =>
            {
                services.AddSingleton(sink);
                services.AddRowHandler<Note, NoteHandlerBeforeRename>();
            }
        );
        var result = await handlerHost.RunSweepAsync(
            new TenantContext(tenantId, "uk", new Dictionary<string, string>()),
            asOf
        );
        await ExecuteAsync(
            $"""UPDATE "sweep_row_handler_status" SET "HandlerType" = '{legacyKey}'"""
        );

        // Step 1, verbatim: find the keys that aren't UUIDs.
        await using (var connection = new NpgsqlConnection(GetConnectionString()))
        {
            await connection.OpenAsync();
            await using var find = connection.CreateCommand();
            find.CommandText = """
                SELECT DISTINCT "HandlerType" FROM public.sweep_row_handler_status WHERE "State" IN (0, 1, 3) AND "HandlerType" !~ '^[0-9a-f]{8}-[0-9a-f]{4}-';
                """;
            (await find.ExecuteScalarAsync()).Should().Be(legacyKey);
        }

        // Step 2, verbatim, with the UUID copied in upper case.
        await ExecuteAsync(
            $"""
            UPDATE public.sweep_row_handler_status SET "HandlerType" = CAST('{NoteHandlerBeforeRename.Id.ToUpperInvariant()}' AS uuid)::text WHERE "HandlerType" = '{legacyKey}' AND "State" IN (0, 1, 3);
            """
        );
        await handlerHost.RunWithServicesAsync(serviceProvider =>
            serviceProvider.GetRequiredService<IRetentionRowDispatcher>().FlushAsync()
        );

        sink.AfterCalls.Should().Equal("original:legacy:1");
        var status = (await LoadHandlerStatusesAsync(result.SweepId)).Single();
        status.State.Should().Be(SucceededState);
        status.HandlerType.Should().Be(NoteHandlerBeforeRename.Id);
    }

    [Theory]
    [InlineData("System.Diagnostics.ProcessStartInfo, System.Diagnostics.Process")]
    // Without an assembly name Type.GetType searches CoreLib and never asks the resolver.
    [InlineData("System.Version")]
    public async Task FlushAsync_Refuses_Snapshot_Payloads_Naming_Types_Outside_The_AllowList(string typeName)
    {
        var tenantId = Guid.NewGuid();
        var asOf = new DateTimeOffset(2026, 4, 13, 12, 0, 0, TimeSpan.Zero);
        var noteId = Guid.NewGuid();
        var recorder = new TypedSnapshotRecorder();

        await using (var db = Host.CreateDbContext())
        {
            db.Notes.Add(
                new Note
                {
                    Id = noteId,
                    TenantId = tenantId,
                    CreatedAt = asOf.AddDays(-120),
                    Body = "tampered-payload-target",
                }
            );
            await db.SaveChangesAsync();
        }

        using var handlerHost = new CohortTestHost(
            GetConnectionString(),
            configurationOverrides: new Dictionary<string, string?>
            {
                [$"{CohortOptions.SectionName}:RowHandlerDispatch:MaxAttempts"] = "1",
            },
            configureServices: services =>
            {
                services.AddSingleton(recorder);
                services.AddRowHandler<Note, TypedSnapshotNoteHandler>();
            }
        );

        var result = await handlerHost.RunSweepAsync(
            new TenantContext(tenantId, "uk", new Dictionary<string, string>()),
            asOf
        );

        // Simulate a tampered captured payload naming a framework type: deserialising it
        // must be refused rather than materialising an arbitrary CLR type.
        await using (var connection = new NpgsqlConnection(GetConnectionString()))
        {
            await connection.OpenAsync();
            await using var command = connection.CreateCommand();
            command.CommandText = """
                UPDATE "sweep_run_row_detail"
                SET "CapturedPayload" = @payload
                WHERE "SweepId" = @sweepId
                """;
            command.Parameters.AddWithValue(
                "payload",
                $$$"""{"payload":{"$cohortType":"{{{typeName}}}","$cohortValue":"1.2"}}"""
            );
            command.Parameters.AddWithValue("sweepId", result.SweepId);
            (await command.ExecuteNonQueryAsync()).Should().Be(1);
        }

        await handlerHost.RunWithServicesAsync(async serviceProvider =>
        {
            var dispatcher = serviceProvider.GetRequiredService<IRetentionRowDispatcher>();
            await dispatcher.FlushAsync();
        });

        recorder.Load().Should().BeEmpty();

        var statuses = await LoadHandlerStatusesAsync(result.SweepId);
        statuses.Should().ContainSingle();
        statuses[0].State.Should().Be(DeadLetteredState);
        AssertInvalidOperationDiagnostic(statuses[0].LastError);
    }

    [Fact]
    public async Task FlushAsync_Delivers_Typed_Array_And_List_Snapshot_Values_With_Their_Element_Types()
    {
        var tenantId = Guid.NewGuid();
        var asOf = new DateTimeOffset(2026, 4, 13, 12, 0, 0, TimeSpan.Zero);
        var recorder = new CollectionSnapshotRecorder();

        await using (var db = Host.CreateDbContext())
        {
            db.Notes.Add(
                new Note
                {
                    Id = Guid.NewGuid(),
                    TenantId = tenantId,
                    CreatedAt = asOf.AddDays(-120),
                    Body = "typed-collections",
                }
            );
            await db.SaveChangesAsync();
        }

        using var handlerHost = new CohortTestHost(
            GetConnectionString(),
            configureServices: services =>
            {
                services.AddSingleton(recorder);
                services.AddRowHandler<Note, CollectionSnapshotNoteHandler>();
            }
        );

        await handlerHost.RunSweepAsync(
            new TenantContext(tenantId, "uk", new Dictionary<string, string>()),
            asOf
        );
        await handlerHost.RunWithServicesAsync(async serviceProvider =>
        {
            await serviceProvider.GetRequiredService<IRetentionRowDispatcher>().FlushAsync();
        });

        var snapshot = recorder.Snapshot.Should().NotBeNull().And.Subject!;
        snapshot["paths"].Should().BeOfType<string[]>().Which.Should().Equal("a/1", "b/2");
        snapshot["tags"].Should().BeOfType<List<string>>().Which.Should().Equal("x", "y");
        snapshot["sizes"].Should().BeOfType<int[]>().Which.Should().Equal(1, 2, 3);
    }

    [Fact]
    public async Task Sweep_Refuses_Snapshot_Values_That_Cannot_Round_Trip_Before_Mutating_The_Row()
    {
        var tenantId = Guid.NewGuid();
        var asOf = new DateTimeOffset(2026, 4, 13, 12, 0, 0, TimeSpan.Zero);
        var noteId = Guid.NewGuid();
        var recorder = new CollectionSnapshotRecorder();

        await using (var db = Host.CreateDbContext())
        {
            db.Notes.Add(
                new Note
                {
                    Id = noteId,
                    TenantId = tenantId,
                    CreatedAt = asOf.AddDays(-120),
                    Body = "unsupported-snapshot",
                }
            );
            await db.SaveChangesAsync();
        }

        using var handlerHost = new CohortTestHost(
            GetConnectionString(),
            configureServices: services =>
            {
                services.AddSingleton(recorder);
                services.AddRowHandler<Note, UnsupportedSnapshotNoteHandler>();
            }
        );

        var result = await handlerHost.RunSweepAsync(
            new TenantContext(tenantId, "uk", new Dictionary<string, string>()),
            asOf
        );
        await handlerHost.RunWithServicesAsync(async serviceProvider =>
        {
            await serviceProvider.GetRequiredService<IRetentionRowDispatcher>().FlushAsync();
        });

        result.EntityFailures.Should().ContainSingle().Which.Should().Be("reason=rows-skipped;entityType=Cohort.Sample.Entities.Note;count=1");
        (await LoadRunStatusAndErrorAsync(result.SweepId))
            .Should()
            .Be((SweepRunStatus.PartiallyFailed, "reason=rows-skipped;entityType=Cohort.Sample.Entities.Note;count=1"));
        result
            .Counts.Should()
            .Contain(
                new EntitySweepCount(
                    typeof(Note),
                    "short-lived",
                    tenantId,
                    Strategy.Purge,
                    0,
                    HeldCount: 0,
                    SkippedCount: 1,
                    NullAnchorCount: 0,
                    RetainedCount: 0,
                    DeferredCount: 0,
                    BlockedCount: 0
                )
            );
        recorder.Snapshot.Should().BeNull();

        await using (var verify = Host.CreateDbContext())
        {
            (await verify.Notes.AnyAsync(note => note.Id == noteId)).Should().BeTrue();
        }

        var statuses = await LoadHandlerStatusesAsync(result.SweepId);
        statuses.Should().ContainSingle().Which.State.Should().Be(DeadLetteredState);
    }

    private static DateTimeOffset EligibleErasureCreatedAt(DateTimeOffset asOf)
    {
        return asOf.AddDays(-45);
    }

    private static void AssertInvalidOperationDiagnostic(string? error)
    {
        error.Should().NotBeNull();
        error.Should().MatchRegex(
            "^type=System\\.InvalidOperationException;code=hresult:0x80131509;diagnosticId=[0-9a-f]{32}$"
        );
    }

    private async Task CreateHoldAsync(
        string tableName,
        Guid recordId,
        Guid tenantId,
        DateTimeOffset asOf
    )
    {
        await Host.RunWithServicesAsync(async services =>
        {
            var repository = services.GetRequiredService<IRetentionHoldsRepository>();
            await repository.CreateAsync(
                new RetentionHoldRequest(
                    Guid.NewGuid(),
                    RetentionEntityIdentity.ForTable(tableName),
                    recordId.ToString(),
                    tenantId,
                    "handler-erasure-hold",
                    asOf.AddDays(-1)
                ),
                CancellationToken.None
            );
        });
    }

    private async Task<IReadOnlyList<CapturedRow>> LoadCapturedRowsAsync(Guid sweepId)
    {
        await using var connection = new NpgsqlConnection(GetConnectionString());
        await connection.OpenAsync();
        await using var command = connection.CreateCommand();
        command.CommandText = """
            SELECT "EntityType", "RetentionEntityId", "RecordId", COALESCE("CapturedPayload", '')
            FROM "sweep_run_row_detail"
            WHERE "SweepId" = @sweepId
            ORDER BY "Id"
            """;
        command.Parameters.AddWithValue("sweepId", sweepId);

        var rows = new List<CapturedRow>();
        await using var reader = await command.ExecuteReaderAsync();
        while (await reader.ReadAsync())
        {
            rows.Add(
                new CapturedRow(
                    reader.GetString(0),
                    reader.GetGuid(1),
                    reader.GetString(2),
                    reader.GetString(3)
                )
            );
        }

        return rows;
    }

    private async Task<IReadOnlyList<CapturedRowDetail>> LoadCapturedRowDetailsAsync(Guid sweepId)
    {
        await using var connection = new NpgsqlConnection(GetConnectionString());
        await connection.OpenAsync();
        await using var command = connection.CreateCommand();
        command.CommandText = """
            SELECT
                "RecordId",
                "Category",
                "Strategy",
                "TenantId",
                "At",
                COALESCE("CapturedPayload", '')
            FROM "sweep_run_row_detail"
            WHERE "SweepId" = @sweepId
            ORDER BY "Id"
            """;
        command.Parameters.AddWithValue("sweepId", sweepId);

        var rows = new List<CapturedRowDetail>();
        await using var reader = await command.ExecuteReaderAsync();
        while (await reader.ReadAsync())
        {
            var payload = reader.GetString(5);
            using var snapshot = JsonDocument.Parse(payload);
            rows.Add(
                new CapturedRowDetail(
                    reader.GetString(0),
                    reader.GetString(1),
                    (Strategy)reader.GetInt32(2),
                    reader.GetGuid(3),
                    reader.GetFieldValue<DateTimeOffset>(4),
                    snapshot.RootElement.GetProperty("body").GetString()!
                )
            );
        }

        return rows;
    }

    private async Task<long> CountRemainingCapturedPayloadsAsync(Guid sweepId)
    {
        await using var connection = new NpgsqlConnection(GetConnectionString());
        await connection.OpenAsync();
        await using var command = connection.CreateCommand();
        command.CommandText = """
            SELECT COUNT(*)
            FROM "sweep_run_row_detail"
            WHERE "SweepId" = @sweepId
              AND "CapturedPayload" IS NOT NULL
            """;
        command.Parameters.AddWithValue("sweepId", sweepId);
        return (long)(await command.ExecuteScalarAsync())!;
    }

    private static string HandlerIdOf<THandler>() =>
        typeof(THandler).GetCustomAttribute<RowHandlerIdAttribute>()!.Id.ToString("D");

    private async Task<IReadOnlyList<HandlerStatusRow>> LoadHandlerStatusesAsync(Guid sweepId, string? connectionString = null)
    {
        await using var connection = new NpgsqlConnection(connectionString ?? GetConnectionString());
        await connection.OpenAsync();
        await using var command = connection.CreateCommand();
        command.CommandText = """
            SELECT
                status."HandlerType",
                status."DispatchPhase",
                status."State",
                status."Attempt",
                status."NextAttemptAt",
                status."ClaimedAt",
                status."CompletedAt",
                status."LastError"
            FROM "sweep_row_handler_status" AS status
            INNER JOIN "sweep_run_row_detail" AS detail ON detail."Id" = status."SweepRunRowDetailId"
            WHERE detail."SweepId" = @sweepId
            ORDER BY status."Id"
            """;
        command.Parameters.AddWithValue("sweepId", sweepId);

        var rows = new List<HandlerStatusRow>();
        await using var reader = await command.ExecuteReaderAsync();
        while (await reader.ReadAsync())
        {
            rows.Add(
                new HandlerStatusRow(
                    reader.GetString(0),
                    reader.GetInt32(1),
                    reader.GetInt32(2),
                    reader.GetInt32(3),
                    reader.GetFieldValue<DateTimeOffset>(4),
                    reader.IsDBNull(5) ? null : reader.GetFieldValue<DateTimeOffset>(5),
                    reader.IsDBNull(6) ? null : reader.GetFieldValue<DateTimeOffset>(6),
                    reader.IsDBNull(7) ? null : reader.GetString(7)
                )
            );
        }

        return rows;
    }

    private async Task<HandlerStatusRow> WaitForHandlerStatusAsync(
        Guid sweepId,
        Func<HandlerStatusRow, bool> predicate,
        TimeSpan? timeout = null
    )
    {
        var deadline = DateTimeOffset.UtcNow + (timeout ?? TimeSpan.FromSeconds(5));

        while (DateTimeOffset.UtcNow <= deadline)
        {
            var match = (await LoadHandlerStatusesAsync(sweepId)).SingleOrDefault(predicate);
            if (match is not null)
            {
                return match;
            }

            await Task.Delay(50);
        }

        throw new TimeoutException(
            $"Timed out waiting for a handler status row that matched the requested predicate for sweep {sweepId}."
        );
    }

    private async Task<(SweepRunStatus Status, string? Error)> LoadRunStatusAndErrorAsync(Guid sweepId)
    {
        await using var connection = new NpgsqlConnection(GetConnectionString());
        await connection.OpenAsync();
        await using var command = connection.CreateCommand();
        command.CommandText = """SELECT "Status", "Error" FROM "sweep_run" WHERE "SweepId" = @sweepId""";
        command.Parameters.AddWithValue("sweepId", sweepId);
        await using var reader = await command.ExecuteReaderAsync();
        (await reader.ReadAsync()).Should().BeTrue();
        return ((SweepRunStatus)reader.GetInt32(0), reader.IsDBNull(1) ? null : reader.GetString(1));
    }

    private async Task<IReadOnlyList<EntitySummaryRow>> LoadEntitySummariesAsync(Guid sweepId)
    {
        await using var connection = new NpgsqlConnection(GetConnectionString());
        await connection.OpenAsync();
        await using var command = connection.CreateCommand();
        command.CommandText = """
            SELECT "EntityType", "Strategy", "Affected", "HeldCount", "SkippedCount"
            FROM "sweep_run_entity_summary"
            WHERE "SweepId" = @sweepId
            ORDER BY "EntityType"
            """;
        command.Parameters.AddWithValue("sweepId", sweepId);

        var rows = new List<EntitySummaryRow>();
        await using var reader = await command.ExecuteReaderAsync();
        while (await reader.ReadAsync())
        {
            rows.Add(
                new EntitySummaryRow(
                    reader.GetString(0),
                    (Strategy)reader.GetInt32(1),
                    reader.GetInt32(2),
                    reader.GetInt32(3),
                    reader.GetInt32(4)
                )
            );
        }

        return rows;
    }

    private string GetConnectionString()
    {
        using var db = Host.CreateDbContext();
        return db.Database.GetConnectionString()!;
    }

    private async Task BackdateRowDetailsAsync(Guid sweepId, DateTimeOffset at)
    {
        await using var connection = new NpgsqlConnection(GetConnectionString());
        await connection.OpenAsync();
        await using var command = connection.CreateCommand();
        command.CommandText = """
            UPDATE "sweep_run_row_detail"
            SET "At" = @at
            WHERE "SweepId" = @sweepId
            """;
        command.Parameters.AddWithValue("at", at);
        command.Parameters.AddWithValue("sweepId", sweepId);
        (await command.ExecuteNonQueryAsync()).Should().BeGreaterThan(0);
    }

    private async Task SetSweepCompletedAtAsync(Guid sweepId, DateTimeOffset? completedAt)
    {
        await using var connection = new NpgsqlConnection(GetConnectionString());
        await connection.OpenAsync();
        await using var command = connection.CreateCommand();
        command.CommandText = """
            UPDATE "sweep_run"
            SET "Status" = @status,
                "SettledAt" = @completedAt
            WHERE "SweepId" = @sweepId
            """;
        command.Parameters.AddWithValue("sweepId", sweepId);
        command.Parameters.AddWithValue("status", completedAt is null ? 0 : 1);
        command.Parameters.AddWithValue("completedAt", (object?)completedAt ?? DBNull.Value);
        await command.ExecuteNonQueryAsync();
    }

    private async Task SetSweepStartedAtAsync(Guid sweepId, DateTimeOffset startedAt)
    {
        await using var connection = new NpgsqlConnection(GetConnectionString());
        await connection.OpenAsync();
        await using var command = connection.CreateCommand();
        command.CommandText = """
            UPDATE "sweep_run"
            SET "Status" = @status,
                "StartedAt" = @startedAt,
                "SettledAt" = NULL
            WHERE "SweepId" = @sweepId
            """;
        command.Parameters.AddWithValue("sweepId", sweepId);
        command.Parameters.AddWithValue("status", (int)SweepRunStatus.Started);
        command.Parameters.AddWithValue("startedAt", startedAt);
        (await command.ExecuteNonQueryAsync()).Should().Be(1);
    }

    private async Task ReclaimInFlightAsync(Guid sweepId)
    {
        await using var connection = new NpgsqlConnection(GetConnectionString());
        await connection.OpenAsync();
        await using var command = connection.CreateCommand();
        command.CommandText = """
            UPDATE "sweep_row_handler_status" AS status
            SET "ClaimedAt" = @claimedAt, "ClaimToken" = gen_random_uuid(), "Attempt" = "Attempt" + 1
            FROM "sweep_run_row_detail" AS detail
            WHERE detail."Id" = status."SweepRunRowDetailId"
              AND detail."SweepId" = @sweepId
              AND status."State" = @inFlight
            """;
        command.Parameters.AddWithValue("claimedAt", DateTimeOffset.UtcNow);
        command.Parameters.AddWithValue("sweepId", sweepId);
        command.Parameters.AddWithValue("inFlight", InFlightState);
        (await command.ExecuteNonQueryAsync()).Should().Be(1);
    }

    private async Task MakeHandlerWorkDueAsync(Guid sweepId)
    {
        await using var connection = new NpgsqlConnection(GetConnectionString());
        await connection.OpenAsync();
        await using var command = connection.CreateCommand();
        command.CommandText = """
            UPDATE "sweep_row_handler_status" AS status
            SET "NextAttemptAt" = now() - interval '1 second'
            FROM "sweep_run_row_detail" AS detail
            WHERE detail."Id" = status."SweepRunRowDetailId"
              AND detail."SweepId" = @sweepId
              AND status."State" = @pending
            """;
        command.Parameters.AddWithValue("sweepId", sweepId);
        command.Parameters.AddWithValue("pending", PendingState);
        (await command.ExecuteNonQueryAsync()).Should().BeGreaterThan(0);
    }

    private async Task SetRowDetailEntityIdAsync(Guid sweepId, Guid retentionEntityId)
    {
        await using var connection = new NpgsqlConnection(GetConnectionString());
        await connection.OpenAsync();
        await using var command = connection.CreateCommand();
        command.CommandText = """
            UPDATE "sweep_run_row_detail"
            SET "RetentionEntityId" = @retentionEntityId
            WHERE "SweepId" = @sweepId
            """;
        command.Parameters.AddWithValue("retentionEntityId", retentionEntityId);
        command.Parameters.AddWithValue("sweepId", sweepId);
        (await command.ExecuteNonQueryAsync()).Should().Be(1);
    }

    private async Task BackdateClaimAsync(Guid sweepId, DateTimeOffset claimedAt)
    {
        await using var connection = new NpgsqlConnection(GetConnectionString());
        await connection.OpenAsync();
        await using var command = connection.CreateCommand();
        command.CommandText = """
            UPDATE "sweep_row_handler_status" AS status
            SET "ClaimedAt" = @claimedAt
            FROM "sweep_run_row_detail" AS detail
            WHERE detail."Id" = status."SweepRunRowDetailId"
              AND detail."SweepId" = @sweepId
              AND status."State" = @inFlight
            """;
        command.Parameters.AddWithValue("claimedAt", claimedAt);
        command.Parameters.AddWithValue("sweepId", sweepId);
        command.Parameters.AddWithValue("inFlight", InFlightState);
        (await command.ExecuteNonQueryAsync()).Should().Be(1);
    }

    private async Task<Guid> LoadInFlightClaimTokenAsync(Guid sweepId)
    {
        await using var connection = new NpgsqlConnection(GetConnectionString());
        await connection.OpenAsync();
        await using var command = connection.CreateCommand();
        command.CommandText = """
            SELECT status."ClaimToken"
            FROM "sweep_row_handler_status" AS status
            INNER JOIN "sweep_run_row_detail" AS detail ON detail."Id" = status."SweepRunRowDetailId"
            WHERE detail."SweepId" = @sweepId AND status."State" = @inFlight
            """;
        command.Parameters.AddWithValue("sweepId", sweepId);
        command.Parameters.AddWithValue("inFlight", InFlightState);
        return (Guid)(await command.ExecuteScalarAsync())!;
    }

    private async Task<IReadOnlyList<long>> LoadHandlerStatusIdsAsync(Guid sweepId)
    {
        await using var connection = new NpgsqlConnection(GetConnectionString());
        await connection.OpenAsync();
        await using var command = connection.CreateCommand();
        command.CommandText = """
            SELECT status."Id"
            FROM "sweep_row_handler_status" AS status
            INNER JOIN "sweep_run_row_detail" AS detail ON detail."Id" = status."SweepRunRowDetailId"
            WHERE detail."SweepId" = @sweepId
            ORDER BY status."Id"
            """;
        command.Parameters.AddWithValue("sweepId", sweepId);
        var ids = new List<long>();
        await using var reader = await command.ExecuteReaderAsync();
        while (await reader.ReadAsync())
        {
            ids.Add(reader.GetInt64(0));
        }

        return ids;
    }

    private async Task AbandonClaimAsync(Guid sweepId, int attempt, DateTimeOffset claimedAt)
    {
        await using var connection = new NpgsqlConnection(GetConnectionString());
        await connection.OpenAsync();
        await using var command = connection.CreateCommand();
        command.CommandText = """
            UPDATE "sweep_row_handler_status" AS status
            SET "State" = @inFlight, "ClaimedAt" = @claimedAt, "ClaimToken" = gen_random_uuid(), "Attempt" = @attempt
            FROM "sweep_run_row_detail" AS detail
            WHERE detail."Id" = status."SweepRunRowDetailId"
              AND detail."SweepId" = @sweepId
              AND status."State" = @pending
            """;
        command.Parameters.AddWithValue("inFlight", InFlightState);
        command.Parameters.AddWithValue("pending", PendingState);
        command.Parameters.AddWithValue("claimedAt", claimedAt);
        command.Parameters.AddWithValue("attempt", attempt);
        command.Parameters.AddWithValue("sweepId", sweepId);
        (await command.ExecuteNonQueryAsync()).Should().Be(1);
    }

    private static ITestRetentionRuleProvider CreateHandlerErasureCategoryRepository(
        RetentionRule? shortLivedRule = null,
        RetentionRule? softDeleteRule = null,
        RetentionRule? anonymiseRule = null
    )
    {
        return new StaticCategoryRepository(
            new Dictionary<string, ITestRetentionRule>
            {
                ["short-lived"] = new StaticTestRetentionRule(
                    shortLivedRule
                        ?? new RetentionRule(
                            new RetentionPeriod(Days: 30),
                            Strategy.Purge,
                            AuditRowDetail: AuditRowDetail.PerRow
                        )
                ),
                ["soft-delete"] = new StaticTestRetentionRule(
                    softDeleteRule ?? new RetentionRule(new RetentionPeriod(Days: 30), Strategy.SoftDelete)
                ),
                ["anonymise"] = new StaticTestRetentionRule(
                    anonymiseRule ?? new RetentionRule(new RetentionPeriod(Days: 30), Strategy.Anonymise)
                ),
            }
        );
    }

    private sealed record CapturedRow(
        string EntityType,
        Guid RetentionEntityId,
        string RecordId,
        string CapturedPayload
    );

    private sealed record CapturedRowDetail(
        string RecordId,
        string Category,
        Strategy Strategy,
        Guid TenantId,
        DateTimeOffset At,
        string Body
    );

    private sealed record HandlerStatusRow(
        string HandlerType,
        int DispatchPhase,
        int State,
        int Attempt,
        DateTimeOffset NextAttemptAt,
        DateTimeOffset? ClaimedAt,
        DateTimeOffset? CompletedAt,
        string? LastError
    );

    private sealed record EntitySummaryRow(
        string EntityType,
        Strategy Strategy,
        int Affected,
        int HeldCount,
        int SkippedCount = 0
    );

    private static ServiceProvider BuildTrackedGraphServices(string connectionString)
    {
        var services = new ServiceCollection();
        services.AddSingleton<IConfiguration>(new ConfigurationBuilder().Build());
        services.AddLogging();
        services.AddDbContext<TrackedGraphDbContext>(options => options.UseNpgsql(connectionString));
        services.AddSingleton<IRetentionRuleProvider>(
            new StaticCategoryRepository(
                new Dictionary<string, ITestRetentionRule>
                {
                    ["tracked-graph"] = new StaticTestRetentionRule(
                        new RetentionRule(new RetentionPeriod(Days: 30), Strategy.Purge)
                    ),
                }
            )
        );
        services.AddSingleton(RetentionExecutionScope.Unisolated);
        services.AddSingleton<TrackedGraphProbe>();
        services.AddCohort<TrackedGraphDbContext>();
        services.AddRowHandler<TrackedGraphRecord, TrackedGraphHandler>();
        return services.BuildServiceProvider(validateScopes: true);
    }

    private static async Task SeedTrackedGraphAsync(
        ServiceProvider services,
        Guid tenantId,
        Guid? subjectId,
        params (string Body, DateTimeOffset CreatedAt)[] records
    )
    {
        await using var scope = services.CreateAsyncScope();
        var db = scope.ServiceProvider.GetRequiredService<TrackedGraphDbContext>();
        await db.Database.EnsureCreatedAsync();
        db.Folders.Add(new TrackedGraphFolder { Id = Guid.NewGuid() });
        db.Records.AddRange(
            records.Select(record => new TrackedGraphRecord
            {
                Id = Guid.NewGuid(),
                TenantId = tenantId,
                SubjectId = subjectId,
                CreatedAt = record.CreatedAt,
                Body = record.Body,
            })
        );
        await db.SaveChangesAsync();
    }

    /// <summary>
    /// What the caller's context might track that a handler could change: a record that isn't
    /// the subject's, a comment in the folder and a tag not yet on it.
    /// </summary>
    private static async Task<Guid> SeedTrackedGraphBystandersAsync(ServiceProvider services, Guid tenantId)
    {
        await using var scope = services.CreateAsyncScope();
        var db = scope.ServiceProvider.GetRequiredService<TrackedGraphDbContext>();
        var bystander = new TrackedGraphRecord
        {
            Id = Guid.NewGuid(),
            TenantId = tenantId,
            CreatedAt = DateTimeOffset.UtcNow,
            Body = TrackedGraphHandler.BystanderBody,
        };
        db.Records.Add(bystander);
        db.Comments.Add(new TrackedGraphComment { Id = Guid.NewGuid(), FolderId = (await db.Folders.SingleAsync()).Id });
        db.Tags.Add(new TrackedGraphTag { Id = Guid.NewGuid() });
        await db.SaveChangesAsync();
        return bystander.Id;
    }

    private sealed class TrackedGraphDbContext(DbContextOptions<TrackedGraphDbContext> options)
        : DbContext(options)
    {
        public DbSet<TrackedGraphRecord> Records => Set<TrackedGraphRecord>();
        public DbSet<TrackedGraphFolder> Folders => Set<TrackedGraphFolder>();
        public DbSet<TrackedGraphComment> Comments => Set<TrackedGraphComment>();
        public DbSet<TrackedGraphTag> Tags => Set<TrackedGraphTag>();

        protected override void OnModelCreating(ModelBuilder modelBuilder)
        {
            modelBuilder.Entity<TrackedGraphRecord>(entity =>
            {
                entity.ToTable("tracked_graph_records");
                entity.HasKey(record => record.Id);
            });
            modelBuilder.Entity<TrackedGraphFolder>(entity =>
            {
                entity.ToTable("tracked_graph_folders");
                entity.HasKey(folder => folder.Id);
                entity.HasMany(folder => folder.Comments).WithOne().HasForeignKey(comment => comment.FolderId);
                entity.HasMany(folder => folder.Tags).WithMany().UsingEntity(join => join.ToTable("tracked_graph_folder_tags"));
            });
            modelBuilder.Entity<TrackedGraphTag>(entity =>
            {
                entity.ToTable("tracked_graph_tags");
                entity.HasKey(tag => tag.Id);
            });
            modelBuilder.Entity<TrackedGraphComment>(entity =>
            {
                entity.ToTable("tracked_graph_comments");
                entity.HasKey(comment => comment.Id);
            });
            modelBuilder.ConfigureCohortTables();
        }
    }

    [Retain("tracked-graph", nameof(CreatedAt))]
    [RetentionEntityId("6d0f3a52-9a8e-4d1b-8f3c-2b7e5a1c9d40")]
    private sealed class TrackedGraphRecord
    {
        public Guid Id { get; set; }
        public Guid TenantId { get; init; }

        [ErasureSubject("person")]
        public Guid? SubjectId { get; init; }
        public DateTimeOffset CreatedAt { get; init; }
        public string Body { get; set; } = "";
    }

    /// <summary>Unretained, so only a handler's change can touch it.</summary>
    private sealed class TrackedGraphFolder
    {
        public Guid Id { get; init; }
        public List<TrackedGraphComment> Comments { get; init; } = [];
        public List<TrackedGraphTag> Tags { get; init; } = [];
    }

    private sealed class TrackedGraphTag
    {
        public Guid Id { get; init; }
    }

    private sealed class TrackedGraphComment
    {
        public Guid Id { get; init; }
        public Guid FolderId { get; init; }
    }

    private sealed class TrackedGraphProbe
    {
        public List<bool> SawOwnRow { get; } = [];

        public List<DbContext> Contexts { get; } = [];
    }

    /// <summary>
    /// Changes its own context in one way, chosen by the row's body, without saving (or with a
    /// save read-only refuses): the kinds of change that once leaked into the caller's context.
    /// </summary>
    [RowHandlerId("fc207c57-c353-409e-ad7d-35a90f7a9bad")]
    private sealed class TrackedGraphHandler(TrackedGraphDbContext db, TrackedGraphProbe probe)
        : IRetentionHandler<TrackedGraphRecord>
    {
        public const string BystanderBody = "bystander";
        public const string StagingBody = "stage-before-delete";
        public const string FailedSaveBody = "save-before-delete";
        public const string KeyChangingBody = "change-key-before-delete";
        public const string EditingBody = "edit-before-delete";
        public const string UndetectedEditBody = "edit-undetected-before-delete";
        public const string AddingBody = "add-comment-before-delete";
        public const string ManyToManyBody = "tag-folder-before-delete";
        public const string TrackingReadBody = "read-tracking-before-delete";
        public const string ClearingBody = "clear-before-delete";
        public const string ReadingOwnRowBody = "read-own-row-before-delete";

        public async Task OnBeforeAsync(TrackedGraphRecord row, RetentionBeforeContext ctx, CancellationToken ct)
        {
            switch (row.Body)
            {
                case StagingBody:
                    db.Records.Add(new TrackedGraphRecord { Id = Guid.NewGuid(), TenantId = row.TenantId, Body = "staged-by-handler" });
                    break;
                case FailedSaveBody:
                    // The save fails read-only (25006) and leaves its entry Modified.
                    (await BystanderAsync(ct)).Body = "overwritten-by-handler";
                    await db.SaveChangesAsync(ct);
                    break;
                case KeyChangingBody:
                    (await BystanderAsync(ct)).Id = Guid.NewGuid();
                    break;
                case EditingBody:
                    (await BystanderAsync(ct)).Body = "overwritten-by-handler";
                    break;
                case UndetectedEditBody:
                    db.ChangeTracker.AutoDetectChangesEnabled = false;
                    (await BystanderAsync(ct)).Body = "overwritten-by-handler";
                    break;
                case AddingBody:
                    (await db.Folders.SingleAsync(ct)).Comments.Add(new TrackedGraphComment { Id = Guid.NewGuid() });
                    break;
                case ManyToManyBody:
                    var folder = await db.Folders.Include(item => item.Tags).SingleAsync(ct);
                    folder.Tags.Add(await db.Tags.SingleAsync(ct));
                    break;
                case TrackingReadBody:
                    await db.Comments.SingleAsync(ct);
                    break;
                case ClearingBody:
                    await BystanderAsync(ct);
                    db.ChangeTracker.Clear();
                    break;
                case ReadingOwnRowBody:
                    probe.SawOwnRow.Add(await db.Records.AsNoTracking().AnyAsync(record => record.Id == row.Id, ct));
                    probe.Contexts.Add(db);
                    break;
            }

            ctx.Snapshot["body"] = row.Body;
        }

        private Task<TrackedGraphRecord> BystanderAsync(CancellationToken ct) =>
            db.Records.SingleAsync(record => record.Body == BystanderBody, ct);
    }

    private sealed class StaticCategoryRepository(
        IReadOnlyDictionary<string, ITestRetentionRule> resolvers
    ) : ITestRetentionRuleProvider
    {
        private static readonly ITestRetentionRule ExemptFallback =
            new StaticTestRetentionRule(
                new RetentionRule(new RetentionPeriod(Days: 30), Strategy.Exempt)
            );

        public Task<ITestRetentionRule?> GetAsync(string category, CancellationToken ct)
        {
            return resolvers.TryGetValue(category, out var resolver)
                ? Task.FromResult<ITestRetentionRule?>(resolver)
                : Task.FromResult<ITestRetentionRule?>(ExemptFallback);
        }
    }
}

[RowHandlerId(NoteHandlerBeforeRename.Id)]
file sealed class NoteHandlerBeforeRename(HandlerExecutionSink sink) : IRetentionHandler<Note>
{
    public const string Id = "0f3c8a52-6e1b-4d7a-9c2e-5b8d1f4a7e63";

    public Task OnBeforeAsync(Note row, RetentionBeforeContext ctx, CancellationToken ct)
    {
        ctx.Snapshot["body"] = row.Body;
        return Task.CompletedTask;
    }

    public Task OnAfterAsync(RetentionAfterContext<Note> ctx, CancellationToken ct)
    {
        sink.AfterCalls.Add($"original:{ctx.Snapshot["body"]}:{ctx.Attempt}");
        return Task.CompletedTask;
    }
}

[RowHandlerId(NoteHandlerBeforeRename.Id)]
file sealed class NoteHandlerAfterRename(HandlerExecutionSink sink) : IRetentionHandler<Note>
{
    public Task OnAfterAsync(RetentionAfterContext<Note> ctx, CancellationToken ct)
    {
        sink.AfterCalls.Add($"renamed:{ctx.Snapshot["body"]}:{ctx.Attempt}");
        return Task.CompletedTask;
    }
}

file sealed class DownstreamOutage
{
    public volatile bool Down;
}

[RowHandlerPriority(10)]
[RowHandlerId("5e0d2b7a-3c41-4f8e-9a6d-1b2c7e4f9a35")]
file sealed class OutageSensitiveNoteHandler(DownstreamOutage outage, HandlerExecutionSink sink)
    : IRetentionHandler<Note>
{
    public Task OnBeforeAsync(Note row, RetentionBeforeContext ctx, CancellationToken ct)
    {
        ctx.Snapshot["body"] = row.Body;
        return Task.CompletedTask;
    }

    public Task OnAfterAsync(RetentionAfterContext<Note> ctx, CancellationToken ct)
    {
        if (outage.Down)
        {
            throw new InvalidOperationException("Simulated downstream outage.");
        }

        sink.AfterCalls.Add($"after:{ctx.Snapshot["body"]}:{ctx.Attempt}");
        return Task.CompletedTask;
    }
}

/// <summary>Blocks on the gate and ignores its cancellation token, as a misbehaving handler would.</summary>
[RowHandlerPriority(10)]
[RowHandlerId("a7c3e9f1-2b84-4d6a-8e15-c9f0d3b2a648")]
file sealed class TokenIgnoringBlockingNoteHandler(HandlerExecutionSink sink, DispatchBlockGate gate)
    : IRetentionHandler<Note>
{
    public Task OnBeforeAsync(Note row, RetentionBeforeContext ctx, CancellationToken ct)
    {
        ctx.Snapshot["body"] = row.Body;
        return Task.CompletedTask;
    }

    public async Task OnAfterAsync(RetentionAfterContext<Note> ctx, CancellationToken ct)
    {
        await gate.WaitForReleaseAsync(CancellationToken.None);
        sink.AfterCalls.Add($"after-{ctx.Snapshot["body"]}:{ctx.Attempt}");
    }
}

file sealed class HandlerExecutionSink
{
    public List<string> BeforeCalls { get; } = [];

    public List<string> AfterCalls { get; } = [];
}

file sealed class BlobCleanupStoreSpy
{
    public List<string> DeletedPaths { get; } = [];

    public Task DeleteAsync(string storagePath)
    {
        DeletedPaths.Add(storagePath);
        return Task.CompletedTask;
    }
}

file sealed class DispatchAttemptTracker
{
    private readonly object gate = new();
    private readonly Dictionary<string, int> attempts = new(StringComparer.Ordinal);

    public int Increment(string recordId)
    {
        lock (gate)
        {
            var next = attempts.TryGetValue(recordId, out var current) ? current + 1 : 1;
            attempts[recordId] = next;
            return next;
        }
    }

    public int LoadAttempt(string recordId)
    {
        lock (gate)
        {
            return attempts.TryGetValue(recordId, out var attempt) ? attempt : 0;
        }
    }
}

file sealed class DispatchBlockGate
{
    private readonly TaskCompletionSource blocked = new(
        TaskCreationOptions.RunContinuationsAsynchronously
    );
    private volatile bool released;

    public Task WaitUntilBlockedAsync()
    {
        return blocked.Task;
    }

    public async Task WaitForReleaseAsync(CancellationToken ct)
    {
        blocked.TrySetResult();

        while (!released)
        {
            await Task.Delay(10, ct);
        }
    }

    public void Release()
    {
        released = true;
    }
}

file sealed class AfterContextRecorder
{
    private readonly object gate = new();
    private readonly List<AfterContextCall> calls = [];

    public void Record(AfterContextCall call)
    {
        lock (gate)
        {
            calls.Add(call);
        }
    }

    public IReadOnlyList<AfterContextCall> Load()
    {
        lock (gate)
        {
            return calls.ToArray();
        }
    }
}

file sealed class TypedSnapshotRecorder
{
    private readonly object gate = new();
    private readonly List<TypedSnapshotAfterCall> calls = [];

    public void Record(TypedSnapshotAfterCall call)
    {
        lock (gate)
        {
            calls.Add(call);
        }
    }

    public IReadOnlyList<TypedSnapshotAfterCall> Load()
    {
        lock (gate)
        {
            return calls.ToArray();
        }
    }
}

file sealed record AfterContextCall(
    Guid SweepId,
    string RecordId,
    string Category,
    Strategy Strategy,
    Guid TenantId,
    DateTimeOffset At,
    int Attempt,
    string Body,
    Guid ScopeInstanceId
);

file sealed record TypedSnapshotPayload(Guid NoteId, DateTimeOffset RecordedAt, string Body);

file sealed record TypedSnapshotAfterCall(
    string RecordId,
    DateTimeOffset At,
    TypedSnapshotPayload Payload
);

file sealed class CollectionSnapshotRecorder
{
    public IDictionary<string, object?>? Snapshot { get; set; }
}

[RowHandlerId("8d5277a6-f37c-4b3a-a2a3-4289f6b2f3f3")]
file sealed class CollectionSnapshotNoteHandler(CollectionSnapshotRecorder recorder)
    : IRetentionHandler<Note>
{
    public Task OnBeforeAsync(Note row, RetentionBeforeContext ctx, CancellationToken ct)
    {
        ctx.Snapshot["paths"] = new[] { "a/1", "b/2" };
        ctx.Snapshot["tags"] = new List<string> { "x", "y" };
        ctx.Snapshot["sizes"] = new[] { 1, 2, 3 };
        return Task.CompletedTask;
    }

    public Task OnAfterAsync(RetentionAfterContext<Note> ctx, CancellationToken ct)
    {
        recorder.Snapshot = new Dictionary<string, object?>(ctx.Snapshot);
        return Task.CompletedTask;
    }
}

file sealed class UnsupportedSnapshotValue
{
    public string Body { get; set; } = "";
}

[RowHandlerId("7e0800f3-c8e8-4214-a4f9-6c3b21c4098b")]
file sealed class UnsupportedSnapshotNoteHandler(CollectionSnapshotRecorder recorder)
    : IRetentionHandler<Note>
{
    public Task OnBeforeAsync(Note row, RetentionBeforeContext ctx, CancellationToken ct)
    {
        ctx.Snapshot["value"] = new UnsupportedSnapshotValue { Body = row.Body };
        return Task.CompletedTask;
    }

    public Task OnAfterAsync(RetentionAfterContext<Note> ctx, CancellationToken ct)
    {
        recorder.Snapshot = new Dictionary<string, object?>(ctx.Snapshot);
        return Task.CompletedTask;
    }
}

file sealed class ScopedDispatchProbe
{
    public Guid InstanceId { get; } = Guid.NewGuid();
}

[RowHandlerPriority(20)]
[RowHandlerId("ecc83692-1503-47b8-bb39-c982311bc4ad")]
file sealed class LowPriorityNoteHandler(HandlerExecutionSink sink) : IRetentionHandler<Note>
{
    public Task OnBeforeAsync(Note row, RetentionBeforeContext ctx, CancellationToken ct)
    {
        sink.BeforeCalls.Add("low");
        ctx.Snapshot["body"] = row.Body;
        return Task.CompletedTask;
    }
}

[RowHandlerPriority(20)]
[RowHandlerId("d096b74c-94e8-48be-897b-00464c4b88f7")]
file sealed class LowPriorityAfterNoteHandler(HandlerExecutionSink sink) : IRetentionHandler<Note>
{
    public Task OnBeforeAsync(Note row, RetentionBeforeContext ctx, CancellationToken ct)
    {
        ctx.Snapshot["body"] = row.Body;
        return Task.CompletedTask;
    }

    public Task OnAfterAsync(RetentionAfterContext<Note> ctx, CancellationToken ct)
    {
        sink.AfterCalls.Add("after-low");
        return Task.CompletedTask;
    }
}

[RowHandlerId("988fd987-0a06-4e6b-8b00-cf56f94ada23")]
file sealed class BlobBackedFileCleanupHandler(BlobCleanupStoreSpy cleanupStore)
    : IRetentionHandler<BlobBackedFile>
{
    public Task OnBeforeAsync(BlobBackedFile row, RetentionBeforeContext ctx, CancellationToken ct)
    {
        ctx.Snapshot["storagePath"] = row.StoragePath;
        ctx.Snapshot["originalFileName"] = row.OriginalFileName;
        return Task.CompletedTask;
    }

    public Task OnAfterAsync(RetentionAfterContext<BlobBackedFile> ctx, CancellationToken ct)
    {
        return cleanupStore.DeleteAsync((string)ctx.Snapshot["storagePath"]!);
    }
}

[RowHandlerPriority(10)]
[RowHandlerId("097a463f-15ed-46c1-a283-a7dae1d3cfdd")]
file sealed class BlockingHighPriorityAfterNoteHandler(
    HandlerExecutionSink sink,
    DispatchBlockGate gate
) : IRetentionHandler<Note>
{
    public Task OnBeforeAsync(Note row, RetentionBeforeContext ctx, CancellationToken ct)
    {
        ctx.Snapshot["body"] = row.Body;
        return Task.CompletedTask;
    }

    public async Task OnAfterAsync(RetentionAfterContext<Note> ctx, CancellationToken ct)
    {
        await gate.WaitForReleaseAsync(ct);
        sink.AfterCalls.Add("after-high-blocking");
    }
}

[RowHandlerPriority(10)]
[RowHandlerId("72cddb27-f1ac-40e4-b7c9-60f67c64e12c")]
file sealed class BlockingThenFailingHighPriorityNoteHandler(DispatchBlockGate gate)
    : IRetentionHandler<Note>
{
    public Task OnBeforeAsync(Note row, RetentionBeforeContext ctx, CancellationToken ct)
    {
        ctx.Snapshot["body"] = row.Body;
        return Task.CompletedTask;
    }

    public async Task OnAfterAsync(RetentionAfterContext<Note> ctx, CancellationToken ct)
    {
        await gate.WaitForReleaseAsync(ct);
        throw new InvalidOperationException("stale delivery failed");
    }
}

[RowHandlerPriority(10)]
[RowHandlerId("f5a079b0-6d26-4ea3-82aa-21ccf81ef9ed")]
file sealed class HighPriorityNoteHandler(HandlerExecutionSink sink) : IRetentionHandler<Note>
{
    public Task OnBeforeAsync(Note row, RetentionBeforeContext ctx, CancellationToken ct)
    {
        sink.BeforeCalls.Add("high");
        ctx.Snapshot["priority"] = "high-first";
        return Task.CompletedTask;
    }
}

[RowHandlerId("8a6c9a72-feb4-4430-976b-331db090867a")]
file sealed class SelectivelyFailingNoteHandler : IRetentionHandler<Note>
{
    public const string FailingBody = "fail-before-delete";

    public Task OnBeforeAsync(Note row, RetentionBeforeContext ctx, CancellationToken ct)
    {
        if (string.Equals(row.Body, FailingBody, StringComparison.Ordinal))
        {
            throw new InvalidOperationException("Simulated handler failure.");
        }

        ctx.Snapshot["body"] = row.Body;
        return Task.CompletedTask;
    }
}

file sealed record WriteTarget(Guid NoteId);

/// <summary>Throws its own cancellation, as an HttpClient timeout does, chosen by the row's body.</summary>
[RowHandlerId("c2e08c92-7a1d-459f-b2e9-e4d12ae83690")]
file sealed class TimingOutNoteHandler : IRetentionHandler<Note>
{
    public const string SynchronousBody = "time-out-synchronously";
    public const string AsynchronousBody = "time-out-asynchronously";

    public Task OnBeforeAsync(Note row, RetentionBeforeContext ctx, CancellationToken ct)
    {
        if (string.Equals(row.Body, SynchronousBody, StringComparison.Ordinal))
        {
            throw new TaskCanceledException("Simulated HttpClient timeout.");
        }

        return string.Equals(row.Body, AsynchronousBody, StringComparison.Ordinal)
            ? TimeOutAsync()
            : Task.CompletedTask;
    }

    private static async Task TimeOutAsync()
    {
        await Task.Yield();
        throw new TaskCanceledException("Simulated HttpClient timeout.");
    }
}

/// <summary>
/// Touches the database from OnBefore through the context it's given, which is on the sweep's
/// connection and transaction: a failing statement, a write, or a read of a related row,
/// chosen by the row's body.
/// </summary>
[RowHandlerId("c17d5835-7c3b-4cb5-91fb-465ea355552c")]
file sealed class DatabaseTouchingNoteHandler(SampleDbContext db, WriteTarget target, IServiceProvider services)
    : IRetentionHandler<Note>
{
    public const string DivideByZeroBody = "divide-by-zero-before-delete";
    public const string WritingBody = "write-before-delete";
    public const string ReadingBody = "read-before-delete";

    public async Task OnBeforeAsync(Note row, RetentionBeforeContext ctx, CancellationToken ct)
    {
        switch (row.Body)
        {
            case DivideByZeroBody:
                await db.Database.ExecuteSqlRawAsync("SELECT 1 / 0", ct);
                break;
            case WritingBody:
                await db.Notes
                    .Where(note => note.Id == target.NoteId)
                    .ExecuteUpdateAsync(setters => setters.SetProperty(note => note.Body, "overwritten-by-handler"), ct);
                break;
            case ReadingBody:
                ctx.Snapshot["related"] = await db.Notes
                    .AsNoTracking()
                    .Where(note => note.Id == target.NoteId)
                    .Select(note => note.Body)
                    .SingleAsync(ct);
                break;
        }

        ctx.Snapshot["body"] = row.Body;
    }

    public Task OnAfterAsync(RetentionAfterContext<Note> ctx, CancellationToken ct)
    {
        if (services.GetService<CollectionSnapshotRecorder>() is { } recorder)
        {
            recorder.Snapshot = new Dictionary<string, object?>(ctx.Snapshot);
        }
        return Task.CompletedTask;
    }
}

[RowHandlerId("97eaff4f-f169-40c3-945d-bf82dc0e557b")]
file sealed class NoteErasureTrackingHandler(HandlerExecutionSink sink) : IRetentionHandler<Note>
{
    public Task OnBeforeAsync(Note row, RetentionBeforeContext ctx, CancellationToken ct)
    {
        sink.BeforeCalls.Add($"note:{row.Body}");
        ctx.Snapshot["body"] = row.Body;
        return Task.CompletedTask;
    }

    public Task OnAfterAsync(RetentionAfterContext<Note> ctx, CancellationToken ct)
    {
        sink.AfterCalls.Add($"after-note:{ctx.Snapshot["body"]}:{ctx.Attempt}");
        return Task.CompletedTask;
    }
}

[RowHandlerId("2e1df983-67b6-43d6-a7bb-a74bbdd91bda")]
file sealed class SoftDeleteErasureTrackingHandler(HandlerExecutionSink sink)
    : IRetentionHandler<SoftDeleteRecord>
{
    public Task OnBeforeAsync(
        SoftDeleteRecord row,
        RetentionBeforeContext ctx,
        CancellationToken ct
    )
    {
        sink.BeforeCalls.Add($"soft:{row.Body}");
        ctx.Snapshot["body"] = row.Body;
        return Task.CompletedTask;
    }

    public Task OnAfterAsync(RetentionAfterContext<SoftDeleteRecord> ctx, CancellationToken ct)
    {
        sink.AfterCalls.Add($"after-soft:{ctx.Snapshot["body"]}:{ctx.Attempt}");
        return Task.CompletedTask;
    }
}

[RowHandlerId("a5d96800-ecca-4d63-a5f7-7168d8a251fa")]
file sealed class AnonymisedContactErasureTrackingHandler(HandlerExecutionSink sink)
    : IRetentionHandler<AnonymisedContact>
{
    public Task OnBeforeAsync(
        AnonymisedContact row,
        RetentionBeforeContext ctx,
        CancellationToken ct
    )
    {
        sink.BeforeCalls.Add($"contact:{row.EmailAddress}");
        ctx.Snapshot["email"] = row.EmailAddress;
        return Task.CompletedTask;
    }

    public Task OnAfterAsync(RetentionAfterContext<AnonymisedContact> ctx, CancellationToken ct)
    {
        sink.AfterCalls.Add($"after-contact:{ctx.Snapshot["email"]}:{ctx.Attempt}");
        return Task.CompletedTask;
    }
}

[RowHandlerId("4620a3a9-2334-4e16-a0fe-d91c619b20f8")]
file sealed class DispatchRecordingNoteHandler(HandlerExecutionSink sink) : IRetentionHandler<Note>
{
    public Task OnBeforeAsync(Note row, RetentionBeforeContext ctx, CancellationToken ct)
    {
        ctx.Snapshot["body"] = row.Body;
        return Task.CompletedTask;
    }

    public async Task OnAfterAsync(RetentionAfterContext<Note> ctx, CancellationToken ct)
    {
        await Task.Delay(100, ct);
        sink.AfterCalls.Add($"after:{ctx.Snapshot["body"]}:{ctx.Attempt}");
    }
}

[RowHandlerId("c438bb3b-e999-4ea2-affe-f5f66551a426")]
file sealed class ContextCapturingNoteHandler(
    AfterContextRecorder recorder,
    ScopedDispatchProbe probe
) : IRetentionHandler<Note>
{
    public Task OnBeforeAsync(Note row, RetentionBeforeContext ctx, CancellationToken ct)
    {
        ctx.Snapshot["body"] = row.Body;
        return Task.CompletedTask;
    }

    public Task OnAfterAsync(RetentionAfterContext<Note> ctx, CancellationToken ct)
    {
        recorder.Record(
            new AfterContextCall(
                ctx.SweepId,
                ctx.RecordId,
                ctx.Category,
                ctx.Strategy,
                ctx.TenantId,
                ctx.At,
                ctx.Attempt,
                (string)ctx.Snapshot["body"]!,
                probe.InstanceId
            )
        );

        return Task.CompletedTask;
    }
}

[RowHandlerId("557a2d8b-1c03-4504-9cdc-96dddffc4dc6")]
file sealed class TypedSnapshotNoteHandler(TypedSnapshotRecorder recorder) : IRetentionHandler<Note>
{
    public Task OnBeforeAsync(Note row, RetentionBeforeContext ctx, CancellationToken ct)
    {
        ctx.Snapshot["payload"] = new TypedSnapshotPayload(row.Id, ctx.At, row.Body);
        return Task.CompletedTask;
    }

    public Task OnAfterAsync(RetentionAfterContext<Note> ctx, CancellationToken ct)
    {
        recorder.Record(
            new TypedSnapshotAfterCall(
                ctx.RecordId,
                ctx.At,
                (TypedSnapshotPayload)ctx.Snapshot["payload"]!
            )
        );

        return Task.CompletedTask;
    }
}

[RowHandlerId("44344a40-ada3-4e8b-909d-ca361d5d2f4d")]
file sealed class AlwaysFailingDispatchNoteHandler(DispatchAttemptTracker tracker)
    : IRetentionHandler<Note>
{
    public Task OnBeforeAsync(Note row, RetentionBeforeContext ctx, CancellationToken ct)
    {
        ctx.Snapshot["body"] = row.Body;
        return Task.CompletedTask;
    }

    public Task OnAfterAsync(RetentionAfterContext<Note> ctx, CancellationToken ct)
    {
        tracker.Increment(ctx.RecordId);
        throw new InvalidOperationException("Simulated permanent after-dispatch failure.");
    }
}

[RowHandlerId("878fa3c5-fbc0-4cfc-8396-5538620ebb4a")]
file sealed class FailsWhileSiblingBlocksNoteHandler(HandlerExecutionSink sink, DispatchBlockGate gate)
    : IRetentionHandler<Note>
{
    public Task OnBeforeAsync(Note row, RetentionBeforeContext ctx, CancellationToken ct)
    {
        ctx.Snapshot["body"] = row.Body;
        return Task.CompletedTask;
    }

    public async Task OnAfterAsync(RetentionAfterContext<Note> ctx, CancellationToken ct)
    {
        if ((string?)ctx.Snapshot["body"] == "blocks")
        {
            await gate.WaitForReleaseAsync(ct);
            sink.AfterCalls.Add($"after:{ctx.Snapshot["body"]}:{ctx.Attempt}");
            return;
        }

        await gate.WaitUntilBlockedAsync().WaitAsync(ct);
        throw new InvalidOperationException("Simulated handler failure.");
    }
}

[RowHandlerId("ff39af63-5a08-4738-95bb-654df181426e")]
file sealed class BlockingDispatchNoteHandler(HandlerExecutionSink sink, DispatchBlockGate gate)
    : IRetentionHandler<Note>
{
    public Task OnBeforeAsync(Note row, RetentionBeforeContext ctx, CancellationToken ct)
    {
        ctx.Snapshot["body"] = row.Body;
        return Task.CompletedTask;
    }

    public async Task OnAfterAsync(RetentionAfterContext<Note> ctx, CancellationToken ct)
    {
        await gate.WaitForReleaseAsync(ct);
        sink.AfterCalls.Add($"after:{ctx.Snapshot["body"]}:{ctx.Attempt}");
    }
}

