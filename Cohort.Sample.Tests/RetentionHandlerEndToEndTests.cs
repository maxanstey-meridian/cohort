using System.Text.Json;
using Cohort.Application;
using Cohort.Domain;
using Cohort.Hosting;
using Cohort.Infrastructure;
using Cohort.Sample.Entities;
using Microsoft.EntityFrameworkCore;
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
            await dispatcher.FlushAsync();
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
                new EntitySweepCount(typeof(Note), "short-lived", tenantId, Strategy.Purge, 1)
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
                type.Contains(nameof(HighPriorityNoteHandler), StringComparison.Ordinal)
            );
        statuses
            .Select(status => status.HandlerType)
            .Should()
            .Contain(type =>
                type.Contains(nameof(LowPriorityNoteHandler), StringComparison.Ordinal)
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
                row.HandlerType.Contains(
                    nameof(BlockingHighPriorityAfterNoteHandler),
                    StringComparison.Ordinal
                )
                && row.State == InFlightState,
            TimeSpan.FromSeconds(5)
        );
        inFlightHigh.Attempt.Should().Be(1);

        var interimStatuses = await LoadHandlerStatusesAsync(result.SweepId);
        interimStatuses
            .Should()
            .ContainSingle(status =>
                status.HandlerType.Contains(
                    nameof(BlockingHighPriorityAfterNoteHandler),
                    StringComparison.Ordinal
                )
                && status.State == InFlightState
                && status.Attempt == 1
                && status.ClaimedAt != null
                && status.CompletedAt == null
            );
        interimStatuses
            .Should()
            .ContainSingle(status =>
                status.HandlerType.Contains(
                    nameof(LowPriorityAfterNoteHandler),
                    StringComparison.Ordinal
                )
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
                services.AddRowHandler<Note, BlockingHighPriorityAfterNoteHandler>();
            }
        );
        var result = await handlerHost.RunSweepAsync(
            new TenantContext(tenantId, "uk", new Dictionary<string, string>()),
            asOf
        );

        var flushTask = handlerHost.RunWithServicesAsync(async serviceProvider =>
        {
            await serviceProvider.GetRequiredService<IRetentionRowDispatcher>().FlushAsync();
        });
        await gate.WaitUntilBlockedAsync().WaitAsync(TimeSpan.FromSeconds(5));

        var newerClaimToken = await ReplaceClaimOwnerAsync(result.SweepId);
        gate.Release();
        var exception = await Assert.ThrowsAsync<RetentionRowDispatchClaimLostException>(() =>
            flushTask.WaitAsync(TimeSpan.FromSeconds(5))
        );
        exception.StatusId.Should().BeGreaterThan(0);

        var status = (await LoadHandlerStatusesAsync(result.SweepId)).Single();
        status.State.Should().Be(InFlightState);
        status.Attempt.Should().Be(2);
        status.ClaimToken.Should().Be(newerClaimToken);
        status.CompletedAt.Should().BeNull();
        status.LastError.Should().BeNull();
        (await CountRemainingCapturedPayloadsAsync(result.SweepId)).Should().Be(1);
    }

    [Fact]
    public async Task FlushAsync_Heartbeat_Claim_Loss_Cancels_The_Running_Handler()
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
                    Body = "heartbeat-fenced-claim",
                }
            );
            await db.SaveChangesAsync();
        }

        using var handlerHost = new CohortTestHost(
            GetConnectionString(),
            configurationOverrides: new Dictionary<string, string?>
            {
                [$"{CohortOptions.SectionName}:RowHandlerDispatch:ClaimTimeout"] = "00:00:30",
            },
            configureServices: services =>
            {
                services.AddSingleton(gate);
                services.AddSingleton(sink);
                services.AddRowHandler<Note, BlockingHighPriorityAfterNoteHandler>();
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

        var newerClaimToken = await ReplaceClaimOwnerAsync(result.SweepId);
        var exception = await Assert.ThrowsAsync<RetentionRowDispatchClaimLostException>(() =>
            flushTask.WaitAsync(TimeSpan.FromSeconds(15))
        );

        exception.StatusId.Should().BeGreaterThan(0);
        sink.AfterCalls.Should().BeEmpty();
        var status = (await LoadHandlerStatusesAsync(result.SweepId)).Single();
        status.State.Should().Be(InFlightState);
        status.ClaimToken.Should().Be(newerClaimToken);
        status.CompletedAt.Should().BeNull();
    }

    [Fact]
    public async Task FlushAsync_Success_Waits_For_A_Blocked_Heartbeat_Without_Reporting_Claim_Loss()
    {
        var tenantId = Guid.NewGuid();
        var asOf = new DateTimeOffset(2026, 4, 13, 12, 0, 0, TimeSpan.Zero);
        var sink = new HandlerExecutionSink();
        var blocker = new StatusUpdateBlocker(GetConnectionString(), holdHandler: true);

        await using (var db = Host.CreateDbContext())
        {
            db.Notes.Add(
                new Note
                {
                    Id = Guid.NewGuid(),
                    TenantId = tenantId,
                    CreatedAt = asOf.AddDays(-120),
                    Body = "success-heartbeat-race",
                }
            );
            await db.SaveChangesAsync();
        }

        using var handlerHost = new CohortTestHost(
            GetConnectionString(),
            configurationOverrides: new Dictionary<string, string?>
            {
                [$"{CohortOptions.SectionName}:RowHandlerDispatch:ClaimTimeout"] = "00:00:30",
            },
            configureServices: services =>
            {
                services.AddSingleton(sink);
                services.AddSingleton(blocker);
                services.AddRowHandler<Note, LocksStatusThenReturnsNoteHandler>();
            }
        );
        var result = await handlerHost.RunSweepAsync(
            new TenantContext(tenantId, "uk", new Dictionary<string, string>()),
            asOf
        );

        var flushTask = handlerHost.RunWithServicesAsync(async serviceProvider =>
            await serviceProvider.GetRequiredService<IRetentionRowDispatcher>().FlushAsync()
        );
        await blocker.WaitUntilLockedAsync().WaitAsync(TimeSpan.FromSeconds(5));

        await WaitForBlockedStatusUpdatesAsync(blocker.BackendId, expectedCount: 1);
        blocker.AllowHandlerToReturn();
        await blocker.ReleaseAsync();
        await flushTask.WaitAsync(TimeSpan.FromSeconds(5));

        sink.AfterCalls.Should().Equal("after:success-heartbeat-race:1");
        var status = (await LoadHandlerStatusesAsync(result.SweepId)).Single();
        status.State.Should().Be(SucceededState);
        status.Attempt.Should().Be(1);
        status.ClaimToken.Should().BeNull();
        status.CompletedAt.Should().NotBeNull();
        status.LastError.Should().BeNull();
    }

    private async Task WaitForBlockedStatusUpdatesAsync(int blockerBackendId, int expectedCount)
    {
        using var timeout = new CancellationTokenSource(TimeSpan.FromSeconds(15));
        await using var connection = new NpgsqlConnection(GetConnectionString());
        await connection.OpenAsync(timeout.Token);

        while (true)
        {
            await using var command = connection.CreateCommand();
            command.CommandText = """
                SELECT COUNT(*)
                FROM pg_stat_activity waiter
                WHERE waiter.query LIKE '%UPDATE %sweep_row_handler_status%'
                  AND (
                      @blockerBackendId = ANY(pg_blocking_pids(waiter.pid))
                      OR EXISTS (
                          SELECT 1
                          FROM unnest(pg_blocking_pids(waiter.pid)) AS immediate_blocker(pid)
                          WHERE @blockerBackendId = ANY(pg_blocking_pids(immediate_blocker.pid))
                      )
                  )
                """;
            command.Parameters.AddWithValue("blockerBackendId", blockerBackendId);
            if ((long)(await command.ExecuteScalarAsync(timeout.Token))! >= expectedCount)
            {
                return;
            }

            await Task.Delay(TimeSpan.FromMilliseconds(20), timeout.Token);
        }
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

        result.EntityFailures.Should().BeEmpty();
        result
            .Counts.Should()
            .Contain(
                new EntitySweepCount(
                    typeof(Note),
                    "short-lived",
                    tenantId,
                    Strategy.Purge,
                    1,
                    SkippedCount: 1
                )
            );

        await using (var verify = Host.CreateDbContext())
        {
            (await verify.Notes.AnyAsync(note => note.Id == healthyNoteId)).Should().BeFalse();
            (await verify.Notes.AnyAsync(note => note.Id == failingNoteId)).Should().BeTrue();
        }
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
                services.AddRowHandler<ErasureSubjectRecord, ExemptErasureTrackingHandler>();
            }
        );

        var result = await handlerHost.RunErasureAsync(
            new TenantContext(tenantId, "uk", new Dictionary<string, string>()),
            new ErasureScope(subjectId, allowSoftDeleteAsErasure: true),
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
                    HeldCount: 1
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
                    HeldCount: 1
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
                    HeldCount: 1
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
                type.Contains(nameof(NoteErasureTrackingHandler), StringComparison.Ordinal)
            );
        statuses
            .Select(status => status.HandlerType)
            .Should()
            .Contain(type =>
                type.Contains(nameof(SoftDeleteErasureTrackingHandler), StringComparison.Ordinal)
            );
        statuses
            .Select(status => status.HandlerType)
            .Should()
            .Contain(type =>
                type.Contains(
                    nameof(AnonymisedContactErasureTrackingHandler),
                    StringComparison.Ordinal
                )
            );
        statuses
            .Select(status => status.HandlerType)
            .Should()
            .NotContain(type =>
                type.Contains(nameof(ExemptErasureTrackingHandler), StringComparison.Ordinal)
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
        sink.AfterCalls.Should()
            .NotContain(call => call.Contains("exempt", StringComparison.Ordinal));

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
                services.AddRowHandler<ErasureSubjectRecord, ExemptErasureTrackingHandler>();
            }
        );

        var result = await handlerHost.RunErasureAsync(
            new TenantContext(tenantId, "uk", new Dictionary<string, string>()),
            new ErasureScope(subjectId, allowSoftDeleteAsErasure: true, dryRun: true),
            asOf
        );

        result
            .Counts.Should()
            .Contain(
                new EntitySweepCount(typeof(Note), "short-lived", tenantId, Strategy.Purge, 1)
            );
        result
            .Counts.Should()
            .Contain(
                new EntitySweepCount(
                    typeof(SoftDeleteRecord),
                    "soft-delete",
                    tenantId,
                    Strategy.SoftDelete,
                    1
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
                    1
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
            claimedStatus.ClaimToken.Should().NotBeNull();

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
        statuses[0].ClaimToken.Should().BeNull();
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
        // claimed, so it could not sit unheartbeated behind it and expire.
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
    public async Task FlushAsync_DeadLetters_Exhausted_Handler_Failures()
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

        await handlerHost.RunWithServicesAsync(async serviceProvider =>
        {
            var dispatcher = serviceProvider.GetRequiredService<IRetentionRowDispatcher>();
            await dispatcher.FlushAsync();
        });

        tracker.LoadAttempt(noteId.ToString()).Should().Be(2);

        var statuses = await LoadHandlerStatusesAsync(result.SweepId);
        statuses.Should().ContainSingle();
        statuses[0].State.Should().Be(DeadLetteredState);
        statuses[0].Attempt.Should().Be(2);
        statuses[0].CompletedAt.Should().NotBeNull();
        AssertInvalidOperationDiagnostic(statuses[0].LastError);
        // Dead-lettering settles the row, so its captured snapshot is scrubbed too.
        (await CountRemainingCapturedPayloadsAsync(result.SweepId))
            .Should()
            .Be(0);
    }

    [Fact]
    public async Task FlushAsync_Refuses_Snapshot_Payloads_Naming_Types_Outside_The_AllowList()
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
                [$"{CohortOptions.SectionName}:RowHandlerDispatch:BaseBackoff"] = "00:05:00",
                [$"{CohortOptions.SectionName}:RowHandlerDispatch:MaxAttempts"] = "2",
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
                """{"payload":{"$cohortType":"System.Diagnostics.ProcessStartInfo, System.Diagnostics.Process","$cohortValue":{}}}"""
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

    private async Task<IReadOnlyList<HandlerStatusRow>> LoadHandlerStatusesAsync(Guid sweepId)
    {
        await using var connection = new NpgsqlConnection(GetConnectionString());
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
                status."ClaimToken",
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
                    reader.IsDBNull(6) ? null : reader.GetGuid(6),
                    reader.IsDBNull(7) ? null : reader.GetFieldValue<DateTimeOffset>(7),
                    reader.IsDBNull(8) ? null : reader.GetString(8)
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

    private async Task<Guid> ReplaceClaimOwnerAsync(Guid sweepId)
    {
        var claimToken = Guid.NewGuid();
        await using var connection = new NpgsqlConnection(GetConnectionString());
        await connection.OpenAsync();
        await using var command = connection.CreateCommand();
        command.CommandText = """
            UPDATE "sweep_row_handler_status" AS status
            SET "ClaimToken" = @claimToken,
                "ClaimedAt" = @claimedAt,
                "Attempt" = "Attempt" + 1
            FROM "sweep_run_row_detail" AS detail
            WHERE detail."Id" = status."SweepRunRowDetailId"
              AND detail."SweepId" = @sweepId
              AND status."State" = @inFlight
              AND status."Id" = (
                  SELECT candidate."Id"
                  FROM "sweep_row_handler_status" AS candidate
                  INNER JOIN "sweep_run_row_detail" AS candidate_detail
                      ON candidate_detail."Id" = candidate."SweepRunRowDetailId"
                  WHERE candidate_detail."SweepId" = @sweepId
                    AND candidate."State" = @inFlight
                  ORDER BY candidate."Id"
                  LIMIT 1
              )
            """;
        command.Parameters.AddWithValue("claimToken", claimToken);
        command.Parameters.AddWithValue("claimedAt", DateTimeOffset.UtcNow);
        command.Parameters.AddWithValue("sweepId", sweepId);
        command.Parameters.AddWithValue("inFlight", InFlightState);
        (await command.ExecuteNonQueryAsync()).Should().Be(1);
        return claimToken;
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
                            TimeSpan.FromDays(30),
                            Strategy.Purge,
                            AuditRowDetail: AuditRowDetail.PerRow
                        )
                ),
                ["soft-delete"] = new StaticTestRetentionRule(
                    softDeleteRule ?? new RetentionRule(TimeSpan.FromDays(30), Strategy.SoftDelete)
                ),
                ["anonymise"] = new StaticTestRetentionRule(
                    anonymiseRule ?? new RetentionRule(TimeSpan.FromDays(30), Strategy.Anonymise)
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
        Guid? ClaimToken,
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

    private sealed class StaticCategoryRepository(
        IReadOnlyDictionary<string, ITestRetentionRule> resolvers
    ) : ITestRetentionRuleProvider
    {
        private static readonly ITestRetentionRule ExemptFallback =
            new StaticTestRetentionRule(
                new RetentionRule(TimeSpan.FromDays(30), Strategy.Exempt)
            );

        public Task<ITestRetentionRule?> GetAsync(string category, CancellationToken ct)
        {
            return resolvers.TryGetValue(category, out var resolver)
                ? Task.FromResult<ITestRetentionRule?>(resolver)
                : Task.FromResult<ITestRetentionRule?>(ExemptFallback);
        }
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
    private readonly Dictionary<string, DateTimeOffset> lastFailures = new(StringComparer.Ordinal);

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

    public void RecordFailure(string recordId, DateTimeOffset at)
    {
        lock (gate)
        {
            lastFailures[recordId] = at;
        }
    }

    public DateTimeOffset? LoadLastFailure(string recordId)
    {
        lock (gate)
        {
            return lastFailures.TryGetValue(recordId, out var failureAt) ? failureAt : null;
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

file sealed class StatusUpdateBlocker(string connectionString, bool holdHandler = false)
    : IAsyncDisposable
{
    private readonly TaskCompletionSource locked = new(
        TaskCreationOptions.RunContinuationsAsynchronously
    );
    private readonly TaskCompletionSource handlerMayReturn = new(
        TaskCreationOptions.RunContinuationsAsynchronously
    );
    private NpgsqlConnection? connection;
    private NpgsqlTransaction? transaction;

    public int BackendId =>
        connection?.ProcessID
        ?? throw new InvalidOperationException("The status update lock has not been acquired.");

    public Task WaitUntilLockedAsync()
    {
        return locked.Task;
    }

    public async Task AcquireAsync(CancellationToken ct)
    {
        connection = new NpgsqlConnection(connectionString);
        await connection.OpenAsync(ct);
        transaction = await connection.BeginTransactionAsync(ct);

        await using var command = connection.CreateCommand();
        command.Transaction = transaction;
        command.CommandText = """LOCK TABLE "sweep_row_handler_status" IN ACCESS EXCLUSIVE MODE""";
        await command.ExecuteNonQueryAsync(ct);
        locked.TrySetResult();
        if (holdHandler)
        {
            await handlerMayReturn.Task.WaitAsync(ct);
        }
    }

    public void AllowHandlerToReturn()
    {
        handlerMayReturn.TrySetResult();
    }

    public async Task ReleaseAsync()
    {
        if (transaction is not null)
        {
            await transaction.CommitAsync();
            await transaction.DisposeAsync();
            transaction = null;
        }

        if (connection is not null)
        {
            await connection.DisposeAsync();
            connection = null;
        }
    }

    public async ValueTask DisposeAsync()
    {
        await ReleaseAsync();
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

file sealed class ScopedDispatchProbe
{
    public Guid InstanceId { get; } = Guid.NewGuid();
}

[RowHandlerPriority(20)]
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
file sealed class HighPriorityNoteHandler(HandlerExecutionSink sink) : IRetentionHandler<Note>
{
    public Task OnBeforeAsync(Note row, RetentionBeforeContext ctx, CancellationToken ct)
    {
        sink.BeforeCalls.Add("high");
        ctx.Snapshot["priority"] = "high-first";
        return Task.CompletedTask;
    }
}

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

file sealed class ExemptErasureTrackingHandler(HandlerExecutionSink sink)
    : IRetentionHandler<ErasureSubjectRecord>
{
    public Task OnBeforeAsync(
        ErasureSubjectRecord row,
        RetentionBeforeContext ctx,
        CancellationToken ct
    )
    {
        sink.BeforeCalls.Add($"exempt:{row.Body}");
        ctx.Snapshot["body"] = row.Body;
        return Task.CompletedTask;
    }

    public Task OnAfterAsync(RetentionAfterContext<ErasureSubjectRecord> ctx, CancellationToken ct)
    {
        sink.AfterCalls.Add($"after-exempt:{ctx.Snapshot["body"]}:{ctx.Attempt}");
        return Task.CompletedTask;
    }
}

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
        tracker.RecordFailure(ctx.RecordId, DateTimeOffset.UtcNow);
        throw new InvalidOperationException("Simulated permanent after-dispatch failure.");
    }
}

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

file sealed class LocksStatusThenReturnsNoteHandler(
    HandlerExecutionSink sink,
    StatusUpdateBlocker blocker
) : IRetentionHandler<Note>
{
    public Task OnBeforeAsync(Note row, RetentionBeforeContext ctx, CancellationToken ct)
    {
        ctx.Snapshot["body"] = row.Body;
        return Task.CompletedTask;
    }

    public async Task OnAfterAsync(RetentionAfterContext<Note> ctx, CancellationToken ct)
    {
        sink.AfterCalls.Add($"after:{ctx.Snapshot["body"]}:{ctx.Attempt}");
        await blocker.AcquireAsync(ct);
    }
}
