using System.Text.Json;

using Cohort.Application;
using Cohort.Domain;
using Cohort.Hosting;
using Cohort.Sample.Entities;

using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.DependencyInjection;

using Npgsql;

namespace Cohort.Sample.Tests;

public sealed class RetentionHandlerEndToEndTests(PostgresFixture fixture)
    : IntegrationTestBase(fixture)
{
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

        result.Counts.Should().Contain(
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
        rowDetails[0].EntityId.Should().Be(noteId.ToString());

        using (var payload = JsonDocument.Parse(rowDetails[0].CapturedPayload))
        {
            payload.RootElement.GetProperty("body").GetString().Should().Be("handler-target");
            payload.RootElement.GetProperty("priority").GetString().Should().Be("high-first");
        }

        var statuses = await LoadHandlerStatusesAsync(result.SweepId);
        statuses.Should().HaveCount(2);
        statuses.Select(status => status.HandlerType).Should().Contain(type => type.Contains(nameof(HighPriorityNoteHandler), StringComparison.Ordinal));
        statuses.Select(status => status.HandlerType).Should().Contain(type => type.Contains(nameof(LowPriorityNoteHandler), StringComparison.Ordinal));
        statuses.All(status => status.State == 0 && status.Attempt == 0).Should().BeTrue();
    }

    [Fact]
    public async Task Scheduled_Sweep_Does_Not_Duplicate_Row_Detail_Audit_For_HandlerManaged_PerRow_Entities()
    {
        var tenantId = Guid.NewGuid();
        var asOf = new DateTimeOffset(2026, 4, 13, 12, 0, 0, TimeSpan.Zero);
        var logId = Guid.NewGuid();

        await using (var db = Host.CreateDbContext())
        {
            db.PerRowAuditedLogs.Add(
                new PerRowAuditedLog
                {
                    Id = logId,
                    TenantId = tenantId,
                    CreatedAt = asOf.AddDays(-120),
                    Payload = "per-row-handler",
                }
            );
            await db.SaveChangesAsync();
        }

        using var handlerHost = new CohortTestHost(
            GetConnectionString(),
            configureServices: services => services.AddRowHandler<PerRowAuditedLog, PerRowAuditHandler>()
        );

        var result = await handlerHost.RunSweepAsync(
            new TenantContext(tenantId, "uk", new Dictionary<string, string>()),
            asOf
        );

        result.Counts.Should().Contain(
            new EntitySweepCount(
                typeof(PerRowAuditedLog),
                "per-row-audit-override",
                tenantId,
                Strategy.Purge,
                1
            )
        );

        var rowDetails = await LoadCapturedRowsAsync(result.SweepId);
        rowDetails
            .Where(row => row.EntityType == typeof(PerRowAuditedLog).FullName)
            .Should()
            .ContainSingle(row => row.EntityId == logId.ToString());

        var statuses = await LoadHandlerStatusesAsync(result.SweepId);
        statuses.Should().ContainSingle(status => status.HandlerType.Contains(nameof(PerRowAuditHandler), StringComparison.Ordinal));
    }

    [Fact]
    public async Task Scheduled_Sweep_With_Handlers_Still_Executes_SoftDelete_And_Anonymise_Strategies()
    {
        var tenantId = Guid.NewGuid();
        var asOf = new DateTimeOffset(2026, 4, 13, 12, 0, 0, TimeSpan.Zero);
        var softDeleteId = Guid.NewGuid();
        var contactId = Guid.NewGuid();

        await using (var db = Host.CreateDbContext())
        {
            db.SoftDeleteRecords.Add(
                new SoftDeleteRecord
                {
                    Id = softDeleteId,
                    TenantId = tenantId,
                    CreatedAt = asOf.AddDays(-120),
                    Body = "soft-delete-handler",
                    IsDeleted = false,
                }
            );
            db.AnonymisedContacts.Add(
                new AnonymisedContact
                {
                    Id = contactId,
                    TenantId = tenantId,
                    CreatedAt = asOf.AddDays(-120),
                    EmailAddress = "handler@example.com",
                    GivenName = "Handler",
                    Surname = "Contact",
                    Notes = "keep-me",
                }
            );
            await db.SaveChangesAsync();
        }

        using var handlerHost = new CohortTestHost(
            GetConnectionString(),
            configureServices: services =>
            {
                services.AddRowHandler<SoftDeleteRecord, SoftDeleteRecordHandler>();
                services.AddRowHandler<AnonymisedContact, AnonymisedContactHandler>();
            }
        );

        var result = await handlerHost.RunSweepAsync(
            new TenantContext(tenantId, "uk", new Dictionary<string, string>()),
            asOf
        );

        result.Counts.Should().Contain(
            new EntitySweepCount(
                typeof(SoftDeleteRecord),
                "soft-delete",
                tenantId,
                Strategy.SoftDelete,
                1
            )
        );
        result.Counts.Should().Contain(
            new EntitySweepCount(
                typeof(AnonymisedContact),
                "anonymise",
                tenantId,
                Strategy.Anonymise,
                1
            )
        );

        await using (var verify = Host.CreateDbContext())
        {
            var softDeleted = await verify.SoftDeleteRecords.SingleAsync(record => record.Id == softDeleteId);
            softDeleted.IsDeleted.Should().BeTrue();
            softDeleted.DeletedAt.Should().Be(asOf);

            var anonymised = await verify.AnonymisedContacts.SingleAsync(contact => contact.Id == contactId);
            anonymised.EmailAddress.Should().BeNull();
            anonymised.GivenName.Should().BeEmpty();
            anonymised.Surname.Should().Be("[redacted]");
            anonymised.Notes.Should().Be("keep-me");
        }

        var rowDetails = await LoadCapturedRowsAsync(result.SweepId);
        rowDetails.Should().Contain(row => row.EntityType == typeof(SoftDeleteRecord).FullName && row.EntityId == softDeleteId.ToString());
        rowDetails.Should().Contain(row => row.EntityType == typeof(AnonymisedContact).FullName && row.EntityId == contactId.ToString());

        var statuses = await LoadHandlerStatusesAsync(result.SweepId);
        statuses.Should().Contain(status => status.HandlerType.Contains(nameof(SoftDeleteRecordHandler), StringComparison.Ordinal));
        statuses.Should().Contain(status => status.HandlerType.Contains(nameof(AnonymisedContactHandler), StringComparison.Ordinal));
    }

    [Fact]
    public async Task Scheduled_Sweep_Without_Handlers_Remains_On_The_Bulk_Path()
    {
        var tenantId = Guid.NewGuid();
        var asOf = new DateTimeOffset(2026, 4, 13, 12, 0, 0, TimeSpan.Zero);
        var noteId = Guid.NewGuid();

        await using (var db = Host.CreateDbContext())
        {
            db.Notes.Add(
                new Note
                {
                    Id = noteId,
                    TenantId = tenantId,
                    CreatedAt = asOf.AddDays(-120),
                    Body = "bulk-delete-target",
                }
            );
            await db.SaveChangesAsync();
        }

        var result = await Host.RunSweepAsync(
            new TenantContext(tenantId, "uk", new Dictionary<string, string>()),
            asOf
        );

        result.Counts.Should().Contain(
            new EntitySweepCount(typeof(Note), "short-lived", tenantId, Strategy.Purge, 1)
        );

        await using (var verify = Host.CreateDbContext())
        {
            (await verify.Notes.AnyAsync(note => note.Id == noteId)).Should().BeFalse();
        }

        (await LoadCapturedRowsAsync(result.SweepId)).Should().BeEmpty();
        (await LoadHandlerStatusesAsync(result.SweepId)).Should().BeEmpty();
    }

    [Fact]
    public async Task Scheduled_Sweep_When_OnBefore_Fails_Skips_Only_That_Row_And_Continues_With_Siblings()
    {
        var tenantId = Guid.NewGuid();
        var asOf = new DateTimeOffset(2026, 4, 13, 12, 0, 0, TimeSpan.Zero);
        var successfulNoteId = Guid.NewGuid();
        var failingNoteId = Guid.NewGuid();

        await using (var db = Host.CreateDbContext())
        {
            db.Notes.AddRange(
                new Note
                {
                    Id = successfulNoteId,
                    TenantId = tenantId,
                    CreatedAt = asOf.AddDays(-120),
                    Body = "surviving-sibling",
                },
                new Note
                {
                    Id = failingNoteId,
                    TenantId = tenantId,
                    CreatedAt = asOf.AddDays(-120),
                    Body = SelectivelyFailingNoteHandler.FailingBody,
                }
            );
            await db.SaveChangesAsync();
        }

        using var handlerHost = new CohortTestHost(
            GetConnectionString(),
            configureServices: services => services.AddRowHandler<Note, SelectivelyFailingNoteHandler>()
        );

        var result = await handlerHost.RunSweepAsync(
            new TenantContext(tenantId, "uk", new Dictionary<string, string>()),
            asOf
        );

        result.Counts.Should().Contain(
            new EntitySweepCount(typeof(Note), "short-lived", tenantId, Strategy.Purge, 1)
        );

        await using (var verify = Host.CreateDbContext())
        {
            (await verify.Notes.AnyAsync(note => note.Id == successfulNoteId)).Should().BeFalse();
            (await verify.Notes.AnyAsync(note => note.Id == failingNoteId)).Should().BeTrue();
        }

        var rowDetails = await LoadCapturedRowsAsync(result.SweepId);
        rowDetails.Should().ContainSingle(row => row.EntityId == successfulNoteId.ToString());
        rowDetails.Should().NotContain(row => row.EntityId == failingNoteId.ToString());

        using (var payload = JsonDocument.Parse(rowDetails[0].CapturedPayload))
        {
            payload.RootElement.GetProperty("body").GetString().Should().Be("surviving-sibling");
        }

        var statuses = await LoadHandlerStatusesAsync(result.SweepId);
        statuses.Should().ContainSingle(
            status => status.HandlerType.Contains(nameof(SelectivelyFailingNoteHandler), StringComparison.Ordinal)
        );
    }

    private async Task<IReadOnlyList<CapturedRow>> LoadCapturedRowsAsync(Guid sweepId)
    {
        await using var connection = new NpgsqlConnection(GetConnectionString());
        await connection.OpenAsync();
        await using var command = connection.CreateCommand();
        command.CommandText =
            """
            SELECT "EntityType", "EntityId", COALESCE("CapturedPayload", '')
            FROM "sweep_run_row_detail"
            WHERE "SweepId" = @sweepId
            ORDER BY "Id"
            """;
        command.Parameters.AddWithValue("sweepId", sweepId);

        var rows = new List<CapturedRow>();
        await using var reader = await command.ExecuteReaderAsync();
        while (await reader.ReadAsync())
        {
            rows.Add(new CapturedRow(reader.GetString(0), reader.GetString(1), reader.GetString(2)));
        }

        return rows;
    }

    private async Task<IReadOnlyList<HandlerStatusRow>> LoadHandlerStatusesAsync(Guid sweepId)
    {
        await using var connection = new NpgsqlConnection(GetConnectionString());
        await connection.OpenAsync();
        await using var command = connection.CreateCommand();
        command.CommandText =
            """
            SELECT status."HandlerType", status."State", status."Attempt"
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
            rows.Add(new HandlerStatusRow(reader.GetString(0), reader.GetInt32(1), reader.GetInt32(2)));
        }

        return rows;
    }

    private string GetConnectionString()
    {
        using var db = Host.CreateDbContext();
        return db.Database.GetConnectionString()!;
    }

    private sealed record CapturedRow(string EntityType, string EntityId, string CapturedPayload);

    private sealed record HandlerStatusRow(string HandlerType, int State, int Attempt);
}

file sealed class HandlerExecutionSink
{
    public List<string> BeforeCalls { get; } = [];
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

file sealed class PerRowAuditHandler : IRetentionHandler<PerRowAuditedLog>
{
    public Task OnBeforeAsync(
        PerRowAuditedLog row,
        RetentionBeforeContext ctx,
        CancellationToken ct
    )
    {
        ctx.Snapshot["payload"] = row.Payload;
        return Task.CompletedTask;
    }
}

file sealed class SoftDeleteRecordHandler : IRetentionHandler<SoftDeleteRecord>
{
    public Task OnBeforeAsync(
        SoftDeleteRecord row,
        RetentionBeforeContext ctx,
        CancellationToken ct
    )
    {
        ctx.Snapshot["body"] = row.Body;
        return Task.CompletedTask;
    }
}

file sealed class AnonymisedContactHandler : IRetentionHandler<AnonymisedContact>
{
    public Task OnBeforeAsync(
        AnonymisedContact row,
        RetentionBeforeContext ctx,
        CancellationToken ct
    )
    {
        ctx.Snapshot["email"] = row.EmailAddress;
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
