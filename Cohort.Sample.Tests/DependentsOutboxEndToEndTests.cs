using Cohort.Application;
using Cohort.Domain;
using Cohort.Hosting;
using Cohort.Sample.Entities;

using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.DependencyInjection;

using Npgsql;

namespace Cohort.Sample.Tests;

// A host's after-commit work goes through its own outbox: the sample's NoteOutboxStep adds an
// outbox row and calls SaveChangesAsync on the context Cohort hands it, and that row must commit
// with the note it announces and roll back with it. Each test reads the outbox back on a
// connection of its own, after the run, so it sees only what committed.
[Collection("Integration")]
public sealed class DependentsOutboxEndToEndTests(PostgresFixture fixture)
{
    private static readonly DateTimeOffset AsOf = new(2026, 11, 4, 12, 0, 0, TimeSpan.Zero);

    [Fact]
    public async Task The_step_saves_inside_the_runs_transaction()
    {
        await using var database = await MigratedTemporaryDatabaseAsync();
        using var host = OutboxHost(database);
        var tenant = Tenant();
        var noteId = Guid.NewGuid();
        await SeedNotesAsync(host, Note(noteId, tenant.Id, AsOf.AddDays(-45)));

        var result = await host.RunSweepAsync(tenant, AsOf);

        // The step refuses to write without a current transaction, which would skip the row.
        result.EntityFailures.Should().BeEmpty();
        result.Counts.Should().ContainSingle(count => count.EntityType == typeof(Note))
            .Which.Should().BeEquivalentTo(new { Affected = 1L, SkippedCount = 0L });
        (await OutboxPayloadsAsync(host)).Should().Equal(noteId.ToString());
    }

    [Fact]
    public async Task A_purged_notes_outbox_message_commits_with_the_purge()
    {
        await using var database = await MigratedTemporaryDatabaseAsync();
        using var host = OutboxHost(database);
        var tenant = Tenant();
        var purgedId = Guid.NewGuid();
        var youngId = Guid.NewGuid();
        await SeedNotesAsync(
            host,
            Note(purgedId, tenant.Id, AsOf.AddDays(-45)),
            Note(youngId, tenant.Id, AsOf.AddDays(-5))
        );

        await host.RunSweepAsync(tenant, AsOf);

        await using var verify = host.CreateDbContext();
        (await verify.OutboxMessages.Select(message => new { message.TenantId, message.Type, message.Payload }).ToListAsync())
            .Should().Equal(new { TenantId = tenant.Id, Type = "note-retired", Payload = purgedId.ToString() });
        (await RemainingNoteIdsAsync(host)).Should().Equal(youngId);
    }

    [Fact]
    public async Task A_note_the_database_refuses_to_delete_has_its_outbox_message_rolled_back()
    {
        await using var database = await MigratedTemporaryDatabaseAsync();
        // A host table Cohort's model doesn't know, still referencing a note. The pre-filter
        // leaves notes alone because they have a step, so the step runs and the delete fails.
        await ExecuteAsync(
            database,
            """
            CREATE TABLE public.note_references (
                "Id" uuid PRIMARY KEY,
                "NoteId" uuid NOT NULL REFERENCES public.notes ("Id") ON DELETE RESTRICT
            )
            """
        );
        using var host = OutboxHost(database);
        var tenant = Tenant();
        var purgedId = Guid.NewGuid();
        var referencedId = Guid.NewGuid();
        await SeedNotesAsync(
            host,
            Note(purgedId, tenant.Id, AsOf.AddDays(-45)),
            Note(referencedId, tenant.Id, AsOf.AddDays(-45))
        );
        await ExecuteAsync(
            database,
            $"""INSERT INTO public.note_references ("Id", "NoteId") VALUES ('{Guid.NewGuid()}', '{referencedId}')"""
        );

        var result = await host.RunSweepAsync(tenant, AsOf);

        result.Counts.Should().ContainSingle(count => count.EntityType == typeof(Note))
            .Which.Should().BeEquivalentTo(new { Affected = 1L, BlockedCount = 1L, SkippedCount = 0L });
        (await RemainingNoteIdsAsync(host)).Should().Equal(referencedId);
        (await OutboxPayloadsAsync(host)).Should().Equal(purgedId.ToString());
    }

    [Fact]
    public async Task A_held_note_writes_no_outbox_message()
    {
        await using var database = await MigratedTemporaryDatabaseAsync();
        using var host = OutboxHost(database);
        var tenant = Tenant();
        var purgedId = Guid.NewGuid();
        var heldId = Guid.NewGuid();
        await SeedNotesAsync(
            host,
            Note(purgedId, tenant.Id, AsOf.AddDays(-45)),
            Note(heldId, tenant.Id, AsOf.AddDays(-45))
        );
        await host.RunWithServicesAsync(services =>
            services.GetRequiredService<IRetentionHoldsRepository>().CreateAsync(
                new RetentionHoldRequest(
                    Guid.NewGuid(),
                    RetentionEntityIdentity.For<Note>(),
                    heldId.ToString(),
                    tenant.Id,
                    "litigation",
                    AsOf.AddDays(-1)
                ),
                CancellationToken.None
            )
        );

        var result = await host.RunSweepAsync(tenant, AsOf);

        result.Counts.Should().ContainSingle(count => count.EntityType == typeof(Note))
            .Which.Should().BeEquivalentTo(new { Affected = 1L, HeldCount = 1L, SkippedCount = 0L });
        (await RemainingNoteIdsAsync(host)).Should().Equal(heldId);
        (await OutboxPayloadsAsync(host)).Should().Equal(purgedId.ToString());
    }

    [Fact]
    public async Task A_caller_transaction_erasure_leaves_an_outbox_message_only_when_the_caller_commits()
    {
        await using var database = await MigratedTemporaryDatabaseAsync();
        using var host = OutboxHost(database);
        var tenant = Tenant();
        var subjectId = Guid.NewGuid();
        var noteId = Guid.NewGuid();
        var note = Note(noteId, tenant.Id, AsOf.AddDays(-1));
        note.SubjectId = subjectId;
        await SeedNotesAsync(host, note);

        foreach (var commit in new[] { false, true })
        {
            await host.RunWithServicesAsync(async services =>
            {
                var db = services.GetRequiredService<SampleDbContext>();
                await using var transaction = await db.Database.BeginTransactionAsync();

                var result = await services.GetRequiredService<IRetentionErasureService>().ExecuteAsync(
                    RetentionErasureRequest.Tenanted(
                        tenant,
                        // The sample's soft-delete entities need the opt-in; none has a row here.
                        new ErasureScope("user", subjectId, allowSoftDeleteAsErasure: true),
                        AsOf,
                        ErasureTransaction.Caller
                    )
                );

                result.EntityFailures.Should().BeEmpty();
                result.Counts.Should().ContainSingle(count => count.EntityType == typeof(Note))
                    .Which.Affected.Should().Be(1);
                (await db.OutboxMessages.Select(message => message.Payload).ToListAsync())
                    .Should().Equal(noteId.ToString());
                if (commit)
                {
                    await transaction.CommitAsync();
                }
                else
                {
                    await transaction.RollbackAsync();
                }
            });

            if (commit)
            {
                (await RemainingNoteIdsAsync(host)).Should().BeEmpty();
                (await OutboxPayloadsAsync(host)).Should().Equal(noteId.ToString());
            }
            else
            {
                (await RemainingNoteIdsAsync(host)).Should().Equal(noteId);
                (await OutboxPayloadsAsync(host)).Should().BeEmpty();
            }
        }
    }

    private static CohortTestHost OutboxHost(TemporaryDatabase database) =>
        new(database.ConnectionString, configureServices: services => services.AddRetentionDependents<Note, NoteOutboxStep>());

    private static TenantContext Tenant() => new(Guid.NewGuid(), "uk", new Dictionary<string, string>());

    private static Note Note(Guid id, Guid tenantId, DateTimeOffset createdAt) =>
        new() { Id = id, TenantId = tenantId, CreatedAt = createdAt, Body = "outbox" };

    private static async Task SeedNotesAsync(CohortTestHost host, params Note[] notes)
    {
        await using var db = host.CreateDbContext();
        db.Notes.AddRange(notes);
        await db.SaveChangesAsync();
    }

    private static async Task<List<Guid>> RemainingNoteIdsAsync(CohortTestHost host)
    {
        await using var db = host.CreateDbContext();
        return await db.Notes.Select(note => note.Id).ToListAsync();
    }

    private static async Task<List<string>> OutboxPayloadsAsync(CohortTestHost host)
    {
        await using var db = host.CreateDbContext();
        return await db.OutboxMessages.Select(message => message.Payload).ToListAsync();
    }

    private static async Task ExecuteAsync(TemporaryDatabase database, string sql)
    {
        await using var connection = new NpgsqlConnection(database.ConnectionString);
        await connection.OpenAsync();
        await using var command = new NpgsqlCommand(sql, connection);
        await command.ExecuteNonQueryAsync();
    }

    private async Task<TemporaryDatabase> MigratedTemporaryDatabaseAsync()
    {
        var database = await TemporaryDatabase.CreateAsync(fixture.ConnectionString);
        await using var db = new SampleDbContext(
            new DbContextOptionsBuilder<SampleDbContext>().UseNpgsql(database.ConnectionString).Options
        );
        await db.Database.MigrateAsync();
        return database;
    }
}
