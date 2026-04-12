using Cohort.Domain;
using Cohort.Infrastructure.Sweep;

using System.Collections;
using System.Data;
using System.Data.Common;
using System.Diagnostics.CodeAnalysis;

using Npgsql;

namespace Cohort.Sample.Tests;

[Collection("Integration")]
public sealed class PurgeSweepStrategyEndToEndTests(PostgresFixture fixture)
{
    [Fact]
    public async Task SweepAsync_Deletes_Only_Rows_Past_Cutoff_For_The_Target_Tenant()
    {
        const string tableName = "purge_candidate_records";
        var tenantA = Guid.NewGuid();
        var tenantB = Guid.NewGuid();
        var now = new DateTimeOffset(2026, 4, 11, 12, 0, 0, TimeSpan.Zero);

        await using var connection = new NpgsqlConnection(fixture.ConnectionString);
        await connection.OpenAsync();

        await using (var setup = connection.CreateCommand())
        {
            setup.CommandText =
                """
                DROP TABLE IF EXISTS "purge_candidate_records";
                CREATE TABLE "purge_candidate_records" (
                    "Id" uuid PRIMARY KEY,
                    "TenantId" uuid NOT NULL,
                    "CreatedAt" timestamp with time zone NOT NULL,
                    "Body" text NOT NULL
                );
                """;
            await setup.ExecuteNonQueryAsync();
        }

        await InsertRecordAsync(connection, Guid.NewGuid(), tenantA, now.AddDays(-45), "delete-me");
        await InsertRecordAsync(connection, Guid.NewGuid(), tenantA, now.AddDays(-5), "keep-newer");
        await InsertRecordAsync(connection, Guid.NewGuid(), tenantB, now.AddDays(-45), "keep-other-tenant");

        var strategy = new PurgeSweepStrategy();
        var entry = new RetentionEntry(
            typeof(PurgeCandidateRecord),
            tableName,
            "short-lived",
            nameof(PurgeCandidateRecord.CreatedAt),
            "CreatedAt",
            [],
            new TenantConvention(nameof(PurgeCandidateRecord.TenantId), "TenantId"),
            null
        );
        var rule = new RetentionRule(TimeSpan.FromDays(30), Strategy.Purge);
        var context = new RetentionResolutionContext(
            "short-lived",
            new TenantContext(tenantA, "uk", new Dictionary<string, string>()),
            now,
            []
        );
        await using var transaction = await connection.BeginTransactionAsync();

        var affected = await strategy.SweepAsync(
            entry,
            rule,
            context,
            connection,
            transaction,
            CancellationToken.None
        );
        await transaction.CommitAsync();

        affected.Should().Be(1);
        var remainingRows = await GetRemainingRowsAsync(connection);
        remainingRows.Should().BeEquivalentTo(
            [
                new RemainingRow(tenantA, "keep-newer"),
                new RemainingRow(tenantB, "keep-other-tenant"),
            ]
        );
    }

    [Fact]
    public async Task SweepAsync_Assigns_The_Provided_Transaction_To_The_Raw_Command()
    {
        var strategy = new PurgeSweepStrategy();
        var connection = new RecordingDbConnection();
        var transaction = connection.BeginTransaction();
        var tenantId = Guid.NewGuid();
        var now = new DateTimeOffset(2026, 4, 11, 12, 0, 0, TimeSpan.Zero);
        var entry = new RetentionEntry(
            typeof(PurgeCandidateRecord),
            "purge_candidate_records",
            "short-lived",
            nameof(PurgeCandidateRecord.CreatedAt),
            "CreatedAt",
            [],
            new TenantConvention(nameof(PurgeCandidateRecord.TenantId), "TenantId"),
            null
        );
        var rule = new RetentionRule(TimeSpan.FromDays(30), Strategy.Purge);
        var context = new RetentionResolutionContext(
            "short-lived",
            new TenantContext(tenantId, "uk", new Dictionary<string, string>()),
            now,
            []
        );

        var affected = await strategy.SweepAsync(
            entry,
            rule,
            context,
            connection,
            transaction,
            CancellationToken.None
        );

        affected.Should().Be(1);
        connection.LastCommand.Should().NotBeNull();
        connection.LastCommand!.AssignedTransaction.Should().BeSameAs(transaction);
        connection.LastCommand.CommandText.Should().Contain("@cutoff");
        connection.LastCommand.CommandText.Should().Contain("@tenantId");
        connection.LastCommand.CommandText.Should().Contain("@holdTableName");
        connection.LastCommand.CommandText.Should().Contain("@holdAsOf");
        connection.LastCommand.CommandText.Should().Contain("NOT EXISTS");
        connection.LastCommand.Parameters.Count.Should().Be(4);
        connection.LastCommand.Parameters.Contains("cutoff").Should().BeTrue();
        connection.LastCommand.Parameters.Contains("tenantId").Should().BeTrue();
        connection.LastCommand.Parameters.Contains("holdTableName").Should().BeTrue();
        connection.LastCommand.Parameters.Contains("holdAsOf").Should().BeTrue();
        connection.LastCommand.Parameters["cutoff"].Value.Should().Be(now.AddDays(-30));
        connection.LastCommand.Parameters["tenantId"].Value.Should().Be(tenantId);
        connection.LastCommand.Parameters["holdTableName"].Value.Should().Be("purge_candidate_records");
        connection.LastCommand.Parameters["holdAsOf"].Value.Should().Be(now);
    }

    private static async Task InsertRecordAsync(
        NpgsqlConnection connection,
        Guid id,
        Guid tenantId,
        DateTimeOffset createdAt,
        string body
    )
    {
        await using var command = connection.CreateCommand();
        command.CommandText =
            """
            INSERT INTO "purge_candidate_records" ("Id", "TenantId", "CreatedAt", "Body")
            VALUES (@id, @tenantId, @createdAt, @body)
            """;
        command.Parameters.Add(CreateParameter(command, "id", id));
        command.Parameters.Add(CreateParameter(command, "tenantId", tenantId));
        command.Parameters.Add(CreateParameter(command, "createdAt", createdAt));
        command.Parameters.Add(CreateParameter(command, "body", body));
        await command.ExecuteNonQueryAsync();
    }

    private static async Task<IReadOnlyList<RemainingRow>> GetRemainingRowsAsync(NpgsqlConnection connection)
    {
        await using var command = connection.CreateCommand();
        command.CommandText =
            """
            SELECT "TenantId", "Body"
            FROM "purge_candidate_records"
            ORDER BY "Body"
            """;

        var rows = new List<RemainingRow>();
        await using var reader = await command.ExecuteReaderAsync();
        while (await reader.ReadAsync())
        {
            rows.Add(new RemainingRow(reader.GetGuid(0), reader.GetString(1)));
        }

        return rows;
    }

    private static NpgsqlParameter CreateParameter(
        NpgsqlCommand command,
        string name,
        object value
    )
    {
        var parameter = command.CreateParameter();
        parameter.ParameterName = name;
        parameter.Value = value;
        return parameter;
    }

    private sealed class PurgeCandidateRecord
    {
        public Guid Id { get; init; }
        public Guid TenantId { get; init; }
        public DateTimeOffset CreatedAt { get; init; }
    }

    private sealed record RemainingRow(Guid TenantId, string Body);

    private sealed class RecordingDbConnection : DbConnection
    {
        private ConnectionState state = ConnectionState.Closed;
        private string connectionString = "Host=recording";

        public RecordingDbCommand? LastCommand { get; private set; }

        [AllowNull]
        public override string ConnectionString
        {
            get => connectionString;
            set => connectionString = value ?? "";
        }

        public override string Database => "recording";

        public override string DataSource => "recording";

        public override string ServerVersion => "1.0";

        public override ConnectionState State => state;

        public override void ChangeDatabase(string databaseName) { }

        public override void Close()
        {
            state = ConnectionState.Closed;
        }

        public override void Open()
        {
            state = ConnectionState.Open;
        }

        protected override DbTransaction BeginDbTransaction(IsolationLevel isolationLevel)
        {
            state = ConnectionState.Open;
            return new RecordingDbTransaction(this);
        }

        protected override DbCommand CreateDbCommand()
        {
            LastCommand = new RecordingDbCommand(this);
            return LastCommand;
        }
    }

    private sealed class RecordingDbTransaction(RecordingDbConnection connection) : DbTransaction
    {
        public override IsolationLevel IsolationLevel => IsolationLevel.ReadCommitted;

        protected override DbConnection? DbConnection => connection;

        public override void Commit() { }

        public override void Rollback() { }
    }

    private sealed class RecordingDbCommand(RecordingDbConnection connection) : DbCommand
    {
        private readonly RecordingDbParameterCollection parameters = new();
        private string commandText = "";

        public DbTransaction? AssignedTransaction { get; private set; }

        [AllowNull]
        public override string CommandText
        {
            get => commandText;
            set => commandText = value ?? "";
        }

        public override int CommandTimeout { get; set; }

        public override CommandType CommandType { get; set; } = CommandType.Text;

        protected override DbConnection? DbConnection { get; set; } = connection;

        protected override DbParameterCollection DbParameterCollection => parameters;

        protected override DbTransaction? DbTransaction
        {
            get => AssignedTransaction;
            set => AssignedTransaction = value;
        }

        public override bool DesignTimeVisible { get; set; }

        public override UpdateRowSource UpdatedRowSource { get; set; }

        public override void Cancel() { }

        public override int ExecuteNonQuery()
        {
            return 1;
        }

        public override object? ExecuteScalar()
        {
            return null;
        }

        public override void Prepare() { }

        protected override DbParameter CreateDbParameter()
        {
            return new RecordingDbParameter();
        }

        protected override DbDataReader ExecuteDbDataReader(CommandBehavior behavior)
        {
            throw new NotSupportedException();
        }

        public override Task<int> ExecuteNonQueryAsync(CancellationToken cancellationToken)
        {
            return Task.FromResult(1);
        }
    }

    private sealed class RecordingDbParameter : DbParameter
    {
        private string parameterName = "";
        private string sourceColumn = "";

        public override DbType DbType { get; set; }

        public override ParameterDirection Direction { get; set; } = ParameterDirection.Input;

        public override bool IsNullable { get; set; }

        [AllowNull]
        public override string ParameterName
        {
            get => parameterName;
            set => parameterName = value ?? "";
        }

        [AllowNull]
        public override string SourceColumn
        {
            get => sourceColumn;
            set => sourceColumn = value ?? "";
        }

        public override object? Value { get; set; }

        public override bool SourceColumnNullMapping { get; set; }

        public override int Size { get; set; }

        public override void ResetDbType() { }
    }

    private sealed class RecordingDbParameterCollection : DbParameterCollection
    {
        private readonly List<DbParameter> items = [];

        public override int Count => items.Count;

        public override object SyncRoot => this;

        public override int Add(object value)
        {
            items.Add((DbParameter)value);
            return items.Count - 1;
        }

        public override void AddRange(Array values)
        {
            foreach (var value in values)
            {
                Add(value!);
            }
        }

        public override void Clear()
        {
            items.Clear();
        }

        public override bool Contains(object value)
        {
            return items.Contains((DbParameter)value);
        }

        public override bool Contains(string value)
        {
            return items.Any(parameter => parameter.ParameterName == value);
        }

        public override void CopyTo(Array array, int index)
        {
            items.ToArray().CopyTo(array, index);
        }

        public override IEnumerator GetEnumerator()
        {
            return items.GetEnumerator();
        }

        public override int IndexOf(object value)
        {
            return items.IndexOf((DbParameter)value);
        }

        public override int IndexOf(string parameterName)
        {
            return items.FindIndex(parameter => parameter.ParameterName == parameterName);
        }

        public override void Insert(int index, object value)
        {
            items.Insert(index, (DbParameter)value);
        }

        public override void Remove(object value)
        {
            items.Remove((DbParameter)value);
        }

        public override void RemoveAt(int index)
        {
            items.RemoveAt(index);
        }

        public override void RemoveAt(string parameterName)
        {
            var index = IndexOf(parameterName);
            if (index >= 0)
            {
                RemoveAt(index);
            }
        }

        protected override DbParameter GetParameter(int index)
        {
            return items[index];
        }

        protected override DbParameter GetParameter(string parameterName)
        {
            return items[IndexOf(parameterName)];
        }

        protected override void SetParameter(int index, DbParameter value)
        {
            items[index] = value;
        }

        protected override void SetParameter(string parameterName, DbParameter value)
        {
            var index = IndexOf(parameterName);
            if (index >= 0)
            {
                items[index] = value;
                return;
            }

            items.Add(value);
        }
    }
}
