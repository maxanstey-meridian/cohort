using Cohort.Application;
using Cohort.Domain;
using Cohort.Infrastructure.Sweep;
using Cohort.Sample.Entities;

using System.Collections;
using System.Data;
using System.Data.Common;
using System.Diagnostics.CodeAnalysis;

using Microsoft.EntityFrameworkCore;

namespace Cohort.Sample.Tests;

public sealed class SoftDeleteSweepEndToEndTests(PostgresFixture fixture)
    : IntegrationTestBase(fixture)
{
    [Fact]
    public async Task Sweep_Path_Soft_Deletes_Only_Expired_Rows_For_The_Target_Tenant_And_Stamps_DeletedAt()
    {
        var tenantA = Guid.NewGuid();
        var tenantB = Guid.NewGuid();
        var asOf = new DateTimeOffset(2026, 4, 12, 12, 0, 0, TimeSpan.Zero);
        var originalDeletedAt = asOf.AddDays(-10);

        await using (var db = Host.CreateDbContext())
        {
            db.SoftDeleteRecords.AddRange(
                new SoftDeleteRecord
                {
                    Id = Guid.NewGuid(),
                    TenantId = tenantA,
                    CreatedAt = asOf.AddDays(-120),
                    Body = "soft-delete-expired-target",
                    IsDeleted = false,
                },
                new SoftDeleteRecord
                {
                    Id = Guid.NewGuid(),
                    TenantId = tenantA,
                    CreatedAt = asOf.AddDays(-5),
                    Body = "soft-delete-keep-newer",
                    IsDeleted = false,
                },
                new SoftDeleteRecord
                {
                    Id = Guid.NewGuid(),
                    TenantId = tenantB,
                    CreatedAt = asOf.AddDays(-120),
                    Body = "soft-delete-keep-other-tenant",
                    IsDeleted = false,
                },
                new SoftDeleteRecord
                {
                    Id = Guid.NewGuid(),
                    TenantId = tenantA,
                    CreatedAt = asOf.AddDays(-120),
                    Body = "soft-delete-keep-existing-deleted",
                    IsDeleted = true,
                    DeletedAt = originalDeletedAt,
                }
            );
            await db.SaveChangesAsync();
        }

        var result = await Host.RunSweepAsync(
            new TenantContext(tenantA, "uk", new Dictionary<string, string>()),
            asOf
        );

        result.Counts.Should().Contain(
            new EntitySweepCount(
                typeof(SoftDeleteRecord),
                "soft-delete",
                tenantA,
                Strategy.SoftDelete,
                1
            )
        );

        await using var verify = Host.CreateDbContext();
        var records = await verify
            .SoftDeleteRecords.OrderBy(record => record.Body)
            .Select(record => new
            {
                record.Body,
                record.IsDeleted,
                record.DeletedAt,
            })
            .ToListAsync();

        records.Should().Equal(
            new
            {
                Body = "soft-delete-expired-target",
                IsDeleted = true,
                DeletedAt = (DateTimeOffset?)asOf,
            },
            new
            {
                Body = "soft-delete-keep-existing-deleted",
                IsDeleted = true,
                DeletedAt = (DateTimeOffset?)originalDeletedAt,
            },
            new
            {
                Body = "soft-delete-keep-newer",
                IsDeleted = false,
                DeletedAt = (DateTimeOffset?)null,
            },
            new
            {
                Body = "soft-delete-keep-other-tenant",
                IsDeleted = false,
                DeletedAt = (DateTimeOffset?)null,
            }
        );
    }

    [Fact]
    public async Task Startup_Path_Fails_When_A_SoftDelete_Category_Lacks_The_Required_Convention()
    {
        await using var db = Host.CreateDbContext();
        var connectionString = db.Database.GetConnectionString()!;

        using var host = new CohortTestHost(
            connectionString,
            new StaticCategoryRepository(
                new Dictionary<string, IRetentionRuleResolver>
                {
                    ["short-lived"] = new StaticRetentionRuleResolver(
                        new RetentionRule(TimeSpan.FromDays(30), Strategy.SoftDelete)
                    ),
                    ["soft-delete"] = new StaticRetentionRuleResolver(
                        new RetentionRule(TimeSpan.FromDays(30), Strategy.SoftDelete)
                    ),
                    ["anonymise"] = new StaticRetentionRuleResolver(
                        new RetentionRule(TimeSpan.FromDays(30), Strategy.Anonymise)
                    ),
                }
            )
        );

        var act = () => host.RunStartupAsync();

        var exception = await act.Should().ThrowAsync<RetentionConfigurationException>();
        exception.Which.Errors.Should().ContainSingle();
        exception
            .Which.Errors[0]
            .Should()
            .Be(
                $"Soft-delete convention on {typeof(Note).FullName}: retained SoftDelete categories require a public bool soft-delete flag property (named IsDeleted by convention, or marked with [RetentionSoftDelete]) mapped by EF."
            );
    }

    [Fact]
    public async Task Sweep_Path_Does_Not_Modify_Rows_That_Were_Already_Soft_Deleted()
    {
        var tenantId = Guid.NewGuid();
        var asOf = new DateTimeOffset(2026, 4, 12, 12, 0, 0, TimeSpan.Zero);
        var originalDeletedAt = asOf.AddDays(-20);

        await using (var db = Host.CreateDbContext())
        {
            db.SoftDeleteRecords.AddRange(
                new SoftDeleteRecord
                {
                    Id = Guid.NewGuid(),
                    TenantId = tenantId,
                    CreatedAt = asOf.AddDays(-120),
                    Body = "soft-delete-once",
                    IsDeleted = false,
                },
                new SoftDeleteRecord
                {
                    Id = Guid.NewGuid(),
                    TenantId = tenantId,
                    CreatedAt = asOf.AddDays(-150),
                    Body = "soft-delete-already-done",
                    IsDeleted = true,
                    DeletedAt = originalDeletedAt,
                }
            );
            await db.SaveChangesAsync();
        }

        var tenant = new TenantContext(tenantId, "uk", new Dictionary<string, string>());

        var first = await Host.RunSweepAsync(tenant, asOf);
        var second = await Host.RunSweepAsync(tenant, asOf);

        first.Counts.Should().Contain(
            new EntitySweepCount(
                typeof(SoftDeleteRecord),
                "soft-delete",
                tenantId,
                Strategy.SoftDelete,
                1
            )
        );
        second.Counts.Should().Contain(
            new EntitySweepCount(
                typeof(SoftDeleteRecord),
                "soft-delete",
                tenantId,
                Strategy.SoftDelete,
                0
            )
        );

        await using var verify = Host.CreateDbContext();
        var records = await verify
            .SoftDeleteRecords.OrderBy(record => record.Body)
            .Select(record => new
            {
                record.Body,
                record.IsDeleted,
                record.DeletedAt,
            })
            .ToListAsync();

        records.Should().Equal(
            new
            {
                Body = "soft-delete-already-done",
                IsDeleted = true,
                DeletedAt = (DateTimeOffset?)originalDeletedAt,
            },
            new
            {
                Body = "soft-delete-once",
                IsDeleted = true,
                DeletedAt = (DateTimeOffset?)asOf,
            }
        );
    }

    [Fact]
    public async Task Preview_Path_Rejects_SoftDelete_Rules_When_The_Entity_Lacks_SoftDelete_Metadata()
    {
        await using var db = Host.CreateDbContext();
        var repository = new StaticCategoryRepository(
            new Dictionary<string, IRetentionRuleResolver>
            {
                ["short-lived"] = new StaticRetentionRuleResolver(
                    new RetentionRule(TimeSpan.FromDays(30), Strategy.SoftDelete)
                ),
                ["soft-delete"] = new StaticRetentionRuleResolver(
                    new RetentionRule(TimeSpan.FromDays(30), Strategy.SoftDelete)
                ),
                ["anonymise"] = new StaticRetentionRuleResolver(
                    new RetentionRule(TimeSpan.FromDays(30), Strategy.Anonymise)
                ),
            }
        );
        var preview = new RetentionPreviewService(
            db,
            new RetentionRegistry(db),
            repository,
            new RetentionStartupValidator(db, repository),
            [new PurgeSweepStrategy(), new SoftDeleteSweepStrategy(), new AnonymiseSweepStrategy()]
        );

        var act = () =>
            preview.PreviewAsync(
                new TenantContext(Guid.NewGuid(), "uk", new Dictionary<string, string>()),
                new DateTimeOffset(2026, 4, 12, 12, 0, 0, TimeSpan.Zero)
            );

        await act
            .Should()
            .ThrowAsync<RetentionConfigurationException>()
            .WithMessage(
                $"*Soft-delete convention on {typeof(Note).FullName}: retained SoftDelete categories require a public bool soft-delete flag property (named IsDeleted by convention, or marked with [RetentionSoftDelete]) mapped by EF.*"
            );
    }

    private sealed class StaticCategoryRepository(
        IReadOnlyDictionary<string, IRetentionRuleResolver> resolvers
    ) : IRetentionCategoryRepository
    {
        public Task<IRetentionRuleResolver?> GetAsync(string category, CancellationToken ct)
        {
            resolvers.TryGetValue(category, out var resolver);
            return Task.FromResult(resolver);
        }
    }

}

[Collection("Integration")]
public sealed class SoftDeleteSweepStrategyCommandTests
{
    [Fact]
    public async Task PreviewAsync_Uses_A_Hold_Aware_Count_Query()
    {
        var strategy = new SoftDeleteSweepStrategy();
        var connection = new RecordingDbConnection();
        var tenantId = Guid.NewGuid();
        var now = new DateTimeOffset(2026, 4, 12, 12, 0, 0, TimeSpan.Zero);
        var entry = new RetentionEntry(
            typeof(SoftDeleteRecord),
            "soft_delete_records",
            "soft-delete",
            nameof(SoftDeleteRecord.CreatedAt),
            "CreatedAt",
            new RecordIdConvention(nameof(SoftDeleteRecord.Id), "Id", typeof(Guid)),
            [],
            new TenantConvention(nameof(SoftDeleteRecord.TenantId), "TenantId"),
            new SoftDeleteConvention(
                nameof(SoftDeleteRecord.IsDeleted),
                "IsDeleted",
                nameof(SoftDeleteRecord.DeletedAt),
                "DeletedAt"
            )
        );
        var rule = new RetentionRule(TimeSpan.FromDays(30), Strategy.SoftDelete);
        var context = new RetentionResolutionContext(
            "soft-delete",
            new TenantContext(tenantId, "uk", new Dictionary<string, string>()),
            now,
            []
        );

        var affected = await strategy.PreviewAsync(
            entry,
            rule,
            context,
            connection,
            CancellationToken.None
        );

        affected.Should().Be(1);
        connection.Commands.Should().ContainSingle();
        connection.LastCommand.Should().NotBeNull();
        connection.LastCommand!.AssignedTransaction.Should().BeNull();
        connection.LastCommand.CommandText.Should().Contain("SELECT COUNT(*)");
        connection.LastCommand.CommandText.Should().Contain("\"IsDeleted\" = FALSE");
        connection.LastCommand.CommandText.Should().Contain("@cutoff");
        connection.LastCommand.CommandText.Should().Contain("@tenantId");
        connection.LastCommand.CommandText.Should().Contain("@holdTableName");
        connection.LastCommand.CommandText.Should().Contain("@holdAsOf");
        connection.LastCommand.CommandText.Should().Contain("NOT EXISTS");
        connection.LastCommand.Parameters.Count.Should().Be(4);
        connection.LastCommand.Parameters["cutoff"].Value.Should().Be(now.AddDays(-30));
        connection.LastCommand.Parameters["tenantId"].Value.Should().Be(tenantId);
        connection.LastCommand.Parameters["holdTableName"].Value.Should().Be("soft_delete_records");
        connection.LastCommand.Parameters["holdAsOf"].Value.Should().Be(now);
    }

    [Fact]
    public async Task SweepAsync_Computes_HeldCount_From_Selected_Candidates_And_Targets_Only_Those_Ids()
    {
        var selectedId = Guid.NewGuid();
        var heldId = Guid.NewGuid();
        var strategy = new SoftDeleteSweepStrategy();
        var connection = new RecordingDbConnection();
        connection.EnqueueResultSet(selectedId, heldId);
        connection.EnqueueResultSet(selectedId);
        var transaction = connection.BeginTransaction();
        var tenantId = Guid.NewGuid();
        var now = new DateTimeOffset(2026, 4, 12, 12, 0, 0, TimeSpan.Zero);
        var entry = new RetentionEntry(
            typeof(SoftDeleteRecord),
            "soft_delete_records",
            "soft-delete",
            nameof(SoftDeleteRecord.CreatedAt),
            "CreatedAt",
            new RecordIdConvention(nameof(SoftDeleteRecord.Id), "Id", typeof(Guid)),
            [],
            new TenantConvention(nameof(SoftDeleteRecord.TenantId), "TenantId"),
            new SoftDeleteConvention(
                nameof(SoftDeleteRecord.IsDeleted),
                "IsDeleted",
                nameof(SoftDeleteRecord.DeletedAt),
                "DeletedAt"
            )
        );
        var rule = new RetentionRule(TimeSpan.FromDays(30), Strategy.SoftDelete);
        var context = new RetentionResolutionContext(
            "soft-delete",
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

        affected.AffectedRecordIds.Should().Equal(selectedId.ToString());
        affected.HeldCount.Should().Be(1);
        connection.Commands.Should().HaveCount(2);
        connection.Commands[0].CommandText.Should().Contain("FOR UPDATE");
        connection.Commands[1].CommandText.Should().Contain("ANY(@candidateIds)");
        connection.Commands[1].Parameters["candidateIds"].Value.Should().BeEquivalentTo(
            new[] { selectedId.ToString(), heldId.ToString() }
        );
    }

    [Fact]
    public async Task SweepAsync_Uses_The_Mapped_Record_Id_Column_In_Hold_Filtering()
    {
        var strategy = new SoftDeleteSweepStrategy();
        var connection = new RecordingDbConnection();
        connection.EnqueueResultSet(Guid.NewGuid());
        connection.EnqueueResultSet(Guid.NewGuid());
        var transaction = connection.BeginTransaction();
        var tenantId = Guid.NewGuid();
        var now = new DateTimeOffset(2026, 4, 12, 12, 0, 0, TimeSpan.Zero);
        var entry = new RetentionEntry(
            typeof(SoftDeleteRecord),
            "soft_delete_records",
            "soft-delete",
            nameof(SoftDeleteRecord.CreatedAt),
            "CreatedAt",
            new RecordIdConvention(nameof(SoftDeleteRecord.Id), "record_id", typeof(Guid)),
            [],
            new TenantConvention(nameof(SoftDeleteRecord.TenantId), "TenantId"),
            new SoftDeleteConvention(
                nameof(SoftDeleteRecord.IsDeleted),
                "IsDeleted",
                nameof(SoftDeleteRecord.DeletedAt),
                "DeletedAt"
            )
        );
        var rule = new RetentionRule(TimeSpan.FromDays(30), Strategy.SoftDelete);
        var context = new RetentionResolutionContext(
            "soft-delete",
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

        affected.AffectedRecordIds.Should().ContainSingle();
        affected.HeldCount.Should().Be(0);
        connection.LastCommand.Should().NotBeNull();
        connection.LastCommand!.CommandText.Should().Contain("hold.\"RecordId\" = CAST(target.\"record_id\" AS text)");
    }

    private sealed class RecordingDbConnection : DbConnection
    {
        private ConnectionState state = ConnectionState.Closed;
        private string connectionString = "Host=recording";
        private readonly Queue<Guid[]> queuedResultSets = new();

        public RecordingDbCommand? LastCommand { get; private set; }
        public List<RecordingDbCommand> Commands { get; } = [];

        public void EnqueueResultSet(params Guid[] values)
        {
            queuedResultSets.Enqueue(values);
        }

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
            Commands.Add(LastCommand);
            return LastCommand;
        }

        public Guid[] DequeueResultSet()
        {
            return queuedResultSets.Count > 0 ? queuedResultSets.Dequeue() : [Guid.NewGuid()];
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
            return 1;
        }

        public override void Prepare() { }

        protected override DbParameter CreateDbParameter()
        {
            return new RecordingDbParameter();
        }

        protected override DbDataReader ExecuteDbDataReader(CommandBehavior behavior)
        {
            return new GuidSequenceDbDataReader(connection.DequeueResultSet());
        }

        public override Task<int> ExecuteNonQueryAsync(CancellationToken cancellationToken)
        {
            return Task.FromResult(1);
        }
    }

    private sealed class GuidSequenceDbDataReader(IReadOnlyList<Guid> values) : DbDataReader
    {
        private int index = -1;

        public override int FieldCount => 1;
        public override bool HasRows => values.Count > 0;
        public override bool IsClosed => false;
        public override int RecordsAffected => 1;
        public override int Depth => 0;
        public override object this[int ordinal] => GetValue(ordinal);
        public override object this[string name] => GetValue(GetOrdinal(name));

        public override bool Read()
        {
            if (index + 1 >= values.Count)
            {
                return false;
            }

            index++;
            return true;
        }

        public override Task<bool> ReadAsync(CancellationToken cancellationToken) => Task.FromResult(Read());
        public override bool NextResult() => false;
        public override Task<bool> NextResultAsync(CancellationToken cancellationToken) => Task.FromResult(false);
        public override Guid GetGuid(int ordinal) => values[index];
        public override object GetValue(int ordinal) => values[index];
        public override int GetValues(object[] items)
        {
            items[0] = values[index];
            return 1;
        }

        public override string GetName(int ordinal) => "Id";
        public override string GetDataTypeName(int ordinal) => nameof(Guid);
        public override Type GetFieldType(int ordinal) => typeof(Guid);
        public override int GetOrdinal(string name) => 0;
        public override bool IsDBNull(int ordinal) => false;
        public override IEnumerator GetEnumerator() => values.GetEnumerator();

        public override bool GetBoolean(int ordinal) => throw new NotSupportedException();
        public override byte GetByte(int ordinal) => throw new NotSupportedException();
        public override long GetBytes(int ordinal, long dataOffset, byte[]? buffer, int bufferOffset, int length) => throw new NotSupportedException();
        public override char GetChar(int ordinal) => throw new NotSupportedException();
        public override long GetChars(int ordinal, long dataOffset, char[]? buffer, int bufferOffset, int length) => throw new NotSupportedException();
        public override string GetString(int ordinal) => throw new NotSupportedException();
        public override short GetInt16(int ordinal) => throw new NotSupportedException();
        public override int GetInt32(int ordinal) => throw new NotSupportedException();
        public override long GetInt64(int ordinal) => throw new NotSupportedException();
        public override float GetFloat(int ordinal) => throw new NotSupportedException();
        public override double GetDouble(int ordinal) => throw new NotSupportedException();
        public override decimal GetDecimal(int ordinal) => throw new NotSupportedException();
        public override DateTime GetDateTime(int ordinal) => throw new NotSupportedException();
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
                items.RemoveAt(index);
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
