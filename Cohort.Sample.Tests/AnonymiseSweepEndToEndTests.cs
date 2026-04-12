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

public sealed class AnonymiseSweepEndToEndTests(PostgresFixture fixture)
    : IntegrationTestBase(fixture)
{
    [Fact]
    public async Task Sweep_Path_Anonymises_Only_Expired_Rows_For_The_Target_Tenant_And_Leaves_Unmarked_Columns_Unchanged()
    {
        var tenantA = Guid.NewGuid();
        var tenantB = Guid.NewGuid();
        var asOf = new DateTimeOffset(2026, 4, 12, 12, 0, 0, TimeSpan.Zero);

        await using (var db = Host.CreateDbContext())
        {
            db.AnonymisedContacts.AddRange(
                new AnonymisedContact
                {
                    Id = Guid.NewGuid(),
                    TenantId = tenantA,
                    CreatedAt = asOf.AddDays(-120),
                    EmailAddress = "expired@example.com",
                    GivenName = "Alice",
                    Surname = "Smith",
                    Notes = "keep-expired-notes",
                },
                new AnonymisedContact
                {
                    Id = Guid.NewGuid(),
                    TenantId = tenantA,
                    CreatedAt = asOf.AddDays(-5),
                    EmailAddress = "newer@example.com",
                    GivenName = "Bob",
                    Surname = "Jones",
                    Notes = "keep-newer-notes",
                },
                new AnonymisedContact
                {
                    Id = Guid.NewGuid(),
                    TenantId = tenantB,
                    CreatedAt = asOf.AddDays(-120),
                    EmailAddress = "other-tenant@example.com",
                    GivenName = "Cara",
                    Surname = "Mills",
                    Notes = "keep-other-tenant-notes",
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
                typeof(AnonymisedContact),
                "anonymise",
                tenantA,
                Strategy.Anonymise,
                1
            )
        );

        await using var verify = Host.CreateDbContext();
        var contacts = await verify
            .AnonymisedContacts.OrderBy(contact => contact.Notes)
            .Select(contact => new
            {
                contact.EmailAddress,
                contact.GivenName,
                contact.Surname,
                contact.Notes,
            })
            .ToListAsync();

        contacts.Should().Equal(
            new
            {
                EmailAddress = (string?)null,
                GivenName = string.Empty,
                Surname = "[redacted]",
                Notes = "keep-expired-notes",
            },
            new
            {
                EmailAddress = (string?)"newer@example.com",
                GivenName = "Bob",
                Surname = "Jones",
                Notes = "keep-newer-notes",
            },
            new
            {
                EmailAddress = (string?)"other-tenant@example.com",
                GivenName = "Cara",
                Surname = "Mills",
                Notes = "keep-other-tenant-notes",
            }
        );
    }

    [Fact]
    public async Task Startup_Path_Fails_When_An_Anonymise_Category_Has_No_Annotated_Fields()
    {
        await using var db = Host.CreateDbContext();
        var connectionString = db.Database.GetConnectionString()!;

        using var host = new CohortTestHost(
            connectionString,
            new StaticCategoryRepository(
                new Dictionary<string, IRetentionRuleResolver>
                {
                    ["short-lived"] = new StaticRetentionRuleResolver(
                        new RetentionRule(TimeSpan.FromDays(30), Strategy.Anonymise)
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
                $"Anonymise convention on {typeof(Note).FullName}: retained Anonymise categories require at least one [Anonymise]-annotated property mapped by EF."
            );
    }

    [Fact]
    public async Task Sweep_Path_Can_Run_Twice_Without_Reintroducing_Scrubbed_Data()
    {
        var tenantId = Guid.NewGuid();
        var asOf = new DateTimeOffset(2026, 4, 12, 12, 0, 0, TimeSpan.Zero);

        await using (var db = Host.CreateDbContext())
        {
            db.AnonymisedContacts.Add(
                new AnonymisedContact
                {
                    Id = Guid.NewGuid(),
                    TenantId = tenantId,
                    CreatedAt = asOf.AddDays(-120),
                    EmailAddress = "repeat@example.com",
                    GivenName = "Repeat",
                    Surname = "Target",
                    Notes = "repeat-notes",
                }
            );
            await db.SaveChangesAsync();
        }

        var tenant = new TenantContext(tenantId, "uk", new Dictionary<string, string>());

        var first = await Host.RunSweepAsync(tenant, asOf);
        var second = await Host.RunSweepAsync(tenant, asOf);

        first.Counts.Should().Contain(
            new EntitySweepCount(
                typeof(AnonymisedContact),
                "anonymise",
                tenantId,
                Strategy.Anonymise,
                1
            )
        );
        second.Counts.Should().Contain(
            count =>
                count.EntityType == typeof(AnonymisedContact)
                && count.Category == "anonymise"
                && count.TenantId == tenantId
                && count.Strategy == Strategy.Anonymise
        );

        await using var verify = Host.CreateDbContext();
        var contact = await verify.AnonymisedContacts.SingleAsync();

        contact.EmailAddress.Should().BeNull();
        contact.GivenName.Should().BeEmpty();
        contact.Surname.Should().Be("[redacted]");
        contact.Notes.Should().Be("repeat-notes");
    }

    [Fact]
    public async Task Sweep_Path_Does_Not_Anonymise_Rows_Exactly_On_The_Cutoff()
    {
        var tenantId = Guid.NewGuid();
        var asOf = new DateTimeOffset(2026, 4, 12, 12, 0, 0, TimeSpan.Zero);

        await using (var db = Host.CreateDbContext())
        {
            db.AnonymisedContacts.AddRange(
                new AnonymisedContact
                {
                    Id = Guid.NewGuid(),
                    TenantId = tenantId,
                    CreatedAt = asOf.AddDays(-31),
                    EmailAddress = "expired-boundary@example.com",
                    GivenName = "Expired",
                    Surname = "Contact",
                    Notes = "expired-before-cutoff",
                },
                new AnonymisedContact
                {
                    Id = Guid.NewGuid(),
                    TenantId = tenantId,
                    CreatedAt = asOf.AddDays(-30),
                    EmailAddress = "boundary@example.com",
                    GivenName = "Boundary",
                    Surname = "Contact",
                    Notes = "exact-cutoff-boundary",
                }
            );
            await db.SaveChangesAsync();
        }

        var result = await Host.RunSweepAsync(
            new TenantContext(tenantId, "uk", new Dictionary<string, string>()),
            asOf
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

        await using var verify = Host.CreateDbContext();
        var contacts = await verify
            .AnonymisedContacts.OrderBy(contact => contact.Notes)
            .Select(contact => new
            {
                contact.EmailAddress,
                contact.GivenName,
                contact.Surname,
                contact.Notes,
            })
            .ToListAsync();

        contacts.Should().Equal(
            new
            {
                EmailAddress = (string?)"boundary@example.com",
                GivenName = "Boundary",
                Surname = "Contact",
                Notes = "exact-cutoff-boundary",
            },
            new
            {
                EmailAddress = (string?)null,
                GivenName = string.Empty,
                Surname = "[redacted]",
                Notes = "expired-before-cutoff",
            }
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
public sealed class AnonymiseSweepStrategyCommandTests
{
    [Fact]
    public async Task SweepAsync_Uses_Parameterized_Assignments_For_All_Anonymise_Methods()
    {
        var strategy = new AnonymiseSweepStrategy();
        var connection = new RecordingDbConnection();
        var transaction = connection.BeginTransaction();
        var tenantId = Guid.NewGuid();
        var now = new DateTimeOffset(2026, 4, 12, 12, 0, 0, TimeSpan.Zero);
        var entry = new RetentionEntry(
            typeof(AnonymisedContact),
            "anonymised_contacts",
            "anonymise",
            nameof(AnonymisedContact.CreatedAt),
            "CreatedAt",
            [
                new AnonymiseField(nameof(AnonymisedContact.EmailAddress), "EmailAddress", AnonymiseMethod.Null),
                new AnonymiseField(nameof(AnonymisedContact.GivenName), "GivenName", AnonymiseMethod.EmptyString),
                new AnonymiseField(
                    nameof(AnonymisedContact.Surname),
                    "Surname",
                    AnonymiseMethod.FixedLiteral,
                    "[redacted]"
                ),
            ],
            new TenantConvention(nameof(AnonymisedContact.TenantId), "TenantId"),
            null
        );
        var rule = new RetentionRule(TimeSpan.FromDays(30), Strategy.Anonymise);
        var context = new RetentionResolutionContext(
            "anonymise",
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
        connection.LastCommand.CommandText.Should().Contain("\"EmailAddress\" = @value0");
        connection.LastCommand.CommandText.Should().Contain("\"GivenName\" = @value1");
        connection.LastCommand.CommandText.Should().Contain("\"Surname\" = @value2");
        connection.LastCommand.CommandText.Should().Contain("@cutoff");
        connection.LastCommand.CommandText.Should().Contain("@tenantId");
        connection.LastCommand.Parameters.Count.Should().Be(5);
        connection.LastCommand.Parameters.Contains("value0").Should().BeTrue();
        connection.LastCommand.Parameters.Contains("value1").Should().BeTrue();
        connection.LastCommand.Parameters.Contains("value2").Should().BeTrue();
        connection.LastCommand.Parameters.Contains("cutoff").Should().BeTrue();
        connection.LastCommand.Parameters.Contains("tenantId").Should().BeTrue();
        connection.LastCommand.Parameters["value0"].Value.Should().Be(DBNull.Value);
        connection.LastCommand.Parameters["value1"].Value.Should().Be(string.Empty);
        connection.LastCommand.Parameters["value2"].Value.Should().Be("[redacted]");
        connection.LastCommand.Parameters["cutoff"].Value.Should().Be(now.AddDays(-30));
        connection.LastCommand.Parameters["tenantId"].Value.Should().Be(tenantId);
    }

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
