#nullable enable

using Cohort.Application;
using Cohort.Infrastructure.Handlers;

namespace Cohort.Infrastructure.Migrations;

internal static class CohortSchemaContract
{
    internal static readonly IReadOnlyList<TableRequirement> Tables =
    [
        new(
            CohortTableNames.RetentionHolds,
            CohortSharedTypeNames.RetentionHold,
            static tables => tables.RetentionHolds,
            [
                Column<Guid>("HoldId", "uuid"),
                Column<Guid>("RetentionEntityId", "uuid"),
                Column<string>("RecordId", "text"),
                Column<Guid?>("TenantId", "uuid", nullable: true),
                Column<string>("Reason", "text"),
                Column<DateTimeOffset>("CreatedAt", "timestamp with time zone", "timestamptz"),
                Column<DateTimeOffset?>("ExpiresAt", "timestamp with time zone", "timestamptz", nullable: true),
                Column<DateTimeOffset?>("RemovedAt", "timestamp with time zone", "timestamptz", nullable: true),
            ],
            ["HoldId"],
            [
                new(["RetentionEntityId", "TenantId", "RecordId"]),
                new(["RetentionEntityId", "RecordId"]),
            ]
        ),
        new(
            CohortTableNames.SweepRun,
            CohortSharedTypeNames.SweepRun,
            static tables => tables.SweepRun,
            [
                Column<Guid>("SweepId", "uuid"),
                Column<DateTimeOffset>("StartedAt", "timestamp with time zone", "timestamptz"),
                Column<int>("Status", "integer", "int4"),
                Column<DateTimeOffset?>("SettledAt", "timestamp with time zone", "timestamptz", nullable: true),
                Column<TimeSpan?>("Duration", "interval", nullable: true),
                Column<int>("TriggerKind", "integer", "int4"),
                Column<bool>("DryRun", "boolean", "bool"),
                Column<Guid>("TenantId", "uuid"),
                Column<long?>("TotalAffected", "bigint", "int8", nullable: true),
                Column<string?>("Error", "text", nullable: true),
            ],
            ["SweepId"],
            Checks:
            [
                new(
                    "CK_sweep_run_Status_Range",
                    "\"Status\" BETWEEN 0 AND 4",
                    "Status>=0ANDStatus<=4"
                ),
                new(
                    "CK_sweep_run_Started_Unsettled",
                    "\"Status\" <> 0 OR \"SettledAt\" IS NULL",
                    "Status<>0ORSettledAtISNULL"
                ),
                new(
                    "CK_sweep_run_Terminal_Settled",
                    "\"Status\" = 0 OR \"SettledAt\" IS NOT NULL",
                    "Status=0ORSettledAtISNOTNULL"
                ),
                new(
                    "CK_sweep_run_TotalAffected_Nonnegative",
                    "\"TotalAffected\" IS NULL OR \"TotalAffected\" >= 0",
                    "TotalAffectedISNULLORTotalAffected>=0"
                ),
                new(
                    "CK_sweep_run_Duration_Nonnegative",
                    "\"Duration\" IS NULL OR \"Duration\" >= INTERVAL '0'",
                    "DurationISNULLORDuration>=000000INTERVAL"
                )
            ]
        ),
        new(
            CohortTableNames.SweepRunEntitySummary,
            CohortSharedTypeNames.SweepRunEntitySummary,
            static tables => tables.SweepRunEntitySummary,
            [
                Column<Guid>("SweepId", "uuid"),
                Column<DateTimeOffset>("At", "timestamp with time zone", "timestamptz"),
                Column<string>("EntityType", "text"),
                Column<Guid>("RetentionEntityId", "uuid"),
                Column<string>("Category", "text"),
                Column<Guid>("TenantId", "uuid"),
                Column<int>("Strategy", "integer", "int4"),
                Column<TimeSpan>("ResolvedPeriod", "interval"),
                Column<long>("Affected", "bigint", "int8"),
                Column<long>("HeldCount", "bigint", "int8"),
                Column<long>("SkippedCount", "bigint", "int8"),
                Column<long>("NullAnchorCount", "bigint", "int8"),
                Column<string?>("RuleSource", "text", nullable: true),
                Column<string?>("RuleReason", "text", nullable: true),
            ],
            ["SweepId", "RetentionEntityId", "Category", "TenantId", "Strategy"],
            [new(["SweepId"])],
            ForeignKeys:
            [
                new(["SweepId"], CohortTableNames.SweepRun, ["SweepId"], ForeignKeyDeleteAction.Restrict),
            ]
        ),
        new(
            CohortTableNames.SweepRunRowDetail,
            CohortSharedTypeNames.SweepRunRowDetail,
            static tables => tables.SweepRunRowDetail,
            [
                Column<long>("Id", "bigint", "int8", generated: true),
                Column<Guid>("SweepId", "uuid"),
                Column<DateTimeOffset>("At", "timestamp with time zone", "timestamptz"),
                Column<string>("EntityType", "text"),
                Column<Guid>("RetentionEntityId", "uuid"),
                Column<string>("RecordId", "text"),
                Column<string>("Category", "text"),
                Column<int>("Strategy", "integer", "int4"),
                Column<Guid>("TenantId", "uuid"),
                Column<string?>("CapturedPayload", "text", nullable: true),
            ],
            ["Id"],
            [
                new(["SweepId"]),
                new(
                    ["SweepId", "RetentionEntityId", "RecordId", "Category", "Strategy", "TenantId"],
                    Unique: true,
                    Name: "IX_sweep_run_row_detail_StableIdentity"
                ),
            ],
            ForeignKeys:
            [
                new(["SweepId"], CohortTableNames.SweepRun, ["SweepId"], ForeignKeyDeleteAction.Restrict),
            ]
        ),
        new(
            CohortTableNames.SweepRowHandlerStatus,
            SharedTypeName: null,
            static tables => tables.SweepRowHandlerStatus,
            [
                Column<long>("Id", "bigint", "int8", generated: true),
                Column<long>("SweepRunRowDetailId", "bigint", "int8"),
                Column<string>("HandlerType", "text"),
                Column<RowHandlerDispatchPhase>("DispatchPhase", "integer", "int4"),
                Column<SweepRowHandlerDispatchState>("State", "integer", "int4"),
                Column<int>("Attempt", "integer", "int4"),
                Column<DateTimeOffset>("QueuedAt", "timestamp with time zone", "timestamptz"),
                Column<DateTimeOffset>("NextAttemptAt", "timestamp with time zone", "timestamptz"),
                Column<DateTimeOffset?>("ClaimedAt", "timestamp with time zone", "timestamptz", nullable: true),
                Column<Guid?>("ClaimToken", "uuid", nullable: true),
                Column<DateTimeOffset?>("CompletedAt", "timestamp with time zone", "timestamptz", nullable: true),
                Column<string?>("LastError", "text", nullable: true),
            ],
            ["Id"],
            [
                new(["SweepRunRowDetailId", "HandlerType"], Unique: true),
                new(["State", "NextAttemptAt", "Id"]),
            ],
            [
                new(
                    "CK_sweep_row_handler_status_Claim",
                    "(\"State\" = 1 AND \"ClaimedAt\" IS NOT NULL AND \"ClaimToken\" IS NOT NULL) OR (\"State\" <> 1 AND \"ClaimedAt\" IS NULL AND \"ClaimToken\" IS NULL)",
                    "(State=1ANDClaimedAtISNOTNULLANDClaimTokenISNOTNULL)OR(State<>1ANDClaimedAtISNULLANDClaimTokenISNULL)"
                ),
                new(
                    "CK_sweep_row_handler_status_Completion",
                    "(\"State\" IN (2, 3) AND \"CompletedAt\" IS NOT NULL) OR (\"State\" IN (0, 1) AND \"CompletedAt\" IS NULL)",
                    "(State=ANYARRAY[2,3]ANDCompletedAtISNOTNULL)OR(State=ANYARRAY[0,1]ANDCompletedAtISNULL)"
                ),
            ],
            [
                new(
                    ["SweepRunRowDetailId"],
                    CohortTableNames.SweepRunRowDetail,
                    ["Id"],
                    ForeignKeyDeleteAction.Cascade
                ),
            ]
        ),
    ];

    internal static IReadOnlyList<string> TableNames { get; } =
        Tables.Select(table => table.Role).ToArray();

    internal static TableRequirement GetTable(string role) =>
        Tables.Single(table => table.Role == role);

    private static ColumnRequirement Column<T>(
        string name,
        string storeType,
        string? catalogType = null,
        bool nullable = false,
        bool generated = false
    ) => new(name, typeof(T), storeType, catalogType ?? storeType, nullable, generated);

    internal sealed record TableRequirement(
        string Role,
        string? SharedTypeName,
        Func<CohortStoreTables, RelationalObjectName> ResolveStoreTable,
        IReadOnlyList<ColumnRequirement> Columns,
        IReadOnlyList<string> PrimaryKey,
        IReadOnlyList<IndexRequirement>? Indexes = null,
        IReadOnlyList<CheckConstraintRequirement>? Checks = null,
        IReadOnlyList<ForeignKeyRequirement>? ForeignKeys = null
    )
    {
        internal string Name => Role;
        internal IReadOnlyList<IndexRequirement> RequiredIndexes => Indexes ?? [];
        internal IReadOnlyList<CheckConstraintRequirement> RequiredChecks => Checks ?? [];
        internal IReadOnlyList<ForeignKeyRequirement> RequiredForeignKeys => ForeignKeys ?? [];
    }

    internal sealed record ColumnRequirement(
        string Name,
        Type ClrType,
        string StoreType,
        string CatalogType,
        bool Nullable,
        bool Generated
    );

    internal sealed record IndexRequirement(
        IReadOnlyList<string> Columns,
        bool Unique = false,
        string? Predicate = null,
        string? Name = null
    );

    internal sealed record CheckConstraintRequirement(string Name, string Sql, string NormalizedSql);

    internal sealed record ForeignKeyRequirement(
        IReadOnlyList<string> Columns,
        string PrincipalTable,
        IReadOnlyList<string> PrincipalColumns,
        ForeignKeyDeleteAction DeleteAction
    )
    {
        internal char CatalogDeleteAction => DeleteAction switch
        {
            ForeignKeyDeleteAction.Restrict => 'r',
            ForeignKeyDeleteAction.Cascade => 'c',
            _ => throw new ArgumentOutOfRangeException(nameof(DeleteAction)),
        };
    }

    internal enum ForeignKeyDeleteAction
    {
        Restrict,
        Cascade,
    }
}

internal static class CohortTableNames
{
    internal const string RetentionHolds = "retention_holds";
    internal const string SweepRun = "sweep_run";
    internal const string SweepRunEntitySummary = "sweep_run_entity_summary";
    internal const string SweepRunRowDetail = "sweep_run_row_detail";
    internal const string SweepRowHandlerStatus = "sweep_row_handler_status";
}

internal static class CohortSharedTypeNames
{
    internal const string RetentionHold = "Cohort.RetentionHold";
    internal const string SweepRun = "Cohort.SweepRun";
    internal const string SweepRunEntitySummary = "Cohort.SweepRunEntitySummary";
    internal const string SweepRunRowDetail = "Cohort.SweepRunRowDetail";
}
