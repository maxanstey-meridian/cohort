using Npgsql;

namespace Cohort.Sample.Tests;

internal static class LegacyCohortSchema
{
    private const string ProductVersion = "9.0.0";

    private static readonly string[] AppliedMigrationIds =
    [
        "20260411203255_Initial",
        "20260412001500_AddNoteTenant",
        "20260412103000_AddSoftDeleteRecord",
        "20260412123000_AddAnonymisedContact",
        "20260412013754_AddRetentionAuditTables",
        "20260412153000_AddRetentionHolds",
        "20260412170000_AddErasureTriggerSurface",
    ];

    public static async Task BootstrapPreRowDispatchAsync(string connectionString)
    {
        await using var connection = new NpgsqlConnection(connectionString);
        await connection.OpenAsync();

        await using (var command = connection.CreateCommand())
        {
            command.CommandText =
                """
                CREATE TABLE "__EFMigrationsHistory" (
                    "MigrationId" character varying(150) NOT NULL,
                    "ProductVersion" character varying(32) NOT NULL,
                    CONSTRAINT "PK___EFMigrationsHistory" PRIMARY KEY ("MigrationId")
                );

                CREATE TABLE "notes" (
                    "Id" uuid NOT NULL,
                    "CreatedAt" timestamp with time zone NOT NULL,
                    "Body" text NOT NULL,
                    "TenantId" uuid NULL,
                    "SubjectId" uuid NULL,
                    CONSTRAINT "PK_notes" PRIMARY KEY ("Id")
                );

                CREATE TABLE "soft_delete_records" (
                    "Id" uuid NOT NULL,
                    "TenantId" uuid NOT NULL,
                    "CreatedAt" timestamp with time zone NOT NULL,
                    "Body" text NOT NULL,
                    "IsDeleted" boolean NOT NULL,
                    "DeletedAt" timestamp with time zone NULL,
                    "SubjectId" uuid NULL,
                    CONSTRAINT "PK_soft_delete_records" PRIMARY KEY ("Id")
                );

                CREATE TABLE "anonymised_contacts" (
                    "Id" uuid NOT NULL,
                    "TenantId" uuid NOT NULL,
                    "CreatedAt" timestamp with time zone NOT NULL,
                    "EmailAddress" text NULL,
                    "GivenName" text NOT NULL,
                    "Surname" text NOT NULL,
                    "Notes" text NOT NULL,
                    "SubjectId" uuid NULL,
                    CONSTRAINT "PK_anonymised_contacts" PRIMARY KEY ("Id")
                );

                CREATE TABLE "erasure_subject_records" (
                    "Id" uuid NOT NULL,
                    "TenantId" uuid NOT NULL,
                    "SubjectId" uuid NULL,
                    "CreatedAt" timestamp with time zone NOT NULL,
                    "Body" text NOT NULL,
                    CONSTRAINT "PK_erasure_subject_records" PRIMARY KEY ("Id")
                );

                CREATE TABLE "retention_holds" (
                    "HoldId" uuid NOT NULL,
                    "TableName" text NOT NULL,
                    "RecordId" text NOT NULL,
                    "TenantId" uuid NOT NULL,
                    "Reason" text NOT NULL,
                    "CreatedAt" timestamp with time zone NOT NULL,
                    "ExpiresAt" timestamp with time zone NULL,
                    "RemovedAt" timestamp with time zone NULL,
                    CONSTRAINT "PK_retention_holds" PRIMARY KEY ("HoldId")
                );

                CREATE INDEX "IX_retention_holds_TableName_TenantId_RecordId"
                    ON "retention_holds" ("TableName", "TenantId", "RecordId");

                CREATE TABLE "sweep_run" (
                    "SweepId" uuid NOT NULL,
                    "CompletedAt" timestamp with time zone NULL,
                    "DryRun" boolean NOT NULL,
                    "Duration" interval NULL,
                    "StartedAt" timestamp with time zone NOT NULL,
                    "TenantId" uuid NOT NULL,
                    "TotalAffected" integer NULL,
                    "TriggerKind" integer NOT NULL,
                    CONSTRAINT "PK_sweep_run" PRIMARY KEY ("SweepId")
                );

                CREATE TABLE "sweep_run_entity_summary" (
                    "SweepId" uuid NOT NULL,
                    "EntityType" text NOT NULL,
                    "Category" text NOT NULL,
                    "TenantId" uuid NOT NULL,
                    "Strategy" integer NOT NULL,
                    "Affected" integer NOT NULL,
                    "At" timestamp with time zone NOT NULL,
                    "HeldCount" integer NOT NULL,
                    "ResolvedPeriod" interval NOT NULL,
                    CONSTRAINT "PK_sweep_run_entity_summary"
                        PRIMARY KEY ("SweepId", "EntityType", "Category", "TenantId", "Strategy")
                );

                CREATE INDEX "IX_sweep_run_entity_summary_SweepId"
                    ON "sweep_run_entity_summary" ("SweepId");

                CREATE TABLE "sweep_run_row_detail" (
                    "SweepId" uuid NOT NULL,
                    "EntityType" text NOT NULL,
                    "EntityId" text NOT NULL,
                    "Category" text NOT NULL,
                    "Strategy" integer NOT NULL,
                    "TenantId" uuid NOT NULL,
                    "At" timestamp with time zone NOT NULL,
                    CONSTRAINT "PK_sweep_run_row_detail"
                        PRIMARY KEY ("SweepId", "EntityType", "EntityId", "Category", "Strategy", "TenantId")
                );

                CREATE INDEX "IX_sweep_run_row_detail_SweepId"
                    ON "sweep_run_row_detail" ("SweepId");
                """;

            await command.ExecuteNonQueryAsync();
        }

        foreach (var migrationId in AppliedMigrationIds)
        {
            await using var command = connection.CreateCommand();
            command.CommandText =
                """
                INSERT INTO "__EFMigrationsHistory" ("MigrationId", "ProductVersion")
                VALUES (@migrationId, @productVersion)
                """;
            command.Parameters.AddWithValue("migrationId", migrationId);
            command.Parameters.AddWithValue("productVersion", ProductVersion);
            await command.ExecuteNonQueryAsync();
        }
    }
}
