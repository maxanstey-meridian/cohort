using System;
using Microsoft.EntityFrameworkCore.Migrations;

#nullable disable

namespace Cohort.Sample.Migrations
{
    /// <inheritdoc />
    public partial class AddStableRetentionEntityIdentity : Migration
    {
        /// <inheritdoc />
        protected override void Up(MigrationBuilder migrationBuilder)
        {
            migrationBuilder.DropIndex(
                name: "IX_retention_holds_TableName_RecordId",
                table: "retention_holds"
            );

            migrationBuilder.DropIndex(
                name: "IX_retention_holds_TableName_TenantId_RecordId",
                table: "retention_holds"
            );

            migrationBuilder.AddColumn<Guid>(
                name: "RetentionEntityId",
                table: "retention_holds",
                type: "uuid",
                nullable: true
            );

            migrationBuilder.AlterColumn<Guid>(
                name: "TenantId",
                table: "retention_holds",
                type: "uuid",
                nullable: true,
                oldClrType: typeof(Guid),
                oldType: "uuid"
            );

            migrationBuilder.Sql(
                """
                DO $$
                DECLARE
                    unmapped_table_names text;
                BEGIN
                    SELECT string_agg(DISTINCT "TableName", ', ' ORDER BY "TableName")
                    INTO unmapped_table_names
                    FROM "retention_holds"
                    WHERE "TableName" NOT IN (
                        'notes',
                        'blob_backed_files',
                        'per_row_audited_logs',
                        'external_numbered_logs',
                        'tenantless_logs',
                        'anonymised_contacts',
                        'tombstone_records',
                        'soft_delete_records',
                        'tenantless_soft_deletes',
                        'nullable_anchor_events'
                    );

                    IF unmapped_table_names IS NOT NULL THEN
                        RAISE EXCEPTION USING MESSAGE =
                            'Cannot assign stable retention entity identities: retention_holds contains unmapped TableName values: '
                            || unmapped_table_names
                            || '. Add an explicit mapping before applying migration 20260711160000_AddStableRetentionEntityIdentity.';
                    END IF;
                END $$;

                UPDATE "retention_holds"
                SET "RetentionEntityId" = CASE "TableName"
                    WHEN 'notes' THEN 'a3f467fe-c5d0-4f17-9897-83c373cc1dc8'::uuid
                    WHEN 'blob_backed_files' THEN '2fb1804d-9ad8-4543-a177-5d4cd14d62ee'::uuid
                    WHEN 'per_row_audited_logs' THEN '42670ee7-c26a-4a2a-a2ab-d9571db7d4f6'::uuid
                    WHEN 'external_numbered_logs' THEN 'd0991164-8823-4f4e-aac1-f9d8d1753764'::uuid
                    WHEN 'tenantless_logs' THEN '992a65db-d658-4b76-aaf5-b11ca52c4a8f'::uuid
                    WHEN 'anonymised_contacts' THEN 'fd4a533e-e6a9-44ea-948e-cbf881f35e57'::uuid
                    WHEN 'tombstone_records' THEN '6ebbc096-d3b8-4077-8f21-bf9b4d53c869'::uuid
                    WHEN 'soft_delete_records' THEN '6107ff39-bf33-413c-889e-6347c909ba15'::uuid
                    WHEN 'tenantless_soft_deletes' THEN '36d4a1a6-f2d8-40a8-84ea-5a062fc82889'::uuid
                    WHEN 'nullable_anchor_events' THEN '314fd4f7-f771-4b94-ab6e-7fc0a09a6ef5'::uuid
                    ELSE NULL
                END;

                UPDATE "retention_holds" SET "TenantId" = NULL
                WHERE "RetentionEntityId" IN (
                    '992a65db-d658-4b76-aaf5-b11ca52c4a8f'::uuid,
                    '36d4a1a6-f2d8-40a8-84ea-5a062fc82889'::uuid
                );
                """
            );

            migrationBuilder.AlterColumn<Guid>(
                name: "RetentionEntityId",
                table: "retention_holds",
                type: "uuid",
                nullable: false,
                oldClrType: typeof(Guid),
                oldType: "uuid",
                oldNullable: true
            );

            migrationBuilder.DropColumn(name: "TableName", table: "retention_holds");

            migrationBuilder.CreateIndex(
                name: "IX_retention_holds_RetentionEntityId_RecordId",
                table: "retention_holds",
                columns: new[] { "RetentionEntityId", "RecordId" }
            );

            migrationBuilder.CreateIndex(
                name: "IX_retention_holds_RetentionEntityId_TenantId_RecordId",
                table: "retention_holds",
                columns: new[] { "RetentionEntityId", "TenantId", "RecordId" }
            );

            migrationBuilder.DropIndex(
                name: "IX_sweep_run_row_detail_SweepId_EntityType_EntityId_Category_S~",
                table: "sweep_run_row_detail"
            );

            migrationBuilder.AddColumn<Guid>(
                name: "RetentionEntityId",
                table: "sweep_run_row_detail",
                type: "uuid",
                nullable: true
            );

            migrationBuilder.AddColumn<Guid>(
                name: "RetentionEntityId",
                table: "sweep_run_entity_summary",
                type: "uuid",
                nullable: true
            );

            migrationBuilder.Sql(
                """
                DO $$
                DECLARE
                    unmapped_entity_types text;
                BEGIN
                    SELECT string_agg("EntityType", ', ' ORDER BY "EntityType")
                    INTO unmapped_entity_types
                    FROM (
                        SELECT DISTINCT "EntityType"
                        FROM (
                            SELECT "EntityType" FROM "sweep_run_entity_summary"
                            UNION
                            SELECT "EntityType" FROM "sweep_run_row_detail"
                        ) AS audit_entity_types
                        WHERE "EntityType" NOT IN (
                            'Cohort.Sample.Entities.Note',
                            'Cohort.Sample.Entities.BlobBackedFile',
                            'Cohort.Sample.Entities.PerRowAuditedLog',
                            'Cohort.Sample.Entities.ExternalNumberedLog',
                            'Cohort.Sample.Entities.TenantlessLog',
                            'Cohort.Sample.Entities.AnonymisedContact',
                            'Cohort.Sample.Entities.TombstoneRecord',
                            'Cohort.Sample.Entities.SoftDeleteRecord',
                            'Cohort.Sample.Entities.TenantlessSoftDelete',
                            'Cohort.Sample.Entities.NullableAnchorEvent'
                        )
                    ) AS unmapped;

                    IF unmapped_entity_types IS NOT NULL THEN
                        RAISE EXCEPTION USING MESSAGE =
                            'Cannot assign stable retention entity identities: sweep audit history contains unmapped EntityType values: '
                            || unmapped_entity_types
                            || '. Add an explicit mapping before applying migration 20260711160000_AddStableRetentionEntityIdentity.';
                    END IF;
                END $$;

                UPDATE "sweep_run_entity_summary"
                SET "RetentionEntityId" = CASE "EntityType"
                    WHEN 'Cohort.Sample.Entities.Note' THEN 'a3f467fe-c5d0-4f17-9897-83c373cc1dc8'::uuid
                    WHEN 'Cohort.Sample.Entities.BlobBackedFile' THEN '2fb1804d-9ad8-4543-a177-5d4cd14d62ee'::uuid
                    WHEN 'Cohort.Sample.Entities.PerRowAuditedLog' THEN '42670ee7-c26a-4a2a-a2ab-d9571db7d4f6'::uuid
                    WHEN 'Cohort.Sample.Entities.ExternalNumberedLog' THEN 'd0991164-8823-4f4e-aac1-f9d8d1753764'::uuid
                    WHEN 'Cohort.Sample.Entities.TenantlessLog' THEN '992a65db-d658-4b76-aaf5-b11ca52c4a8f'::uuid
                    WHEN 'Cohort.Sample.Entities.AnonymisedContact' THEN 'fd4a533e-e6a9-44ea-948e-cbf881f35e57'::uuid
                    WHEN 'Cohort.Sample.Entities.TombstoneRecord' THEN '6ebbc096-d3b8-4077-8f21-bf9b4d53c869'::uuid
                    WHEN 'Cohort.Sample.Entities.SoftDeleteRecord' THEN '6107ff39-bf33-413c-889e-6347c909ba15'::uuid
                    WHEN 'Cohort.Sample.Entities.TenantlessSoftDelete' THEN '36d4a1a6-f2d8-40a8-84ea-5a062fc82889'::uuid
                    WHEN 'Cohort.Sample.Entities.NullableAnchorEvent' THEN '314fd4f7-f771-4b94-ab6e-7fc0a09a6ef5'::uuid
                    ELSE NULL
                END;

                UPDATE "sweep_run_row_detail"
                SET "RetentionEntityId" = summary."RetentionEntityId"
                FROM "sweep_run_entity_summary" AS summary
                WHERE summary."SweepId" = "sweep_run_row_detail"."SweepId"
                  AND summary."EntityType" = "sweep_run_row_detail"."EntityType"
                  AND summary."Category" = "sweep_run_row_detail"."Category"
                  AND summary."TenantId" = "sweep_run_row_detail"."TenantId"
                  AND summary."Strategy" = "sweep_run_row_detail"."Strategy";

                DO $$
                DECLARE
                    unmatched_entity_types text;
                BEGIN
                    SELECT string_agg(DISTINCT "EntityType", ', ' ORDER BY "EntityType")
                    INTO unmatched_entity_types
                    FROM "sweep_run_row_detail"
                    WHERE "RetentionEntityId" IS NULL;

                    IF unmatched_entity_types IS NOT NULL THEN
                        RAISE EXCEPTION USING MESSAGE =
                            'Cannot assign stable retention entity identities: sweep_run_row_detail contains rows without matching entity summaries for: '
                            || unmatched_entity_types
                            || '. Repair the audit history before applying migration 20260711160000_AddStableRetentionEntityIdentity.';
                    END IF;
                END $$;
                """
            );

            migrationBuilder.AlterColumn<Guid>(
                name: "RetentionEntityId",
                table: "sweep_run_row_detail",
                type: "uuid",
                nullable: false,
                oldClrType: typeof(Guid),
                oldType: "uuid",
                oldNullable: true
            );

            migrationBuilder.DropPrimaryKey(
                name: "PK_sweep_run_entity_summary",
                table: "sweep_run_entity_summary"
            );

            migrationBuilder.AlterColumn<Guid>(
                name: "RetentionEntityId",
                table: "sweep_run_entity_summary",
                type: "uuid",
                nullable: false,
                oldClrType: typeof(Guid),
                oldType: "uuid",
                oldNullable: true
            );

            migrationBuilder.AddPrimaryKey(
                name: "PK_sweep_run_entity_summary",
                table: "sweep_run_entity_summary",
                columns: new[]
                {
                    "SweepId",
                    "RetentionEntityId",
                    "Category",
                    "TenantId",
                    "Strategy",
                }
            );

            migrationBuilder.CreateIndex(
                name: "IX_sweep_run_row_detail_StableIdentity",
                table: "sweep_run_row_detail",
                columns: new[]
                {
                    "SweepId",
                    "RetentionEntityId",
                    "EntityId",
                    "Category",
                    "Strategy",
                    "TenantId",
                },
                unique: true
            );
        }

        /// <inheritdoc />
        protected override void Down(MigrationBuilder migrationBuilder)
        {
            migrationBuilder.DropIndex(
                name: "IX_retention_holds_RetentionEntityId_RecordId",
                table: "retention_holds"
            );

            migrationBuilder.DropIndex(
                name: "IX_retention_holds_RetentionEntityId_TenantId_RecordId",
                table: "retention_holds"
            );

            migrationBuilder.AddColumn<string>(
                name: "TableName",
                table: "retention_holds",
                type: "text",
                nullable: true
            );

            migrationBuilder.Sql(
                """
                DO $$
                BEGIN
                    IF EXISTS (
                        SELECT 1
                        FROM "retention_holds"
                        WHERE "RetentionEntityId" IN (
                            '992a65db-d658-4b76-aaf5-b11ca52c4a8f'::uuid,
                            '36d4a1a6-f2d8-40a8-84ea-5a062fc82889'::uuid
                        )
                    ) THEN
                        RAISE EXCEPTION 'Cannot downgrade stable retention entity identities while tenantless holds exist because the previous schema requires TenantId. Remove or migrate those holds explicitly before downgrading.';
                    END IF;

                    IF EXISTS (
                        SELECT 1
                        FROM "retention_holds"
                        WHERE "RetentionEntityId" NOT IN (
                            'a3f467fe-c5d0-4f17-9897-83c373cc1dc8'::uuid,
                            '2fb1804d-9ad8-4543-a177-5d4cd14d62ee'::uuid,
                            '42670ee7-c26a-4a2a-a2ab-d9571db7d4f6'::uuid,
                            'd0991164-8823-4f4e-aac1-f9d8d1753764'::uuid,
                            '992a65db-d658-4b76-aaf5-b11ca52c4a8f'::uuid,
                            'fd4a533e-e6a9-44ea-948e-cbf881f35e57'::uuid,
                            '6ebbc096-d3b8-4077-8f21-bf9b4d53c869'::uuid,
                            '6107ff39-bf33-413c-889e-6347c909ba15'::uuid,
                            '36d4a1a6-f2d8-40a8-84ea-5a062fc82889'::uuid,
                            '314fd4f7-f771-4b94-ab6e-7fc0a09a6ef5'::uuid
                        )
                    ) THEN
                        RAISE EXCEPTION 'Cannot downgrade stable retention entity identities because retention_holds contains unknown RetentionEntityId values.';
                    END IF;
                END $$;

                UPDATE "retention_holds"
                SET "TableName" = CASE "RetentionEntityId"
                    WHEN 'a3f467fe-c5d0-4f17-9897-83c373cc1dc8'::uuid THEN 'notes'
                    WHEN '2fb1804d-9ad8-4543-a177-5d4cd14d62ee'::uuid THEN 'blob_backed_files'
                    WHEN '42670ee7-c26a-4a2a-a2ab-d9571db7d4f6'::uuid THEN 'per_row_audited_logs'
                    WHEN 'd0991164-8823-4f4e-aac1-f9d8d1753764'::uuid THEN 'external_numbered_logs'
                    WHEN 'fd4a533e-e6a9-44ea-948e-cbf881f35e57'::uuid THEN 'anonymised_contacts'
                    WHEN '6ebbc096-d3b8-4077-8f21-bf9b4d53c869'::uuid THEN 'tombstone_records'
                    WHEN '6107ff39-bf33-413c-889e-6347c909ba15'::uuid THEN 'soft_delete_records'
                    WHEN '314fd4f7-f771-4b94-ab6e-7fc0a09a6ef5'::uuid THEN 'nullable_anchor_events'
                END;
                """
            );

            migrationBuilder.AlterColumn<string>(
                name: "TableName",
                table: "retention_holds",
                type: "text",
                nullable: false,
                oldClrType: typeof(string),
                oldType: "text",
                oldNullable: true
            );

            migrationBuilder.AlterColumn<Guid>(
                name: "TenantId",
                table: "retention_holds",
                type: "uuid",
                nullable: false,
                oldClrType: typeof(Guid),
                oldType: "uuid",
                oldNullable: true
            );

            migrationBuilder.DropColumn(name: "RetentionEntityId", table: "retention_holds");

            migrationBuilder.CreateIndex(
                name: "IX_retention_holds_TableName_RecordId",
                table: "retention_holds",
                columns: new[] { "TableName", "RecordId" }
            );

            migrationBuilder.CreateIndex(
                name: "IX_retention_holds_TableName_TenantId_RecordId",
                table: "retention_holds",
                columns: new[] { "TableName", "TenantId", "RecordId" }
            );

            migrationBuilder.DropIndex(
                name: "IX_sweep_run_row_detail_StableIdentity",
                table: "sweep_run_row_detail"
            );

            migrationBuilder.DropPrimaryKey(
                name: "PK_sweep_run_entity_summary",
                table: "sweep_run_entity_summary"
            );

            migrationBuilder.AlterColumn<Guid>(
                name: "RetentionEntityId",
                table: "sweep_run_entity_summary",
                type: "uuid",
                nullable: true,
                oldClrType: typeof(Guid),
                oldType: "uuid"
            );

            migrationBuilder.AddPrimaryKey(
                name: "PK_sweep_run_entity_summary",
                table: "sweep_run_entity_summary",
                columns: new[] { "SweepId", "EntityType", "Category", "TenantId", "Strategy" }
            );

            migrationBuilder.AlterColumn<Guid>(
                name: "RetentionEntityId",
                table: "sweep_run_row_detail",
                type: "uuid",
                nullable: true,
                oldClrType: typeof(Guid),
                oldType: "uuid"
            );

            migrationBuilder.DropColumn(name: "RetentionEntityId", table: "sweep_run_row_detail");

            migrationBuilder.DropColumn(
                name: "RetentionEntityId",
                table: "sweep_run_entity_summary"
            );

            migrationBuilder.CreateIndex(
                name: "IX_sweep_run_row_detail_SweepId_EntityType_EntityId_Category_S~",
                table: "sweep_run_row_detail",
                columns: new[]
                {
                    "SweepId",
                    "EntityType",
                    "EntityId",
                    "Category",
                    "Strategy",
                    "TenantId",
                },
                unique: true
            );
        }
    }
}
