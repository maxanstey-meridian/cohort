using Microsoft.EntityFrameworkCore.Infrastructure;
using Microsoft.EntityFrameworkCore.Migrations;

#nullable disable

namespace Cohort.Sample.Migrations;

[DbContext(typeof(SampleDbContext))]
[Migration("20260711130000_RequireRetainedTenantIds")]
public partial class RequireRetainedTenantIds : Migration
{
    protected override void Up(MigrationBuilder migrationBuilder)
    {
        migrationBuilder.Sql(
            """
            DO $$
            BEGIN
                IF EXISTS (SELECT 1 FROM notes WHERE "TenantId" IS NULL)
                   OR EXISTS (SELECT 1 FROM nullable_anchor_events WHERE "TenantId" IS NULL) THEN
                    RAISE EXCEPTION 'Cannot require retained tenant IDs: notes and/or nullable_anchor_events contain NULL TenantId values. Backfill TenantId before applying migration 20260711130000_RequireRetainedTenantIds.';
                END IF;
            END $$;
            """
        );

        migrationBuilder.AlterColumn<Guid>(
            name: "TenantId",
            table: "notes",
            type: "uuid",
            nullable: false,
            oldClrType: typeof(Guid),
            oldType: "uuid",
            oldNullable: true
        );

        migrationBuilder.AlterColumn<Guid>(
            name: "TenantId",
            table: "nullable_anchor_events",
            type: "uuid",
            nullable: false,
            oldClrType: typeof(Guid),
            oldType: "uuid",
            oldNullable: true
        );
    }

    protected override void Down(MigrationBuilder migrationBuilder)
    {
        migrationBuilder.AlterColumn<Guid>(
            name: "TenantId",
            table: "notes",
            type: "uuid",
            nullable: true,
            oldClrType: typeof(Guid),
            oldType: "uuid"
        );

        migrationBuilder.AlterColumn<Guid>(
            name: "TenantId",
            table: "nullable_anchor_events",
            type: "uuid",
            nullable: true,
            oldClrType: typeof(Guid),
            oldType: "uuid"
        );
    }
}
