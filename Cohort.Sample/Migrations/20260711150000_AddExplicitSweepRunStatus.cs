using Cohort.Sample;
using Microsoft.EntityFrameworkCore.Infrastructure;
using Microsoft.EntityFrameworkCore.Migrations;

#nullable disable

namespace Cohort.Sample.Migrations;

[DbContext(typeof(SampleDbContext))]
[Migration("20260711150000_AddExplicitSweepRunStatus")]
public sealed class AddExplicitSweepRunStatus : Migration
{
    protected override void Up(MigrationBuilder migrationBuilder)
    {
        migrationBuilder.AddColumn<int>(
            name: "Status",
            table: "sweep_run",
            type: "integer",
            nullable: false,
            defaultValue: 0
        );
        migrationBuilder.AddColumn<DateTimeOffset>(
            name: "SettledAt",
            table: "sweep_run",
            type: "timestamp with time zone",
            nullable: true
        );
        migrationBuilder.Sql(
            """
            UPDATE "sweep_run"
            SET "Status" = CASE
                    WHEN "CompletedAt" IS NOT NULL AND "FailedAt" IS NOT NULL THEN 2
                    WHEN "FailedAt" IS NOT NULL THEN 3
                    WHEN "CompletedAt" IS NOT NULL THEN 1
                    ELSE 0
                END,
                "SettledAt" = COALESCE("FailedAt", "CompletedAt")
            """
        );
        migrationBuilder.DropColumn(name: "CompletedAt", table: "sweep_run");
        migrationBuilder.DropColumn(name: "FailedAt", table: "sweep_run");
    }

    protected override void Down(MigrationBuilder migrationBuilder)
    {
        migrationBuilder.AddColumn<DateTimeOffset>(
            name: "CompletedAt",
            table: "sweep_run",
            type: "timestamp with time zone",
            nullable: true
        );
        migrationBuilder.AddColumn<DateTimeOffset>(
            name: "FailedAt",
            table: "sweep_run",
            type: "timestamp with time zone",
            nullable: true
        );
        migrationBuilder.Sql(
            """
            UPDATE "sweep_run"
            SET "CompletedAt" = CASE WHEN "Status" IN (1, 2) THEN "SettledAt" END,
                "FailedAt" = CASE WHEN "Status" IN (2, 3, 4) THEN "SettledAt" END
            """
        );
        migrationBuilder.DropColumn(name: "Status", table: "sweep_run");
        migrationBuilder.DropColumn(name: "SettledAt", table: "sweep_run");
    }
}
