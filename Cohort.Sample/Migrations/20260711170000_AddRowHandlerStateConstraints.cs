using Cohort.Sample;
using Microsoft.EntityFrameworkCore.Infrastructure;
using Microsoft.EntityFrameworkCore.Migrations;

#nullable disable

namespace Cohort.Sample.Migrations;

[DbContext(typeof(SampleDbContext))]
[Migration("20260711170000_AddRowHandlerStateConstraints")]
public sealed class AddRowHandlerStateConstraints : Migration
{
    protected override void Up(MigrationBuilder migrationBuilder)
    {
        migrationBuilder.Sql(
            """
            UPDATE "sweep_row_handler_status"
            SET "CompletedAt" = COALESCE("ClaimedAt", "NextAttemptAt", "QueuedAt")
            WHERE "State" IN (2, 3)
              AND "CompletedAt" IS NULL;

            UPDATE "sweep_row_handler_status"
            SET "CompletedAt" = NULL
            WHERE "State" IN (0, 1)
              AND "CompletedAt" IS NOT NULL;

            UPDATE "sweep_row_handler_status"
            SET "State" = 0,
                "ClaimedAt" = NULL,
                "ClaimToken" = NULL,
                "NextAttemptAt" = LEAST("NextAttemptAt", CURRENT_TIMESTAMP)
            WHERE "State" = 1
              AND "ClaimToken" IS NULL;

            UPDATE "sweep_row_handler_status"
            SET "ClaimedAt" = NULL,
                "ClaimToken" = NULL
            WHERE "State" <> 1
              AND ("ClaimedAt" IS NOT NULL OR "ClaimToken" IS NOT NULL);
            """
        );

        migrationBuilder.AddCheckConstraint(
            name: "CK_sweep_row_handler_status_Claim",
            table: "sweep_row_handler_status",
            sql: "(\"State\" = 1 AND \"ClaimedAt\" IS NOT NULL AND \"ClaimToken\" IS NOT NULL) OR (\"State\" <> 1 AND \"ClaimedAt\" IS NULL AND \"ClaimToken\" IS NULL)"
        );
        migrationBuilder.AddCheckConstraint(
            name: "CK_sweep_row_handler_status_Completion",
            table: "sweep_row_handler_status",
            sql: "(\"State\" IN (2, 3) AND \"CompletedAt\" IS NOT NULL) OR (\"State\" IN (0, 1) AND \"CompletedAt\" IS NULL)"
        );
    }

    protected override void Down(MigrationBuilder migrationBuilder)
    {
        migrationBuilder.DropCheckConstraint(
            name: "CK_sweep_row_handler_status_Claim",
            table: "sweep_row_handler_status"
        );
        migrationBuilder.DropCheckConstraint(
            name: "CK_sweep_row_handler_status_Completion",
            table: "sweep_row_handler_status"
        );
    }
}
