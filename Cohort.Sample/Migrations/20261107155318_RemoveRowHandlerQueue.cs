using System;
using Microsoft.EntityFrameworkCore.Migrations;
using Npgsql.EntityFrameworkCore.PostgreSQL.Metadata;

#nullable disable

namespace Cohort.Sample.Migrations
{
    /// <inheritdoc />
    public partial class RemoveRowHandlerQueue : Migration
    {
        /// <inheritdoc />
        protected override void Up(MigrationBuilder migrationBuilder)
        {
            migrationBuilder.AddColumn<string>(
                name: "Failure",
                schema: "public",
                table: "sweep_run_row_detail",
                type: "text",
                nullable: true);

            // A row skipped by a failing before-mutation step was recorded as a dead-lettered
            // status queued, and completed, at one database-clock instant, never claimed; its
            // error moves to the row detail. An after-commit dead-letter belongs to a mutated
            // row, so its row detail keeps Failure NULL.
            migrationBuilder.Sql(
                """
                UPDATE "public"."sweep_run_row_detail" AS detail
                SET "Failure" = status."LastError"
                FROM "public"."sweep_row_handler_status" AS status
                WHERE status."SweepRunRowDetailId" = detail."Id"
                  AND status."State" = 3
                  AND status."Attempt" = 1
                  AND status."ClaimedAt" IS NULL
                  AND status."CompletedAt" = status."QueuedAt";
                """);

            migrationBuilder.DropTable(
                name: "sweep_row_handler_status",
                schema: "public");

            migrationBuilder.DropColumn(
                name: "CapturedPayload",
                schema: "public",
                table: "sweep_run_row_detail");
        }

        /// <inheritdoc />
        protected override void Down(MigrationBuilder migrationBuilder)
        {
            migrationBuilder.AddColumn<string>(
                name: "CapturedPayload",
                schema: "public",
                table: "sweep_run_row_detail",
                type: "text",
                nullable: true);

            migrationBuilder.CreateTable(
                name: "sweep_row_handler_status",
                schema: "public",
                columns: table => new
                {
                    Id = table.Column<long>(type: "bigint", nullable: false)
                        .Annotation("Npgsql:ValueGenerationStrategy", NpgsqlValueGenerationStrategy.IdentityByDefaultColumn),
                    Attempt = table.Column<int>(type: "integer", nullable: false),
                    ClaimToken = table.Column<Guid>(type: "uuid", nullable: true),
                    ClaimedAt = table.Column<DateTimeOffset>(type: "timestamp with time zone", nullable: true),
                    CompletedAt = table.Column<DateTimeOffset>(type: "timestamp with time zone", nullable: true),
                    DispatchPhase = table.Column<int>(type: "integer", nullable: false),
                    HandlerType = table.Column<string>(type: "text", nullable: false),
                    LastError = table.Column<string>(type: "text", nullable: true),
                    NextAttemptAt = table.Column<DateTimeOffset>(type: "timestamp with time zone", nullable: false),
                    QueuedAt = table.Column<DateTimeOffset>(type: "timestamp with time zone", nullable: false),
                    State = table.Column<int>(type: "integer", nullable: false),
                    SweepRunRowDetailId = table.Column<long>(type: "bigint", nullable: false)
                },
                constraints: table =>
                {
                    table.PrimaryKey("PK_sweep_row_handler_status", x => x.Id);
                    table.CheckConstraint("CK_sweep_row_handler_status_Claim", "(\"State\" = 1 AND \"ClaimedAt\" IS NOT NULL AND \"ClaimToken\" IS NOT NULL) OR (\"State\" <> 1 AND \"ClaimedAt\" IS NULL AND \"ClaimToken\" IS NULL)");
                    table.CheckConstraint("CK_sweep_row_handler_status_Completion", "(\"State\" IN (2, 3) AND \"CompletedAt\" IS NOT NULL) OR (\"State\" IN (0, 1) AND \"CompletedAt\" IS NULL)");
                    table.ForeignKey(
                        name: "FK_sweep_row_handler_status_sweep_run_row_detail_SweepRunRowDe~",
                        column: x => x.SweepRunRowDetailId,
                        principalSchema: "public",
                        principalTable: "sweep_run_row_detail",
                        principalColumn: "Id",
                        onDelete: ReferentialAction.Cascade);
                });

            migrationBuilder.CreateIndex(
                name: "IX_sweep_row_handler_status_State_NextAttemptAt_Id",
                schema: "public",
                table: "sweep_row_handler_status",
                columns: new[] { "State", "NextAttemptAt", "Id" });

            migrationBuilder.CreateIndex(
                name: "IX_sweep_row_handler_status_SweepRunRowDetailId_HandlerType",
                schema: "public",
                table: "sweep_row_handler_status",
                columns: new[] { "SweepRunRowDetailId", "HandlerType" },
                unique: true);

            migrationBuilder.DropColumn(
                name: "Failure",
                schema: "public",
                table: "sweep_run_row_detail");
        }
    }
}
