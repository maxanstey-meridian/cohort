using System;
using Microsoft.EntityFrameworkCore.Migrations;

#nullable disable

namespace Cohort.Sample.Migrations
{
    /// <inheritdoc />
    public partial class AddRetentionAuditTables : Migration
    {
        /// <inheritdoc />
        protected override void Up(MigrationBuilder migrationBuilder)
        {
            migrationBuilder.CreateTable(
                name: "sweep_run",
                columns: table => new
                {
                    SweepId = table.Column<Guid>(type: "uuid", nullable: false),
                    CompletedAt = table.Column<DateTimeOffset>(type: "timestamp with time zone", nullable: true),
                    DryRun = table.Column<bool>(type: "boolean", nullable: false),
                    Duration = table.Column<TimeSpan>(type: "interval", nullable: true),
                    StartedAt = table.Column<DateTimeOffset>(type: "timestamp with time zone", nullable: false),
                    TenantId = table.Column<Guid>(type: "uuid", nullable: false),
                    TotalAffected = table.Column<int>(type: "integer", nullable: true)
                },
                constraints: table =>
                {
                    table.PrimaryKey("PK_sweep_run", x => x.SweepId);
                });

            migrationBuilder.CreateTable(
                name: "sweep_run_entity_summary",
                columns: table => new
                {
                    SweepId = table.Column<Guid>(type: "uuid", nullable: false),
                    EntityType = table.Column<string>(type: "text", nullable: false),
                    Category = table.Column<string>(type: "text", nullable: false),
                    TenantId = table.Column<Guid>(type: "uuid", nullable: false),
                    Strategy = table.Column<int>(type: "integer", nullable: false),
                    Affected = table.Column<int>(type: "integer", nullable: false),
                    At = table.Column<DateTimeOffset>(type: "timestamp with time zone", nullable: false),
                    HeldCount = table.Column<int>(type: "integer", nullable: false),
                    ResolvedPeriod = table.Column<TimeSpan>(type: "interval", nullable: false)
                },
                constraints: table =>
                {
                    table.PrimaryKey("PK_sweep_run_entity_summary", x => new { x.SweepId, x.EntityType, x.Category, x.TenantId, x.Strategy });
                });

            migrationBuilder.CreateTable(
                name: "sweep_run_row_detail",
                columns: table => new
                {
                    SweepId = table.Column<Guid>(type: "uuid", nullable: false),
                    EntityType = table.Column<string>(type: "text", nullable: false),
                    EntityId = table.Column<Guid>(type: "uuid", nullable: false),
                    Category = table.Column<string>(type: "text", nullable: false),
                    Strategy = table.Column<int>(type: "integer", nullable: false),
                    TenantId = table.Column<Guid>(type: "uuid", nullable: false),
                    At = table.Column<DateTimeOffset>(type: "timestamp with time zone", nullable: false)
                },
                constraints: table =>
                {
                    table.PrimaryKey("PK_sweep_run_row_detail", x => new { x.SweepId, x.EntityType, x.EntityId, x.Category, x.Strategy, x.TenantId });
                });

            migrationBuilder.CreateIndex(
                name: "IX_sweep_run_entity_summary_SweepId",
                table: "sweep_run_entity_summary",
                column: "SweepId");

            migrationBuilder.CreateIndex(
                name: "IX_sweep_run_row_detail_SweepId",
                table: "sweep_run_row_detail",
                column: "SweepId");
        }

        /// <inheritdoc />
        protected override void Down(MigrationBuilder migrationBuilder)
        {
            migrationBuilder.DropTable(
                name: "sweep_run");

            migrationBuilder.DropTable(
                name: "sweep_run_entity_summary");

            migrationBuilder.DropTable(
                name: "sweep_run_row_detail");
        }
    }
}
