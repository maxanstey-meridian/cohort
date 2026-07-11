using Microsoft.EntityFrameworkCore.Migrations;

#nullable disable

namespace Cohort.Sample.Migrations
{
    /// <inheritdoc />
    public partial class AddAuditRunForeignKeys : Migration
    {
        /// <inheritdoc />
        protected override void Up(MigrationBuilder migrationBuilder)
        {
            migrationBuilder.AddForeignKey(
                name: "FK_sweep_run_entity_summary_sweep_run_SweepId",
                table: "sweep_run_entity_summary",
                column: "SweepId",
                principalTable: "sweep_run",
                principalColumn: "SweepId",
                onDelete: ReferentialAction.Restrict);

            migrationBuilder.AddForeignKey(
                name: "FK_sweep_run_row_detail_sweep_run_SweepId",
                table: "sweep_run_row_detail",
                column: "SweepId",
                principalTable: "sweep_run",
                principalColumn: "SweepId",
                onDelete: ReferentialAction.Restrict);
        }

        /// <inheritdoc />
        protected override void Down(MigrationBuilder migrationBuilder)
        {
            migrationBuilder.DropForeignKey(
                name: "FK_sweep_run_entity_summary_sweep_run_SweepId",
                table: "sweep_run_entity_summary");

            migrationBuilder.DropForeignKey(
                name: "FK_sweep_run_row_detail_sweep_run_SweepId",
                table: "sweep_run_row_detail");
        }
    }
}
