using Microsoft.EntityFrameworkCore.Migrations;

#nullable disable

namespace Cohort.Sample.Migrations
{
    /// <inheritdoc />
    public partial class AddSweepRunOccurrenceIndex : Migration
    {
        /// <inheritdoc />
        protected override void Up(MigrationBuilder migrationBuilder)
        {
            migrationBuilder.CreateIndex(
                name: "IX_sweep_run_TriggerKind_StartedAt",
                schema: "public",
                table: "sweep_run",
                columns: new[] { "TriggerKind", "StartedAt" });
        }

        /// <inheritdoc />
        protected override void Down(MigrationBuilder migrationBuilder)
        {
            migrationBuilder.DropIndex(
                name: "IX_sweep_run_TriggerKind_StartedAt",
                schema: "public",
                table: "sweep_run");
        }
    }
}
