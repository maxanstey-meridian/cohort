using Microsoft.EntityFrameworkCore.Migrations;

#nullable disable

namespace Cohort.Sample.Migrations
{
    /// <inheritdoc />
    public partial class AddRetainedDeferredAndBlockedCounts : Migration
    {
        /// <inheritdoc />
        protected override void Up(MigrationBuilder migrationBuilder)
        {
            migrationBuilder.AddColumn<long>(
                name: "BlockedCount",
                schema: "public",
                table: "sweep_run_entity_summary",
                type: "bigint",
                nullable: false,
                defaultValue: 0L);

            migrationBuilder.AddColumn<long>(
                name: "DeferredCount",
                schema: "public",
                table: "sweep_run_entity_summary",
                type: "bigint",
                nullable: false,
                defaultValue: 0L);

            migrationBuilder.AddColumn<long>(
                name: "RetainedCount",
                schema: "public",
                table: "sweep_run_entity_summary",
                type: "bigint",
                nullable: false,
                defaultValue: 0L);
        }

        /// <inheritdoc />
        protected override void Down(MigrationBuilder migrationBuilder)
        {
            migrationBuilder.DropColumn(
                name: "BlockedCount",
                schema: "public",
                table: "sweep_run_entity_summary");

            migrationBuilder.DropColumn(
                name: "DeferredCount",
                schema: "public",
                table: "sweep_run_entity_summary");

            migrationBuilder.DropColumn(
                name: "RetainedCount",
                schema: "public",
                table: "sweep_run_entity_summary");
        }
    }
}
