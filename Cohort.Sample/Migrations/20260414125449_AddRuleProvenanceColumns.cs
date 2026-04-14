using Microsoft.EntityFrameworkCore.Migrations;

#nullable disable

namespace Cohort.Sample.Migrations
{
    /// <inheritdoc />
    public partial class AddRuleProvenanceColumns : Migration
    {
        /// <inheritdoc />
        protected override void Up(MigrationBuilder migrationBuilder)
        {
            migrationBuilder.AddColumn<string>(
                name: "RuleReason",
                table: "sweep_run_entity_summary",
                type: "text",
                nullable: true);

            migrationBuilder.AddColumn<string>(
                name: "RuleSource",
                table: "sweep_run_entity_summary",
                type: "text",
                nullable: true);
        }

        /// <inheritdoc />
        protected override void Down(MigrationBuilder migrationBuilder)
        {
            migrationBuilder.DropColumn(
                name: "RuleReason",
                table: "sweep_run_entity_summary");

            migrationBuilder.DropColumn(
                name: "RuleSource",
                table: "sweep_run_entity_summary");
        }
    }
}
