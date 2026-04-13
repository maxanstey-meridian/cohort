using Microsoft.EntityFrameworkCore.Infrastructure;
using Microsoft.EntityFrameworkCore.Migrations;

#nullable disable

namespace Cohort.Sample.Migrations;

[DbContext(typeof(SampleDbContext))]
[Migration("20260413190000_AddSweepSkippedCount")]
public partial class AddSweepSkippedCount : Migration
{
    protected override void Up(MigrationBuilder migrationBuilder)
    {
        migrationBuilder.AddColumn<int>(
            name: "SkippedCount",
            table: "sweep_run_entity_summary",
            type: "integer",
            nullable: false,
            defaultValue: 0
        );
    }

    protected override void Down(MigrationBuilder migrationBuilder)
    {
        migrationBuilder.DropColumn(
            name: "SkippedCount",
            table: "sweep_run_entity_summary"
        );
    }
}
