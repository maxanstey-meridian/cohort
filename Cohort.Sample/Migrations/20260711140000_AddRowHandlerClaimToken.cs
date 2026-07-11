using Cohort.Sample;
using Microsoft.EntityFrameworkCore.Infrastructure;
using Microsoft.EntityFrameworkCore.Migrations;

#nullable disable

namespace Cohort.Sample.Migrations;

[DbContext(typeof(SampleDbContext))]
[Migration("20260711140000_AddRowHandlerClaimToken")]
public sealed class AddRowHandlerClaimToken : Migration
{
    protected override void Up(MigrationBuilder migrationBuilder)
    {
        migrationBuilder.AddColumn<Guid>(
            name: "ClaimToken",
            table: "sweep_row_handler_status",
            type: "uuid",
            nullable: true
        );
    }

    protected override void Down(MigrationBuilder migrationBuilder)
    {
        migrationBuilder.DropColumn(name: "ClaimToken", table: "sweep_row_handler_status");
    }
}
