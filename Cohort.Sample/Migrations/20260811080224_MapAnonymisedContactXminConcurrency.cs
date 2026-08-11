using Microsoft.EntityFrameworkCore.Migrations;

#nullable disable

namespace Cohort.Sample.Migrations
{
    /// <inheritdoc />
    public partial class MapAnonymisedContactXminConcurrency : Migration
    {
        /// <inheritdoc />
        protected override void Up(MigrationBuilder migrationBuilder)
        {
            migrationBuilder.AddColumn<uint>(
                name: "xmin",
                table: "anonymised_contacts",
                type: "xid",
                rowVersion: true,
                nullable: false,
                defaultValue: 0u);
        }

        /// <inheritdoc />
        protected override void Down(MigrationBuilder migrationBuilder)
        {
            migrationBuilder.DropColumn(
                name: "xmin",
                table: "anonymised_contacts");
        }
    }
}
