using System;
using Microsoft.EntityFrameworkCore.Infrastructure;
using Microsoft.EntityFrameworkCore.Migrations;

#nullable disable

namespace Cohort.Sample.Migrations
{
    [DbContext(typeof(SampleDbContext))]
    [Migration("20260711120000_AddExternalNumberedLog")]
    public partial class AddExternalNumberedLog : Migration
    {
        protected override void Up(MigrationBuilder migrationBuilder)
        {
            migrationBuilder.CreateTable(
                name: "external_numbered_logs",
                columns: table => new
                {
                    Id = table.Column<Guid>(type: "uuid", nullable: false),
                    ExternalId = table.Column<int>(type: "integer", nullable: false),
                    CreatedAt = table.Column<DateTimeOffset>(
                        type: "timestamp with time zone",
                        nullable: false
                    ),
                    Payload = table.Column<string>(type: "text", nullable: false),
                },
                constraints: table =>
                {
                    table.PrimaryKey("PK_external_numbered_logs", x => x.Id);
                }
            );

            migrationBuilder.CreateIndex(
                name: "IX_external_numbered_logs_ExternalId",
                table: "external_numbered_logs",
                column: "ExternalId",
                unique: true
            );
        }

        protected override void Down(MigrationBuilder migrationBuilder)
        {
            migrationBuilder.DropTable(name: "external_numbered_logs");
        }
    }
}
