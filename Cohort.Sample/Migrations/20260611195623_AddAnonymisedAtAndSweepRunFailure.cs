using System;
using Microsoft.EntityFrameworkCore.Migrations;

#nullable disable

namespace Cohort.Sample.Migrations
{
    /// <inheritdoc />
    public partial class AddAnonymisedAtAndSweepRunFailure : Migration
    {
        /// <inheritdoc />
        protected override void Up(MigrationBuilder migrationBuilder)
        {
            migrationBuilder.AddColumn<DateTimeOffset>(
                name: "AnonymisedAt",
                table: "tombstone_records",
                type: "timestamp with time zone",
                nullable: true);

            migrationBuilder.AddColumn<string>(
                name: "Error",
                table: "sweep_run",
                type: "text",
                nullable: true);

            migrationBuilder.AddColumn<DateTimeOffset>(
                name: "FailedAt",
                table: "sweep_run",
                type: "timestamp with time zone",
                nullable: true);

            migrationBuilder.AddColumn<DateTimeOffset>(
                name: "AnonymisedAt",
                table: "anonymised_contacts",
                type: "timestamp with time zone",
                nullable: true);
        }

        /// <inheritdoc />
        protected override void Down(MigrationBuilder migrationBuilder)
        {
            migrationBuilder.DropColumn(
                name: "AnonymisedAt",
                table: "tombstone_records");

            migrationBuilder.DropColumn(
                name: "Error",
                table: "sweep_run");

            migrationBuilder.DropColumn(
                name: "FailedAt",
                table: "sweep_run");

            migrationBuilder.DropColumn(
                name: "AnonymisedAt",
                table: "anonymised_contacts");
        }
    }
}
