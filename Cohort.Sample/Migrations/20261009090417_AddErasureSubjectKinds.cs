using System;
using Microsoft.EntityFrameworkCore.Migrations;

#nullable disable

namespace Cohort.Sample.Migrations
{
    /// <inheritdoc />
    public partial class AddErasureSubjectKinds : Migration
    {
        /// <inheritdoc />
        protected override void Up(MigrationBuilder migrationBuilder)
        {
            migrationBuilder.AddColumn<string>(
                name: "ErasureSubjectKind",
                schema: "public",
                table: "sweep_run",
                type: "text",
                nullable: true);

            migrationBuilder.AddColumn<Guid>(
                name: "PersonId",
                table: "notes",
                type: "uuid",
                nullable: true);

            migrationBuilder.AddCheckConstraint(
                name: "CK_sweep_run_ErasureSubjectKind_Erasure_Only",
                schema: "public",
                table: "sweep_run",
                sql: "\"TriggerKind\" = 1 OR \"ErasureSubjectKind\" IS NULL");
        }

        /// <inheritdoc />
        protected override void Down(MigrationBuilder migrationBuilder)
        {
            migrationBuilder.DropCheckConstraint(
                name: "CK_sweep_run_ErasureSubjectKind_Erasure_Only",
                schema: "public",
                table: "sweep_run");

            migrationBuilder.DropColumn(
                name: "ErasureSubjectKind",
                schema: "public",
                table: "sweep_run");

            migrationBuilder.DropColumn(
                name: "PersonId",
                table: "notes");
        }
    }
}
