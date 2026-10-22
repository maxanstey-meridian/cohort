using System;
using Microsoft.EntityFrameworkCore.Migrations;

#nullable disable

namespace Cohort.Sample.Migrations
{
    /// <inheritdoc />
    public partial class StoreResolvedPeriodAsMonthsAndDays : Migration
    {
        /// <inheritdoc />
        protected override void Up(MigrationBuilder migrationBuilder)
        {
            migrationBuilder.AddColumn<int>(
                name: "ResolvedMonths",
                schema: "public",
                table: "sweep_run_entity_summary",
                type: "integer",
                nullable: false,
                defaultValue: 0);

            migrationBuilder.AddColumn<int>(
                name: "ResolvedDays",
                schema: "public",
                table: "sweep_run_entity_summary",
                type: "integer",
                nullable: false,
                defaultValue: 0);

            // Keep the period each existing summary recorded. Before 0.10.0 it was a TimeSpan,
            // so whole days carry over and any part of a day is dropped.
            migrationBuilder.Sql(
                """
                UPDATE "public"."sweep_run_entity_summary"
                SET "ResolvedMonths" = EXTRACT(YEAR FROM justify_hours("ResolvedPeriod"))::integer * 12
                                     + EXTRACT(MONTH FROM justify_hours("ResolvedPeriod"))::integer,
                    "ResolvedDays" = EXTRACT(DAY FROM justify_hours("ResolvedPeriod"))::integer;
                """);

            migrationBuilder.DropColumn(
                name: "ResolvedPeriod",
                schema: "public",
                table: "sweep_run_entity_summary");
        }

        /// <inheritdoc />
        protected override void Down(MigrationBuilder migrationBuilder)
        {
            migrationBuilder.AddColumn<TimeSpan>(
                name: "ResolvedPeriod",
                schema: "public",
                table: "sweep_run_entity_summary",
                type: "interval",
                nullable: false,
                defaultValue: new TimeSpan(0, 0, 0, 0, 0));

            migrationBuilder.Sql(
                """
                UPDATE "public"."sweep_run_entity_summary"
                SET "ResolvedPeriod" = make_interval(months => "ResolvedMonths", days => "ResolvedDays");
                """);

            migrationBuilder.DropColumn(
                name: "ResolvedMonths",
                schema: "public",
                table: "sweep_run_entity_summary");

            migrationBuilder.DropColumn(
                name: "ResolvedDays",
                schema: "public",
                table: "sweep_run_entity_summary");
        }
    }
}
