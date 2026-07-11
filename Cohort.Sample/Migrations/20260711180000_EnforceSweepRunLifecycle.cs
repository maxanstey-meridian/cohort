using Cohort.Sample;
using Microsoft.EntityFrameworkCore.Infrastructure;
using Microsoft.EntityFrameworkCore.Migrations;

#nullable disable

namespace Cohort.Sample.Migrations;

[DbContext(typeof(SampleDbContext))]
[Migration("20260711180000_EnforceSweepRunLifecycle")]
public sealed class EnforceSweepRunLifecycle : Migration
{
    protected override void Up(MigrationBuilder migrationBuilder)
    {
        migrationBuilder.AddCheckConstraint(
            name: "CK_sweep_run_Status_Range",
            table: "sweep_run",
            sql: "\"Status\" BETWEEN 0 AND 4"
        );
        migrationBuilder.AddCheckConstraint(
            name: "CK_sweep_run_Started_Unsettled",
            table: "sweep_run",
            sql: "\"Status\" <> 0 OR \"SettledAt\" IS NULL"
        );
        migrationBuilder.AddCheckConstraint(
            name: "CK_sweep_run_Terminal_Settled",
            table: "sweep_run",
            sql: "\"Status\" = 0 OR \"SettledAt\" IS NOT NULL"
        );
        migrationBuilder.AddCheckConstraint(
            name: "CK_sweep_run_TotalAffected_Nonnegative",
            table: "sweep_run",
            sql: "\"TotalAffected\" IS NULL OR \"TotalAffected\" >= 0"
        );
        migrationBuilder.AddCheckConstraint(
            name: "CK_sweep_run_Duration_Nonnegative",
            table: "sweep_run",
            sql: "\"Duration\" IS NULL OR \"Duration\" >= INTERVAL '0'"
        );
    }

    protected override void Down(MigrationBuilder migrationBuilder)
    {
        migrationBuilder.DropCheckConstraint(
            name: "CK_sweep_run_Status_Range",
            table: "sweep_run"
        );
        migrationBuilder.DropCheckConstraint(
            name: "CK_sweep_run_Started_Unsettled",
            table: "sweep_run"
        );
        migrationBuilder.DropCheckConstraint(
            name: "CK_sweep_run_Terminal_Settled",
            table: "sweep_run"
        );
        migrationBuilder.DropCheckConstraint(
            name: "CK_sweep_run_TotalAffected_Nonnegative",
            table: "sweep_run"
        );
        migrationBuilder.DropCheckConstraint(
            name: "CK_sweep_run_Duration_Nonnegative",
            table: "sweep_run"
        );
    }
}
