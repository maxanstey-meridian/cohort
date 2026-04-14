# Changelog

## 0.3.0

Current release.

### Release notes

- Startup tenant enforcement is now fail-closed. Retained entities must resolve tenant metadata by convention or `[RetentionTenant]`, unless they are intentionally global and explicitly marked with `[RetentionTenantless]`.
- Erasure now respects the effective retention cutoff. A subject-matched row is only mutated when it is already past `max(Period, LegalMin)`, and active holds still block mutation.
- Erasure subject metadata now supports multiple `[ErasureSubject]` properties on the same entity. The erasure contract now passes an `ErasureSubjectPredicate`, and hosts should expect erasure matching to treat any marked subject column as eligible input.
- `RetentionRowDispatcher` remains part of the runtime surface even when a host registers zero row handlers. A correctly migrated host should boot and idle cleanly with that dispatcher registration in place.

### Upgrade notes

- Existing hosts must regenerate and apply the Cohort migration before booting `0.3.0` unless they intentionally enable `Cohort:ApplyMigrations=true` during startup.
- `ApplyMigrations` still defaults to `false`, so upgrading the package alone does not move an existing database to the current Cohort table shape.

### Release verification gates

Run these before calling the package release-ready:

1. `dotnet pack Cohort/Cohort.csproj`
2. Restore the packed version into a clean consumer.
3. Regenerate your host migration against the `0.3.0` package and apply that migration before booting the new package version unless startup migrations are explicitly enabled.
4. Verify the restored consumer can resolve the current public/runtime surface:
   `AnonymiseWithAttribute`, `ErasureSubjectPredicate`, `IRetentionSweepStrategy.PreviewEraseAsync(...)`, and the row-dispatch surface (`IRetentionRowDispatcher` plus dispatcher-backed runtime registration).
