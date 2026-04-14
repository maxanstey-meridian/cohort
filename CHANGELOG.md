# Changelog

## 0.2.0

Current release.

### Release notes

- Startup tenant enforcement is now fail-closed. Retained entities must resolve tenant metadata by convention or `[RetentionTenant]`, unless they are intentionally global and explicitly marked with `[RetentionTenantless]`.
- Erasure now respects the effective retention cutoff. A subject-matched row is only mutated when it is already past `max(Period, LegalMin)`, and active holds still block mutation.
- `RetentionRowDispatcher` remains part of the runtime surface even when a host registers zero row handlers. A correctly migrated host should boot and idle cleanly with that dispatcher registration in place.

### Release verification gates

Run these before calling the package release-ready:

1. `dotnet pack Cohort/Cohort.csproj`
2. Restore the packed version into a clean consumer.
3. Verify the restored consumer can resolve the current public/runtime surface:
   `AnonymiseWithAttribute`, `IRetentionSweepStrategy.PreviewEraseAsync(...)`, and the row-dispatch surface (`IRetentionRowDispatcher` plus dispatcher-backed runtime registration).
