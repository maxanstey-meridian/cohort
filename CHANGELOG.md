# Changelog

## 0.2.0

First truthful post-`0.1.1` release. This version aligns the published package with the runtime surface already present in source.

### Release notes

- Startup tenant enforcement is now fail-closed. Retained entities must resolve tenant metadata by convention or `[RetentionTenant]`, unless they are intentionally global and explicitly marked with `[RetentionTenantless]`.
- Erasure now respects the effective retention cutoff. A subject-matched row is only mutated when it is already past `max(Period, LegalMin)`, and active holds still block mutation.
- Hosts upgrading from `0.1.1` must refresh their Cohort-owned table migrations before booting this package version.
- `RetentionRowDispatcher` remains part of the runtime surface even when a host registers zero row handlers. A correctly migrated host should boot and idle cleanly with that dispatcher registration in place.

### Upgrade notes for `0.1.1` consumers

1. Regenerate your host migration against the `0.2.0` package so the Cohort-owned tables match the current `ConfigureCohortTables()` model.
2. Apply that migration before booting the new package version in any environment.
3. Review retained entities for tenant metadata. Add `[RetentionTenant]` when convention is wrong, or `[RetentionTenantless]` when the entity is intentionally global.
4. Re-run any right-to-erasure flows that assumed subject match alone was enough; `0.2.0` now requires both subject match and retention-cutoff eligibility.

### Release verification gates

Run these before calling the package release-ready:

1. `dotnet pack Cohort/Cohort.csproj`
2. Restore the packed version into a clean consumer.
3. Verify the restored consumer can resolve the post-`0.1.1` public/runtime surface:
   `AnonymiseWithAttribute`, `IRetentionSweepStrategy.PreviewEraseAsync(...)`, and the row-dispatch surface (`IRetentionRowDispatcher` plus dispatcher-backed runtime registration).
