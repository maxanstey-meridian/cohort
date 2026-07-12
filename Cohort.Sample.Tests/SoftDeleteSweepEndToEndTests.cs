using System.Data;
using Cohort.Application;
using Cohort.Domain;
using Cohort.Infrastructure;
using Cohort.Infrastructure.Sweep;
using Cohort.Sample.Entities;
using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;

namespace Cohort.Sample.Tests;

public sealed class SoftDeleteSweepEndToEndTests(PostgresFixture fixture)
    : IntegrationTestBase(fixture)
{
    [Fact]
    public async Task Sweep_Path_Soft_Deletes_Only_Expired_Rows_For_The_Target_Tenant_And_Stamps_DeletedAt()
    {
        var tenantA = Guid.NewGuid();
        var tenantB = Guid.NewGuid();
        var asOf = new DateTimeOffset(2026, 4, 12, 12, 0, 0, TimeSpan.Zero);
        var originalDeletedAt = asOf.AddDays(-10);

        await using (var db = Host.CreateDbContext())
        {
            db.SoftDeleteRecords.AddRange(
                new SoftDeleteRecord
                {
                    Id = Guid.NewGuid(),
                    TenantId = tenantA,
                    CreatedAt = asOf.AddDays(-120),
                    Body = "soft-delete-expired-target",
                    IsDeleted = false,
                },
                new SoftDeleteRecord
                {
                    Id = Guid.NewGuid(),
                    TenantId = tenantA,
                    CreatedAt = asOf.AddDays(-5),
                    Body = "soft-delete-keep-newer",
                    IsDeleted = false,
                },
                new SoftDeleteRecord
                {
                    Id = Guid.NewGuid(),
                    TenantId = tenantB,
                    CreatedAt = asOf.AddDays(-120),
                    Body = "soft-delete-keep-other-tenant",
                    IsDeleted = false,
                },
                new SoftDeleteRecord
                {
                    Id = Guid.NewGuid(),
                    TenantId = tenantA,
                    CreatedAt = asOf.AddDays(-120),
                    Body = "soft-delete-keep-existing-deleted",
                    IsDeleted = true,
                    DeletedAt = originalDeletedAt,
                }
            );
            await db.SaveChangesAsync();
        }

        var result = await Host.RunSweepAsync(
            new TenantContext(tenantA, "uk", new Dictionary<string, string>()),
            asOf
        );

        result
            .Counts.Should()
            .Contain(
                new EntitySweepCount(
                    typeof(SoftDeleteRecord),
                    "soft-delete",
                    tenantA,
                    Strategy.SoftDelete,
                    1
                )
            );

        await using var verify = Host.CreateDbContext();
        var records = await verify
            .SoftDeleteRecords.OrderBy(record => record.Body)
            .Select(record => new
            {
                record.Body,
                record.IsDeleted,
                record.DeletedAt,
            })
            .ToListAsync();

        records
            .Should()
            .Equal(
                new
                {
                    Body = "soft-delete-expired-target",
                    IsDeleted = true,
                    DeletedAt = (DateTimeOffset?)asOf,
                },
                new
                {
                    Body = "soft-delete-keep-existing-deleted",
                    IsDeleted = true,
                    DeletedAt = (DateTimeOffset?)originalDeletedAt,
                },
                new
                {
                    Body = "soft-delete-keep-newer",
                    IsDeleted = false,
                    DeletedAt = (DateTimeOffset?)null,
                },
                new
                {
                    Body = "soft-delete-keep-other-tenant",
                    IsDeleted = false,
                    DeletedAt = (DateTimeOffset?)null,
                }
            );
    }

    [Fact]
    public async Task Validation_Fails_When_A_SoftDelete_Category_Lacks_The_Required_Convention()
    {
        await using var db = Host.CreateDbContext();
        var connectionString = db.Database.GetConnectionString()!;
        var builder = Microsoft.Extensions.Hosting.Host.CreateApplicationBuilder();
        builder.Configuration.AddInMemoryCollection(
            new Dictionary<string, string?>
            {
                [$"{SampleOptions.SectionName}:{nameof(SampleOptions.ConnectionString)}"] =
                    connectionString,
            }
        );
        builder.Services.AddSampleRetentionServices();
        builder.Services.RemoveAll<IRetentionRuleProvider>();
        builder.Services.AddSingleton<IRetentionRuleProvider>(
            new StaticCategoryRepository(
                new Dictionary<string, ITestRetentionRule>
                {
                    ["short-lived"] = new StaticTestRetentionRule(
                        new RetentionRule(TimeSpan.FromDays(30), Strategy.SoftDelete)
                    ),
                    ["soft-delete"] = new StaticTestRetentionRule(
                        new RetentionRule(TimeSpan.FromDays(30), Strategy.SoftDelete)
                    ),
                    ["anonymise"] = new StaticTestRetentionRule(
                        new RetentionRule(TimeSpan.FromDays(30), Strategy.Anonymise)
                    ),
                }
            )
        );
        using var host = builder.Build();

        var act = () => host.StartAsync();

        var exception = await act.Should().ThrowAsync<RetentionConfigurationException>();
        exception.Which.Errors.Should().ContainSingle();
        exception
            .Which.Errors[0]
            .Should()
            .Be(
                $"Soft-delete convention on {typeof(Note).FullName}: retained SoftDelete categories require a public bool soft-delete flag property (named IsDeleted by convention, or marked with [RetentionSoftDelete]) mapped by EF."
            );
    }

    [Fact]
    public async Task Sweep_Path_Does_Not_Modify_Rows_That_Were_Already_Soft_Deleted()
    {
        var tenantId = Guid.NewGuid();
        var asOf = new DateTimeOffset(2026, 4, 12, 12, 0, 0, TimeSpan.Zero);
        var originalDeletedAt = asOf.AddDays(-20);

        await using (var db = Host.CreateDbContext())
        {
            db.SoftDeleteRecords.AddRange(
                new SoftDeleteRecord
                {
                    Id = Guid.NewGuid(),
                    TenantId = tenantId,
                    CreatedAt = asOf.AddDays(-120),
                    Body = "soft-delete-once",
                    IsDeleted = false,
                },
                new SoftDeleteRecord
                {
                    Id = Guid.NewGuid(),
                    TenantId = tenantId,
                    CreatedAt = asOf.AddDays(-150),
                    Body = "soft-delete-already-done",
                    IsDeleted = true,
                    DeletedAt = originalDeletedAt,
                }
            );
            await db.SaveChangesAsync();
        }

        var tenant = new TenantContext(tenantId, "uk", new Dictionary<string, string>());

        var first = await Host.RunSweepAsync(tenant, asOf);
        var second = await Host.RunSweepAsync(tenant, asOf);

        first
            .Counts.Should()
            .Contain(
                new EntitySweepCount(
                    typeof(SoftDeleteRecord),
                    "soft-delete",
                    tenantId,
                    Strategy.SoftDelete,
                    1
                )
            );
        second
            .Counts.Should()
            .Contain(
                new EntitySweepCount(
                    typeof(SoftDeleteRecord),
                    "soft-delete",
                    tenantId,
                    Strategy.SoftDelete,
                    0
                )
            );

        await using var verify = Host.CreateDbContext();
        var records = await verify
            .SoftDeleteRecords.OrderBy(record => record.Body)
            .Select(record => new
            {
                record.Body,
                record.IsDeleted,
                record.DeletedAt,
            })
            .ToListAsync();

        records
            .Should()
            .Equal(
                new
                {
                    Body = "soft-delete-already-done",
                    IsDeleted = true,
                    DeletedAt = (DateTimeOffset?)originalDeletedAt,
                },
                new
                {
                    Body = "soft-delete-once",
                    IsDeleted = true,
                    DeletedAt = (DateTimeOffset?)asOf,
                }
            );
    }

    [Fact]
    public async Task Preview_Path_Rejects_SoftDelete_Rules_When_The_Entity_Lacks_SoftDelete_Metadata()
    {
        await using var db = Host.CreateDbContext();
        var repository = new StaticCategoryRepository(
            new Dictionary<string, ITestRetentionRule>
            {
                ["short-lived"] = new StaticTestRetentionRule(
                    new RetentionRule(TimeSpan.FromDays(30), Strategy.SoftDelete)
                ),
                ["soft-delete"] = new StaticTestRetentionRule(
                    new RetentionRule(TimeSpan.FromDays(30), Strategy.SoftDelete)
                ),
                ["anonymise"] = new StaticTestRetentionRule(
                    new RetentionRule(TimeSpan.FromDays(30), Strategy.Anonymise)
                ),
            }
        );
        var preview = new RetentionPreviewService(
            db,
            new RetentionRegistry(db, new RetentionEntryBuilder(new RetentionModelConventions())),
            repository,
            new RetentionRuntimeReadinessValidator(
                db,
                new RetentionStartupValidator(
                    db,
                    repository,
                    new RetentionEntryBuilder(new RetentionModelConventions()),
                    [new GuidTombstoneFactory(), new OriginalValueTombstoneFactory()]
                ),
                new CohortSchemaValidator(db),
                new RetentionRuntimeReadinessState()
            ),
            [
                new PurgeSweepStrategy(),
                new SoftDeleteSweepStrategy(),
                new AnonymiseSweepStrategy(
                    db,
                    [new GuidTombstoneFactory(), new OriginalValueTombstoneFactory()]
                ),
            ]
        );

        var act = () =>
            preview.ExecuteAsync(
                RetentionPreviewRequest.Tenanted(
                    new TenantContext(Guid.NewGuid(), "uk", new Dictionary<string, string>()),
                    new DateTimeOffset(2026, 4, 12, 12, 0, 0, TimeSpan.Zero)
                )
            );

        await act.Should()
            .ThrowAsync<RetentionConfigurationException>()
            .WithMessage(
                $"*Soft-delete convention on {typeof(Note).FullName}: retained SoftDelete categories require a public bool soft-delete flag property (named IsDeleted by convention, or marked with [RetentionSoftDelete]) mapped by EF.*"
            );
    }

    private sealed class StaticCategoryRepository(
        IReadOnlyDictionary<string, ITestRetentionRule> resolvers
    ) : ITestRetentionRuleProvider
    {
        private static readonly ITestRetentionRule ExemptFallback =
            new StaticTestRetentionRule(
                new RetentionRule(TimeSpan.FromDays(30), Strategy.Exempt)
            );

        public Task<ITestRetentionRule?> GetAsync(string category, CancellationToken ct)
        {
            return resolvers.TryGetValue(category, out var resolver)
                ? Task.FromResult<ITestRetentionRule?>(resolver)
                : Task.FromResult<ITestRetentionRule?>(ExemptFallback);
        }
    }
}
