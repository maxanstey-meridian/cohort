using Cohort.Domain;

namespace Cohort.Tests;

public sealed class RetentionRuleContractTests
{
    [Fact]
    public void RetentionRule_Uses_Summary_Only_Audit_And_Null_Legal_Min_By_Default()
    {
        var rule = new RetentionRule(TimeSpan.FromDays(30), Strategy.Purge);

        rule.Period.Should().Be(TimeSpan.FromDays(30));
        rule.Strategy.Should().Be(Strategy.Purge);
        rule.LegalMin.Should().BeNull();
        rule.AuditRowDetail.Should().Be(AuditRowDetail.SummaryOnly);
    }

    [Fact]
    public void RetentionRule_Preserves_Explicit_Legal_Min_And_Audit_Detail()
    {
        var rule = new RetentionRule(
            TimeSpan.FromDays(30),
            Strategy.Anonymise,
            TimeSpan.FromDays(90),
            AuditRowDetail.PerRow
        );

        rule.Period.Should().Be(TimeSpan.FromDays(30));
        rule.Strategy.Should().Be(Strategy.Anonymise);
        rule.LegalMin.Should().Be(TimeSpan.FromDays(90));
        rule.AuditRowDetail.Should().Be(AuditRowDetail.PerRow);
    }

    [Fact]
    public void Strategy_Enum_Exposes_The_Planned_Public_Vocabulary()
    {
        Enum.GetNames<Strategy>()
            .Should()
            .Equal(nameof(Strategy.Purge), nameof(Strategy.SoftDelete), nameof(Strategy.Anonymise), nameof(Strategy.Exempt));
    }

    [Fact]
    public void Audit_Row_Detail_Enum_Exposes_The_Planned_Public_Vocabulary()
    {
        Enum.GetNames<AuditRowDetail>()
            .Should()
            .Equal(nameof(AuditRowDetail.SummaryOnly), nameof(AuditRowDetail.PerRow));
    }

    [Fact]
    public void Anonymise_Method_Enum_Exposes_The_Planned_Public_Vocabulary()
    {
        Enum.GetNames<AnonymiseMethod>()
            .Should()
            .Equal(
                nameof(AnonymiseMethod.Null),
                nameof(AnonymiseMethod.EmptyString),
                nameof(AnonymiseMethod.FixedLiteral)
            );
    }

    [Fact]
    public void Retention_Resolution_Context_Captures_Category_Tenant_Now_And_Alias_Path()
    {
        var tenantId = Guid.NewGuid();
        var tenant = new TenantContext(
            tenantId,
            "uk-england",
            new Dictionary<string, string> { ["service"] = "cohort" }
        );
        var now = DateTimeOffset.Parse("2026-01-01T00:00:00+00:00");
        IReadOnlyList<string> aliasPath = ["base-policy", "county-override"];

        var context = new RetentionResolutionContext("short-lived", tenant, now, aliasPath);

        context.Category.Should().Be("short-lived");
        context.Tenant.Id.Should().Be(tenantId);
        context.Tenant.Jurisdiction.Should().Be("uk-england");
        context.Tenant.Tags.Should().Contain(new KeyValuePair<string, string>("service", "cohort"));
        context.Now.Should().Be(now);
        context.AliasPath.Should().Equal("base-policy", "county-override");
    }

    [Fact]
    public void Retention_Sweep_Result_Carries_Grouped_Entity_Counts()
    {
        var sweepId = Guid.NewGuid();
        var tenantId = Guid.NewGuid();
        var count = new EntitySweepCount(
            typeof(string),
            "short-lived",
            tenantId,
            Strategy.Purge,
            3
        );
        var result = new RetentionSweepResult(
            sweepId,
            DateTimeOffset.Parse("2026-01-01T00:00:00+00:00"),
            DateTimeOffset.Parse("2026-01-01T00:05:00+00:00"),
            [count]
        );

        result.SweepId.Should().Be(sweepId);
        result.Counts.Should().ContainSingle();
        result.Counts[0].EntityType.Should().Be(typeof(string));
        result.Counts[0].Category.Should().Be("short-lived");
        result.Counts[0].TenantId.Should().Be(tenantId);
        result.Counts[0].Strategy.Should().Be(Strategy.Purge);
        result.Counts[0].Affected.Should().Be(3);
    }
}
