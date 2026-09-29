using Cohort.Domain;

namespace Cohort.Tests;

public sealed class RetentionRuleContractTests
{
    [Fact]
    public void RetentionRule_Rejects_A_Negative_Period()
    {
        var act = () => new RetentionRule(TimeSpan.FromDays(-30), Strategy.Purge);

        act.Should().Throw<ArgumentOutOfRangeException>().WithParameterName("Period");
    }

    [Fact]
    public void RetentionRule_Rejects_A_Negative_Legal_Min()
    {
        var act = () =>
            new RetentionRule(TimeSpan.FromDays(30), Strategy.Purge, TimeSpan.FromDays(-90));

        act.Should().Throw<ArgumentOutOfRangeException>().WithParameterName("LegalMin");
    }

    [Theory]
    [InlineData(99, (int)AuditRowDetail.SummaryOnly, "Strategy")]
    [InlineData((int)Strategy.Purge, 99, "AuditRowDetail")]
    public void RetentionRule_Rejects_Undefined_Enum_Values(
        int strategy,
        int auditRowDetail,
        string parameterName
    )
    {
        var act = () =>
            new RetentionRule(
                TimeSpan.FromDays(30),
                (Strategy)strategy,
                AuditRowDetail: (AuditRowDetail)auditRowDetail
            );

        act.Should().Throw<ArgumentOutOfRangeException>().WithParameterName(parameterName);
    }

    [Fact]
    public void RetentionRule_Rejects_Inherited_Audit_Row_Detail()
    {
        var act = () =>
            new RetentionRule(
                TimeSpan.FromDays(30),
                Strategy.Purge,
                AuditRowDetail: AuditRowDetail.Inherit
            );

        act.Should().Throw<ArgumentOutOfRangeException>().WithParameterName("AuditRowDetail");
    }
}
