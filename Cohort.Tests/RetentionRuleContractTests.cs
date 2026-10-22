using Cohort.Domain;

namespace Cohort.Tests;

public sealed class RetentionRuleContractTests
{
    [Theory]
    [InlineData(-1, 0, 0, "Years")]
    [InlineData(0, -1, 0, "Months")]
    [InlineData(0, 0, -1, "Days")]
    [InlineData(int.MaxValue, 0, 0, "Years")]
    public void RetentionPeriod_Rejects_A_Negative_Or_Unstorable_Part(
        int years,
        int months,
        int days,
        string parameterName
    )
    {
        var act = () => new RetentionPeriod(years, months, days);

        act.Should().Throw<ArgumentOutOfRangeException>().WithParameterName(parameterName);
    }

    [Fact]
    public void RetentionPeriod_Carries_Twelve_Months_Into_A_Year_But_Never_Days_Into_A_Month()
    {
        new RetentionPeriod(Months: 18).Should().Be(new RetentionPeriod(Years: 1, Months: 6));
        new RetentionPeriod(Days: 365).Should().NotBe(new RetentionPeriod(Years: 1));
    }

    [Fact]
    public void RetentionRule_Requires_A_Period()
    {
        var act = () => new RetentionRule(null!, Strategy.Purge);

        act.Should().Throw<ArgumentNullException>().WithParameterName("Period");
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
                new RetentionPeriod(Days: 30),
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
                new RetentionPeriod(Days: 30),
                Strategy.Purge,
                AuditRowDetail: AuditRowDetail.Inherit
            );

        act.Should().Throw<ArgumentOutOfRangeException>().WithParameterName("AuditRowDetail");
    }
}
