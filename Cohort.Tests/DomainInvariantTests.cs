using Cohort.Application;
using Cohort.Domain;

namespace Cohort.Tests;

public sealed class DomainInvariantTests
{
    [Fact]
    public void Retention_Hold_Request_Rejects_Expiry_Before_Creation()
    {
        var createdAt = DateTimeOffset.Parse("2026-01-02T00:00:00+00:00");

        var act = () => CreateHold(
            createdAt: createdAt,
            expiresAt: createdAt.AddTicks(-1)
        );

        act.Should().Throw<ArgumentOutOfRangeException>().WithParameterName("ExpiresAt");
    }

    [Fact]
    public void Tenant_Context_Copies_And_Protects_Tags()
    {
        var source = new Dictionary<string, string> { ["region"] = "south-east" };
        var tenant = new TenantContext(Guid.NewGuid(), "uk", source);

        source["region"] = "north-west";
        var act = () => ((IDictionary<string, string>)tenant.Tags)["region"] = "london";

        tenant.Tags["region"].Should().Be("south-east");
        act.Should().Throw<NotSupportedException>();
    }

    [Fact]
    public void Tenant_Context_Rejects_The_Reserved_Tenantless_Identity()
    {
        var act = () =>
            new TenantContext(Guid.Empty, "uk", new Dictionary<string, string>());

        act.Should().Throw<ArgumentException>().WithParameterName("id");
    }

    [Theory]
    [InlineData("")]
    [InlineData(" ")]
    public void Erasure_Subject_Kinds_Cannot_Be_Blank(string kind)
    {
        var attribute = () => new ErasureSubjectAttribute(kind);
        var scope = () => new ErasureScope(kind, Guid.NewGuid());

        attribute.Should().Throw<ArgumentException>().WithParameterName("kind");
        scope.Should().Throw<ArgumentException>().WithParameterName("kind");
    }

    [Theory]
    [InlineData(SweepTriggerKind.Erasure, null)]
    [InlineData(SweepTriggerKind.Erasure, " ")]
    [InlineData(SweepTriggerKind.Scheduled, "person")]
    [InlineData(SweepTriggerKind.Manual, "person")]
    public void Only_An_Erasure_Run_Starts_With_A_Subject_Kind(
        SweepTriggerKind trigger,
        string? erasureSubjectKind
    )
    {
        var act = () => new SweepEvent.Started(
            Guid.NewGuid(),
            DateTimeOffset.UnixEpoch,
            trigger,
            DryRun: false,
            Guid.NewGuid(),
            erasureSubjectKind
        );

        act.Should().Throw<ArgumentException>().WithParameterName("ErasureSubjectKind");
    }

    private static RetentionHoldRequest CreateHold(
        Guid? holdId = null,
        Guid? retentionEntityId = null,
        string recordId = "record-1",
        string reason = "Legal dispute",
        DateTimeOffset? createdAt = null,
        DateTimeOffset? expiresAt = null
    )
    {
        return new RetentionHoldRequest(
            holdId ?? Guid.NewGuid(),
            retentionEntityId ?? Guid.NewGuid(),
            recordId,
            Guid.NewGuid(),
            reason,
            createdAt ?? DateTimeOffset.Parse("2026-01-01T00:00:00+00:00"),
            expiresAt
        );
    }
}
