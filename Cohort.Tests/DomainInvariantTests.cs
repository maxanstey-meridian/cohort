using Cohort.Application;
using Cohort.Domain;

namespace Cohort.Tests;

public sealed class DomainInvariantTests
{
    [Theory]
    [InlineData(-1)]
    [InlineData(0)]
    public void Retention_Hold_Request_Rejects_Expiry_Not_After_Creation(long expiryOffsetTicks)
    {
        var createdAt = DateTimeOffset.Parse("2026-01-02T00:00:00+00:00");

        var act = () => CreateHold(
            createdAt: createdAt,
            expiresAt: createdAt.AddTicks(expiryOffsetTicks)
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

    [Fact]
    public void Tenant_Execution_Context_Rejects_The_Reserved_Tenantless_Identity()
    {
        var act = () => new RetentionExecutionContext.Tenant(Guid.Empty);

        act.Should().Throw<ArgumentException>().WithParameterName("tenantId");
    }

    [Fact]
    public void System_Execution_Context_Rejects_Undefined_Work()
    {
        var act = () => new RetentionExecutionContext.System((RetentionSystemWork)99);

        act.Should().Throw<ArgumentOutOfRangeException>().WithParameterName("work");
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

    [Fact]
    public void A_Sweep_Request_Cannot_Be_An_Erasure()
    {
        var tenanted = () =>
            RetentionSweepRequest.Tenanted(
                new TenantContext(Guid.NewGuid(), "uk", new Dictionary<string, string>()),
                DateTimeOffset.UnixEpoch,
                SweepTriggerKind.Erasure
            );
        var tenantless = () =>
            RetentionSweepRequest.Tenantless(DateTimeOffset.UnixEpoch, SweepTriggerKind.Erasure);

        tenanted.Should().Throw<ArgumentException>().WithParameterName("trigger");
        tenantless.Should().Throw<ArgumentException>().WithParameterName("trigger");
    }

    [Fact]
    public void An_Erasure_Request_Needs_A_Defined_Transaction_Mode_And_Keeps_Its_Time_In_Utc()
    {
        var scope = new ErasureScope("user", Guid.NewGuid());
        var undefined = () => RetentionErasureRequest.Tenantless(scope, transaction: (ErasureTransaction)7);
        var at = new DateTimeOffset(2026, 4, 12, 13, 0, 0, TimeSpan.FromHours(1));

        undefined.Should().Throw<ArgumentOutOfRangeException>().WithParameterName("transaction");
        RetentionErasureRequest.Tenantless(scope, at).At.Should().Be(at).And.Subject!.Value.Offset.Should().Be(TimeSpan.Zero);
        RetentionErasureRequest.Tenantless(scope).At.Should().BeNull();
        RetentionSweepRequest.Tenantless().At.Should().BeNull();
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
