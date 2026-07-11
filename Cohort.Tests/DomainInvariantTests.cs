using Cohort.Domain;

namespace Cohort.Tests;

public sealed class DomainInvariantTests
{
    [Fact]
    public void Retention_Hold_Request_Rejects_An_Empty_Retention_Entity_Id()
    {
        var act = () => CreateHold(retentionEntityId: Guid.Empty);

        act.Should().Throw<ArgumentException>().WithParameterName("RetentionEntityId");
    }

    [Theory]
    [InlineData(null)]
    [InlineData("")]
    [InlineData(" ")]
    public void Retention_Hold_Request_Rejects_A_Blank_Reason(string? reason)
    {
        var act = () => CreateHold(reason: reason!);

        act.Should().Throw<ArgumentException>().WithParameterName("Reason");
    }

    [Theory]
    [InlineData(null)]
    [InlineData("")]
    [InlineData(" ")]
    public void Retention_Hold_Request_Rejects_A_Blank_Record_Id(string? recordId)
    {
        var act = () => CreateHold(recordId: recordId!);

        act.Should().Throw<ArgumentException>().WithParameterName("RecordId");
    }

    [Fact]
    public void Retention_Hold_Request_Rejects_An_Empty_Hold_Id()
    {
        var act = () => CreateHold(holdId: Guid.Empty);

        act.Should().Throw<ArgumentException>().WithParameterName("HoldId");
    }

    [Fact]
    public void Retention_Hold_Request_Rejects_An_Empty_Tenant_Id()
    {
        var act = () => new RetentionHoldRequest(
            Guid.NewGuid(),
            Guid.NewGuid(),
            "record-1",
            Guid.Empty,
            "Legal dispute",
            DateTimeOffset.Parse("2026-01-01T00:00:00+00:00")
        );

        act.Should().Throw<ArgumentException>().WithParameterName("TenantId");
    }

    [Fact]
    public void Retention_Hold_Request_Allows_A_Null_Tenant_Id()
    {
        var request = new RetentionHoldRequest(
            Guid.NewGuid(),
            Guid.NewGuid(),
            "record-1",
            null,
            "Legal dispute",
            DateTimeOffset.Parse("2026-01-01T00:00:00+00:00")
        );

        request.TenantId.Should().BeNull();
    }

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
    public void Retention_Hold_Request_Allows_Expiry_At_Creation()
    {
        var createdAt = DateTimeOffset.Parse("2026-01-02T00:00:00+00:00");

        var request = CreateHold(createdAt: createdAt, expiresAt: createdAt);

        request.ExpiresAt.Should().Be(createdAt);
    }

    [Fact]
    public void Retention_Hold_Request_Timestamps_Cannot_Be_Init_Mutated()
    {
        typeof(RetentionHoldRequest).GetProperty(nameof(RetentionHoldRequest.CreatedAt))!
            .SetMethod.Should()
            .BeNull();
        typeof(RetentionHoldRequest).GetProperty(nameof(RetentionHoldRequest.ExpiresAt))!
            .SetMethod.Should()
            .BeNull();
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
    public void Tenant_Context_Copies_Tags_Assigned_Through_A_Record_Copy()
    {
        var source = new Dictionary<string, string> { ["region"] = "south-east" };
        var tenant = TenantContext.Tenantless with { Tags = source };

        source["region"] = "north-west";

        tenant.Tags["region"].Should().Be("south-east");
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
