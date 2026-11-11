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

    [Theory]
    [InlineData("11111111-1111-1111-1111-111111111111", "uk", "south-east", false, true)]
    [InlineData("22222222-2222-2222-2222-222222222222", "uk", "south-east", false, false)]
    [InlineData("11111111-1111-1111-1111-111111111111", "fr", "south-east", false, false)]
    [InlineData("11111111-1111-1111-1111-111111111111", "UK", "south-east", false, false)]
    [InlineData("11111111-1111-1111-1111-111111111111", "uk", "north-west", false, false)]
    [InlineData("11111111-1111-1111-1111-111111111111", "uk", "South-East", false, false)]
    [InlineData("11111111-1111-1111-1111-111111111111", "uk", "south-east", true, false)]
    public void Tenant_Contexts_Are_Equal_By_Id_Jurisdiction_And_Tags(
        string otherId,
        string otherJurisdiction,
        string otherRegion,
        bool otherHasExtraTag,
        bool equal
    )
    {
        var first = new TenantContext(
            Guid.Parse("11111111-1111-1111-1111-111111111111"),
            "uk",
            new Dictionary<string, string> { ["region"] = "south-east" }
        );
        var otherTags = new Dictionary<string, string> { ["region"] = otherRegion };
        if (otherHasExtraTag)
        {
            otherTags["tier"] = "gold";
        }
        var other = new TenantContext(Guid.Parse(otherId), otherJurisdiction, otherTags);

        (first == other).Should().Be(equal);
        first.Equals((object)other).Should().Be(equal);
        if (equal)
        {
            other.GetHashCode().Should().Be(first.GetHashCode());
        }
    }

    [Fact]
    public void Tenant_Contexts_With_No_Jurisdiction_Are_Equal_And_Differ_From_One_With_A_Jurisdiction()
    {
        var id = Guid.NewGuid();
        var none = new TenantContext(id, null, new Dictionary<string, string>());

        none.Should().Be(new TenantContext(id, null, new Dictionary<string, string>()));
        none.Should().NotBe(new TenantContext(id, "uk", new Dictionary<string, string>()));
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

    [Theory]
    [InlineData("")]
    [InlineData("not-a-uuid")]
    [InlineData("00000000-0000-0000-0000-000000000000")]
    public void The_Persisted_Retention_Entity_Id_Needs_A_Non_Empty_Uuid(string id)
    {
        var entityId = () => new RetentionEntityIdAttribute(id);

        entityId.Should().Throw<ArgumentException>().WithParameterName("id");
    }

    [Fact]
    public void An_Erasure_Request_Needs_A_Defined_Transaction_Mode_And_Keeps_Its_Time_In_Utc()
    {
        var scope = new ErasureScope("user", Guid.NewGuid());
        var undefined = () => RetentionErasureRequest.Tenanted(
            new TenantContext(Guid.NewGuid(), "uk", new Dictionary<string, string>()),
            scope,
            transaction: (ErasureTransaction)7
        );
        var at = new DateTimeOffset(2026, 4, 12, 13, 0, 0, TimeSpan.FromHours(1));

        undefined.Should().Throw<ArgumentOutOfRangeException>().WithParameterName("transaction");
        RetentionErasureRequest.Tenantless(scope, at).At.Should().Be(at).And.Subject!.Value.Offset.Should().Be(TimeSpan.Zero);
        RetentionErasureRequest.Tenantless(scope).At.Should().BeNull();
        RetentionSweepRequest.Tenantless().At.Should().BeNull();
    }

    [Fact]
    public void A_Preview_Request_Keeps_Its_Time_In_Utc_And_Defaults_To_The_Database_Clock()
    {
        var at = new DateTimeOffset(2026, 4, 12, 13, 0, 0, TimeSpan.FromHours(1));

        RetentionPreviewRequest.Tenantless(at).At.Should().Be(at).And.Subject!.Value.Offset.Should().Be(TimeSpan.Zero);
        RetentionPreviewRequest.Tenantless().At.Should().BeNull();
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

    [Fact]
    public void A_Run_Starts_With_A_Sweep_Id()
    {
        var act = () => new SweepEvent.Started(
            Guid.Empty,
            DateTimeOffset.UnixEpoch,
            SweepTriggerKind.Manual,
            DryRun: false,
            Guid.NewGuid()
        );

        act.Should().Throw<ArgumentException>().WithParameterName("SweepId");
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
