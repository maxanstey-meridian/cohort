using Cohort.Domain;

namespace Cohort.Tests;

public sealed class RowHandlerPriorityAttributeTests
{
    [Fact]
    public void Retention_After_Context_Preserves_Immutable_Dispatch_Metadata()
    {
        var snapshot = new Dictionary<string, object?> { ["StoragePath"] = "blob/row-42" };
        var context = new RetentionAfterContext<object>(
            Guid.Parse("fe482ec4-bb4d-4509-b8d6-8a516bd7a1f0"),
            "row-42",
            "files",
            Strategy.SoftDelete,
            Guid.Parse("4c3640af-a9f1-4c01-b7dd-bfe4ee8435ea"),
            DateTimeOffset.Parse("2026-04-13T09:05:00+00:00"),
            3,
            snapshot
        );

        snapshot["StoragePath"] = "blob/row-99";
        snapshot["Checksum"] = "sha256:abc123";

        context.SweepId.Should().Be(Guid.Parse("fe482ec4-bb4d-4509-b8d6-8a516bd7a1f0"));
        context.RecordId.Should().Be("row-42");
        context.Category.Should().Be("files");
        context.Strategy.Should().Be(Strategy.SoftDelete);
        context.TenantId.Should().Be(Guid.Parse("4c3640af-a9f1-4c01-b7dd-bfe4ee8435ea"));
        context.At.Should().Be(DateTimeOffset.Parse("2026-04-13T09:05:00+00:00"));
        context.Attempt.Should().Be(3);
        context.Snapshot.Should().Contain("StoragePath", "blob/row-42");
        context.Snapshot.Should().NotContainKey("Checksum");
    }
}
