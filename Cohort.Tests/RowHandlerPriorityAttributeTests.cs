using Cohort.Application;
using Cohort.Domain;

namespace Cohort.Tests;

public sealed class RowHandlerPriorityAttributeTests
{
    [Fact]
    public async Task Retention_Handler_Default_Methods_Are_No_Ops()
    {
        IRetentionHandler<object> handler = new NoOpHandler();
        var before = new RetentionBeforeContext(
            Guid.NewGuid(),
            "files",
            Strategy.Purge,
            Guid.NewGuid(),
            DateTimeOffset.Parse("2026-04-13T09:00:00+00:00")
        );
        var after = new RetentionAfterContext<object>(
            Guid.NewGuid(),
            "row-42",
            "files",
            Strategy.Purge,
            Guid.NewGuid(),
            DateTimeOffset.Parse("2026-04-13T09:05:00+00:00"),
            2,
            new Dictionary<string, object?>()
        );

        var beforeAction = () => handler.OnBeforeAsync(new object(), before, CancellationToken.None);
        var afterAction = () => handler.OnAfterAsync(after, CancellationToken.None);

        await beforeAction.Should().NotThrowAsync();
        await afterAction.Should().NotThrowAsync();
    }

    [Fact]
    public void Retention_Before_Context_Exposes_A_Mutable_Snapshot_Bag()
    {
        var context = new RetentionBeforeContext(
            Guid.Parse("f6483ef7-a60e-49c9-bd3d-6b7e156c3f93"),
            "files",
            Strategy.Anonymise,
            Guid.Parse("ce90e454-77d1-4df7-b291-6971320ebc94"),
            DateTimeOffset.Parse("2026-04-13T09:00:00+00:00")
        );

        context.Snapshot["StoragePath"] = "blob/row-42";

        context.Snapshot.Should().Contain("StoragePath", "blob/row-42");
        context.Category.Should().Be("files");
        context.Strategy.Should().Be(Strategy.Anonymise);
    }

    [Fact]
    public void Retention_After_Context_Preserves_Immutable_Dispatch_Metadata()
    {
        var snapshot = new Dictionary<string, object?>
        {
            ["StoragePath"] = "blob/row-42",
        };
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
        context.EntityId.Should().Be("row-42");
        context.Category.Should().Be("files");
        context.Strategy.Should().Be(Strategy.SoftDelete);
        context.TenantId.Should().Be(Guid.Parse("4c3640af-a9f1-4c01-b7dd-bfe4ee8435ea"));
        context.At.Should().Be(DateTimeOffset.Parse("2026-04-13T09:05:00+00:00"));
        context.Attempt.Should().Be(3);
        context.Snapshot.Should().Contain("StoragePath", "blob/row-42");
        context.Snapshot.Should().NotContainKey("Checksum");
    }

    [Fact]
    public void Get_Priority_Returns_Declared_Priority()
    {
        var priority = RowHandlerPriorityAttribute.GetPriority(typeof(HighPriorityHandler));

        priority.Should().Be(100);
    }

    [Fact]
    public void Get_Priority_Uses_Max_Value_When_Attribute_Is_Not_Present()
    {
        var priority = RowHandlerPriorityAttribute.GetPriority(typeof(DefaultPriorityHandler));

        priority.Should().Be(int.MaxValue);
    }

    [Fact]
    public void Get_Priority_Allows_Unannotated_Handlers_To_Sort_Last()
    {
        var ordered = new[]
        {
            typeof(DefaultPriorityHandler),
            typeof(LowPriorityHandler),
            typeof(HighPriorityHandler),
        }.OrderBy(RowHandlerPriorityAttribute.GetPriority).ToArray();

        ordered.Should().Equal(
            typeof(HighPriorityHandler),
            typeof(LowPriorityHandler),
            typeof(DefaultPriorityHandler)
        );
    }

    [RowHandlerPriority(100)]
    private sealed class HighPriorityHandler;

    [RowHandlerPriority(200)]
    private sealed class LowPriorityHandler;

    private sealed class DefaultPriorityHandler;

    private sealed class NoOpHandler : IRetentionHandler<object>;
}
