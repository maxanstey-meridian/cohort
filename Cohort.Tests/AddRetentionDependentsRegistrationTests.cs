using Cohort.Application;
using Cohort.Domain;
using Cohort.Hosting;

using Microsoft.Extensions.DependencyInjection;

namespace Cohort.Tests;

public sealed class AddRetentionDependentsRegistrationTests
{
    [Fact]
    public void AddRetentionDependents_Registers_The_Step_Once_And_Retires_Referencing_Rows_By_Default()
    {
        var services = new ServiceCollection();
        services.AddRetentionDependents<FirstEntity, FirstStep>();
        services.AddRetentionDependents<FirstEntity, FirstStep>();

        using var provider = services.BuildServiceProvider();
        using var scope = provider.CreateScope();
        scope.ServiceProvider.GetServices<IRetentionDependents<FirstEntity>>().Should().ContainSingle()
            .Which.Should().BeOfType<FirstStep>();
        var registrations = provider.GetServices<IRetentionDependentsRegistration>().ToArray();
        registrations.Should().ContainSingle();
        var step = registrations.StepFor(typeof(FirstEntity));
        step.Should().NotBeNull();
        step!.StepType.Should().Be(typeof(FirstStep));
        step.RetiresReferencingRows.Should().BeTrue();
        registrations.RetiresReferencingRows(typeof(FirstEntity)).Should().BeTrue();
        registrations.StepFor(typeof(SecondEntity)).Should().BeNull();
    }

    [Fact]
    public void AddRetentionDependents_Refuses_A_Second_Step_For_One_Entity()
    {
        var services = new ServiceCollection();
        services.AddRetentionDependents<FirstEntity, FirstStep>();

        var second = () => services.AddRetentionDependents<FirstEntity, OtherFirstStep>();

        second
            .Should()
            .Throw<InvalidOperationException>()
            .WithMessage($"*{typeof(FirstEntity).FullName}*{typeof(FirstStep).FullName}*{typeof(OtherFirstStep).FullName}*one dependents step*");
    }

    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    public void AddRetentionDependents_Refuses_A_Repeat_Call_That_Changes_RetiresReferencingRows(bool first)
    {
        var services = new ServiceCollection();
        services.AddRetentionDependents<FirstEntity, FirstStep>(retiresReferencingRows: first);

        var sameValue = () => services.AddRetentionDependents<FirstEntity, FirstStep>(retiresReferencingRows: first);
        var otherValue = () => services.AddRetentionDependents<FirstEntity, FirstStep>(retiresReferencingRows: !first);

        sameValue.Should().NotThrow();
        otherValue
            .Should()
            .Throw<InvalidOperationException>()
            .WithMessage($"*{typeof(FirstStep).FullName}*{typeof(FirstEntity).FullName}*retiresReferencingRows*");
        using var provider = services.BuildServiceProvider();
        var registrations = provider.GetServices<IRetentionDependentsRegistration>().ToArray();
        registrations.Should().ContainSingle().Which.RetiresReferencingRows.Should().Be(first);
        registrations.HasStep(typeof(FirstEntity)).Should().BeTrue();
        registrations.RetiresReferencingRows(typeof(FirstEntity)).Should().Be(first);
    }

    private sealed class FirstEntity;

    private sealed class SecondEntity;

    private sealed class FirstStep : IRetentionDependents<FirstEntity>
    {
        public Task RetireAsync(FirstEntity row, RetentionDependentsContext ctx, CancellationToken ct) => Task.CompletedTask;
    }

    private sealed class OtherFirstStep : IRetentionDependents<FirstEntity>
    {
        public Task RetireAsync(FirstEntity row, RetentionDependentsContext ctx, CancellationToken ct) => Task.CompletedTask;
    }
}
