using Cohort.Application;
using Cohort.Domain;
using Cohort.Hosting;

using Microsoft.Extensions.DependencyInjection;

namespace Cohort.Tests;

public sealed class AddRowHandlerRegistrationTests
{
    [Fact]
    public void AddRowHandler_Preserves_Dispatch_Phase_Metadata_For_Every_Registration()
    {
        // Regression guard: TryAddEnumerable dedupes instance descriptors by the
        // instance's concrete type. With a single shared registration type, every
        // AddRowHandler call after the first silently lost its dispatch phase and
        // fell back to Immediate.
        var services = new ServiceCollection();
        services.AddRowHandler<FirstEntity, FirstHandler>(
            RowHandlerDispatchPhase.AfterSweepSettled
        );
        services.AddRowHandler<SecondEntity, SecondHandler>(
            RowHandlerDispatchPhase.AfterSweepSettled
        );

        using var provider = services.BuildServiceProvider();
        var registrations = provider.GetServices<IRetentionHandlerRegistration>().ToArray();

        registrations.Should().HaveCount(2);
        registrations
            .Should()
            .OnlyContain(registration =>
                registration.DispatchPhase == RowHandlerDispatchPhase.AfterSweepSettled
            );
        registrations
            .Select(registration => registration.HandlerType)
            .Should()
            .BeEquivalentTo([typeof(FirstHandler), typeof(SecondHandler)]);
    }

    [Fact]
    public void AddRowHandler_Rejects_Two_Handlers_Sharing_An_Identity_For_The_Same_Entity()
    {
        // FirstHandler and AnotherFirstHandler carry the same [RowHandlerId]: both would
        // persist queued work under it, and dispatch would hand either's rows to whichever
        // resolves first.
        var services = new ServiceCollection();
        services.AddRowHandler<FirstEntity, FirstHandler>();

        var clash = () => services.AddRowHandler<FirstEntity, AnotherFirstHandler>();

        clash
            .Should()
            .Throw<InvalidOperationException>()
            .WithMessage($"*{typeof(FirstHandler).FullName}*{typeof(AnotherFirstHandler).FullName}*own [RowHandlerId]*");
    }

    [Fact]
    public void AddRowHandler_Rejects_Two_Handlers_Sharing_An_Identity_Across_Entities()
    {
        // A requeue by handler id doesn't filter on the entity, so it would reach both.
        var services = new ServiceCollection();
        services.AddRowHandler<FirstEntity, FirstHandler>();

        var clash = () => services.AddRowHandler<SecondEntity, SecondEntityHandlerReusingFirstsId>();

        clash
            .Should()
            .Throw<InvalidOperationException>()
            .WithMessage(
                $"*{typeof(FirstHandler).FullName}*{typeof(SecondEntityHandlerReusingFirstsId).FullName}*own [RowHandlerId]*"
            );
    }

    [Fact]
    public void AddRowHandler_Lets_One_Handler_Class_Keep_Its_Id_For_Two_Entities()
    {
        var services = new ServiceCollection();
        services.AddRowHandler<FirstEntity, TwoEntityHandler>();

        services.AddRowHandler<SecondEntity, TwoEntityHandler>();

        using var provider = services.BuildServiceProvider();
        provider.GetServices<IRetentionHandlerRegistration>().Should().HaveCount(2);
    }

    [Fact]
    public void AddRowHandler_Refuses_A_Handler_With_A_Malformed_RowHandlerId()
    {
        var services = new ServiceCollection();

        var register = () => services.AddRowHandler<FirstEntity, MalformedIdHandler>();

        register
            .Should()
            .Throw<InvalidOperationException>()
            .WithMessage($"*{typeof(MalformedIdHandler).FullName}*invalid [RowHandlerId]*non-empty UUID*");
        services.Should().BeEmpty();
    }

    [Fact]
    public void AddRowHandler_Refuses_A_Handler_Without_RowHandlerId()
    {
        var services = new ServiceCollection();

        var register = () => services.AddRowHandler<FirstEntity, UnidentifiedHandler>();

        register
            .Should()
            .Throw<InvalidOperationException>()
            .WithMessage($"*{typeof(UnidentifiedHandler).FullName}*[RowHandlerId(*");
        services.Should().BeEmpty();
    }

    [Fact]
    public void AddRetentionDependents_Registers_The_Step_And_Its_Identity()
    {
        var services = new ServiceCollection();
        services.AddRetentionDependents<FirstEntity, FirstStep>();
        services.AddRetentionDependents<FirstEntity, FirstStep>();

        using var provider = services.BuildServiceProvider();
        using var scope = provider.CreateScope();
        scope.ServiceProvider.GetServices<IRetentionDependents<FirstEntity>>().Should().ContainSingle()
            .Which.Should().BeOfType<FirstStep>();
        var step = provider.GetServices<IRetentionDependentsRegistration>().StepFor(typeof(FirstEntity));
        step.Should().NotBeNull();
        step!.StepType.Should().Be(typeof(FirstStep));
        step.Identity.Should().Be(Guid.Parse(FirstStepId));
        provider.GetServices<IRetentionDependentsRegistration>().StepFor(typeof(SecondEntity)).Should().BeNull();
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

    [Fact]
    public void AddRetentionDependents_Refuses_An_Id_Another_Step_Uses()
    {
        var services = new ServiceCollection();
        services.AddRetentionDependents<FirstEntity, FirstStep>();

        var clash = () => services.AddRetentionDependents<SecondEntity, SecondStepReusingFirstStepsId>();

        clash
            .Should()
            .Throw<InvalidOperationException>()
            .WithMessage($"*{typeof(FirstStep).FullName}*{typeof(SecondStepReusingFirstStepsId).FullName}*own [RowHandlerId]*");
    }

    // One id space: a step's failure is recorded under its id beside handlers' work.
    [Fact]
    public void AddRetentionDependents_Refuses_An_Id_A_Row_Handler_Uses()
    {
        var services = new ServiceCollection();
        services.AddRowHandler<FirstEntity, FirstHandler>();

        var clash = () => services.AddRetentionDependents<SecondEntity, SecondStepReusingFirstHandlersId>();

        clash
            .Should()
            .Throw<InvalidOperationException>()
            .WithMessage($"*{typeof(FirstHandler).FullName}*{typeof(SecondStepReusingFirstHandlersId).FullName}*own [RowHandlerId]*");
    }

    [Fact]
    public void AddRowHandler_Refuses_An_Id_A_Dependents_Step_Uses()
    {
        var services = new ServiceCollection();
        services.AddRetentionDependents<SecondEntity, SecondStepReusingFirstHandlersId>();

        var clash = () => services.AddRowHandler<FirstEntity, FirstHandler>();

        clash
            .Should()
            .Throw<InvalidOperationException>()
            .WithMessage($"*{typeof(SecondStepReusingFirstHandlersId).FullName}*{typeof(FirstHandler).FullName}*own [RowHandlerId]*");
    }

    [Fact]
    public void One_Class_May_Be_The_Handler_And_The_Step_Of_One_Entity_In_Either_Order()
    {
        var handlerFirst = new ServiceCollection();
        handlerFirst.AddRowHandler<FirstEntity, HandlerAndStep>();
        handlerFirst.AddRetentionDependents<FirstEntity, HandlerAndStep>();

        var stepFirst = new ServiceCollection();
        stepFirst.AddRetentionDependents<FirstEntity, HandlerAndStep>();
        stepFirst.AddRowHandler<FirstEntity, HandlerAndStep>();

        foreach (var services in new[] { handlerFirst, stepFirst })
        {
            using var provider = services.BuildServiceProvider();
            provider.GetServices<IRetentionHandlerRegistration>().Should().ContainSingle();
            provider.GetServices<IRetentionDependentsRegistration>().Should().ContainSingle();
        }
    }

    [Fact]
    public void One_Class_Can_Not_Be_The_Handler_Of_One_Entity_And_The_Step_Of_Another()
    {
        var handlerFirst = new ServiceCollection();
        handlerFirst.AddRowHandler<FirstEntity, HandlerAndStep>();
        var stepFirst = new ServiceCollection();
        stepFirst.AddRetentionDependents<SecondEntity, HandlerAndStep>();

        var stepOfAnother = () => handlerFirst.AddRetentionDependents<SecondEntity, HandlerAndStep>();
        var handlerOfAnother = () => stepFirst.AddRowHandler<FirstEntity, HandlerAndStep>();

        stepOfAnother.Should().Throw<InvalidOperationException>().WithMessage("*same entity under one id*");
        handlerOfAnother.Should().Throw<InvalidOperationException>().WithMessage("*same entity under one id*");
    }

    [Fact]
    public void AddRetentionDependents_Refuses_A_Step_With_A_Malformed_RowHandlerId()
    {
        var services = new ServiceCollection();

        var register = () => services.AddRetentionDependents<FirstEntity, MalformedIdStep>();

        register
            .Should()
            .Throw<InvalidOperationException>()
            .WithMessage($"Dependents step {typeof(MalformedIdStep).FullName}*invalid [RowHandlerId]*non-empty UUID*");
        services.Should().BeEmpty();
    }

    [Fact]
    public void AddRetentionDependents_Refuses_A_Step_Without_RowHandlerId()
    {
        var services = new ServiceCollection();

        var register = () => services.AddRetentionDependents<FirstEntity, UnidentifiedStep>();

        register
            .Should()
            .Throw<InvalidOperationException>()
            .WithMessage($"Dependents step {typeof(UnidentifiedStep).FullName}*[RowHandlerId(*failures are recorded under it*");
        services.Should().BeEmpty();
    }

    private const string FirstStepId = "4b7e0d29-81c3-4a5f-9e62-d3f18a0b7c54";

    private sealed class FirstEntity;

    private sealed class SecondEntity;

    [RowHandlerId("761d9837-05be-4694-bc7d-0fac82e98e67")]
    private sealed class FirstHandler : IRetentionHandler<FirstEntity>;

    [RowHandlerId("761d9837-05be-4694-bc7d-0fac82e98e67")]
    private sealed class AnotherFirstHandler : IRetentionHandler<FirstEntity>;

    [RowHandlerId("bb3f5ec6-9a5a-4f2b-b251-712f862ad752")]
    private sealed class SecondHandler : IRetentionHandler<SecondEntity>;

    [RowHandlerId("761d9837-05be-4694-bc7d-0fac82e98e67")]
    private sealed class SecondEntityHandlerReusingFirstsId : IRetentionHandler<SecondEntity>;

    [RowHandlerId("0e0b8a43-6f1c-4d0e-9d55-2f4b6f3c9a17")]
    private sealed class TwoEntityHandler : IRetentionHandler<FirstEntity>, IRetentionHandler<SecondEntity>;

    [RowHandlerId("not-a-uuid")]
    private sealed class MalformedIdHandler : IRetentionHandler<FirstEntity>;

    private sealed class UnidentifiedHandler : IRetentionHandler<FirstEntity>;

    [RowHandlerId(FirstStepId)]
    private sealed class FirstStep : IRetentionDependents<FirstEntity>
    {
        public Task RetireAsync(FirstEntity row, RetentionDependentsContext ctx, CancellationToken ct) => Task.CompletedTask;
    }

    [RowHandlerId("a93f6c18-5d2e-4b07-8c41-7e0b9d3a6f25")]
    private sealed class OtherFirstStep : IRetentionDependents<FirstEntity>
    {
        public Task RetireAsync(FirstEntity row, RetentionDependentsContext ctx, CancellationToken ct) => Task.CompletedTask;
    }

    [RowHandlerId(FirstStepId)]
    private sealed class SecondStepReusingFirstStepsId : IRetentionDependents<SecondEntity>
    {
        public Task RetireAsync(SecondEntity row, RetentionDependentsContext ctx, CancellationToken ct) => Task.CompletedTask;
    }

    [RowHandlerId("761d9837-05be-4694-bc7d-0fac82e98e67")]
    private sealed class SecondStepReusingFirstHandlersId : IRetentionDependents<SecondEntity>
    {
        public Task RetireAsync(SecondEntity row, RetentionDependentsContext ctx, CancellationToken ct) => Task.CompletedTask;
    }

    [RowHandlerId("e25c9a70-4f1b-4d83-a6e9-0b8d7c3f1a46")]
    private sealed class HandlerAndStep
        : IRetentionHandler<FirstEntity>, IRetentionDependents<FirstEntity>, IRetentionDependents<SecondEntity>
    {
        public Task RetireAsync(FirstEntity row, RetentionDependentsContext ctx, CancellationToken ct) => Task.CompletedTask;

        public Task RetireAsync(SecondEntity row, RetentionDependentsContext ctx, CancellationToken ct) => Task.CompletedTask;
    }

    [RowHandlerId("not-a-uuid")]
    private sealed class MalformedIdStep : IRetentionDependents<FirstEntity>
    {
        public Task RetireAsync(FirstEntity row, RetentionDependentsContext ctx, CancellationToken ct) => Task.CompletedTask;
    }

    private sealed class UnidentifiedStep : IRetentionDependents<FirstEntity>
    {
        public Task RetireAsync(FirstEntity row, RetentionDependentsContext ctx, CancellationToken ct) => Task.CompletedTask;
    }
}
