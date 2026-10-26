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

    private sealed class FirstEntity;

    private sealed class SecondEntity;

    [RowHandlerId("761d9837-05be-4694-bc7d-0fac82e98e67")]
    private sealed class FirstHandler : IRetentionHandler<FirstEntity>;

    [RowHandlerId("761d9837-05be-4694-bc7d-0fac82e98e67")]
    private sealed class AnotherFirstHandler : IRetentionHandler<FirstEntity>;

    [RowHandlerId("bb3f5ec6-9a5a-4f2b-b251-712f862ad752")]
    private sealed class SecondHandler : IRetentionHandler<SecondEntity>;

    private sealed class UnidentifiedHandler : IRetentionHandler<FirstEntity>;
}
