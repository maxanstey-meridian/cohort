using Cohort.Application;
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
        services.AddRowHandler<FirstEntity, FirstHandler>(RowHandlerDispatchPhase.AfterSweepSettled);
        services.AddRowHandler<SecondEntity, SecondHandler>(RowHandlerDispatchPhase.AfterSweepSettled);

        using var provider = services.BuildServiceProvider();
        var registrations = provider.GetServices<IRetentionHandlerRegistration>().ToArray();

        registrations.Should().HaveCount(2);
        registrations
            .Should()
            .OnlyContain(registration =>
                registration.DispatchPhase == RowHandlerDispatchPhase.AfterSweepSettled
            );
        registrations.Select(registration => registration.HandlerType)
            .Should()
            .BeEquivalentTo([typeof(FirstHandler), typeof(SecondHandler)]);
    }

    [Fact]
    public void AddRowHandler_Registers_The_Same_Pair_Only_Once()
    {
        var services = new ServiceCollection();
        services.AddRowHandler<FirstEntity, FirstHandler>(RowHandlerDispatchPhase.AfterSweepSettled);
        services.AddRowHandler<FirstEntity, FirstHandler>(RowHandlerDispatchPhase.AfterSweepSettled);

        using var provider = services.BuildServiceProvider();

        provider.GetServices<IRetentionHandlerRegistration>().Should().ContainSingle();
        provider.GetServices<IRetentionHandler<FirstEntity>>().Should().ContainSingle();
    }

    private sealed class FirstEntity;

    private sealed class SecondEntity;

    private sealed class FirstHandler : IRetentionHandler<FirstEntity>;

    private sealed class SecondHandler : IRetentionHandler<SecondEntity>;
}
