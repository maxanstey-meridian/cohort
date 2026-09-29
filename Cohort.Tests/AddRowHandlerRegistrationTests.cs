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
        // Both handlers would persist queued work under the same HandlerIdentity, and
        // dispatch would hand either's rows to whichever resolves first.
        var identity = Guid.NewGuid();
        var services = new ServiceCollection();
        services.AddRowHandler<FirstEntity, FirstHandler>(identity: identity);

        var clash = () =>
            services.AddRowHandler<FirstEntity, AnotherFirstHandler>(identity: identity);

        clash.Should().Throw<InvalidOperationException>().WithMessage("*unique per entity*");
    }

    private sealed class FirstEntity;

    private sealed class SecondEntity;

    private sealed class FirstHandler : IRetentionHandler<FirstEntity>;

    private sealed class AnotherFirstHandler : IRetentionHandler<FirstEntity>;

    private sealed class SecondHandler : IRetentionHandler<SecondEntity>;
}
