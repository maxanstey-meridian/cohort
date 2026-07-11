using Cohort.Application;
using Cohort.Domain;

namespace Cohort.Tests;

public sealed class RetentionSweepContractTests
{
    [Fact]
    public async Task SweepAsync_Uses_Tenanted_Entity_Scope_Through_Default_Interface_Implementation()
    {
        var sweep = new CapturingSweep();
        var tenant = new TenantContext(Guid.NewGuid(), "uk", new Dictionary<string, string>());

        await ((IRetentionSweep)sweep).SweepAsync(tenant, DateTimeOffset.UtcNow);

        sweep.Request.Should().NotBeNull();
        sweep.Request.Should().BeOfType<RetentionSweepRequest.TenantedRequest>();
        ((RetentionSweepRequest.TenantedRequest)sweep.Request!).Tenant.Should().BeSameAs(tenant);
    }

    [Fact]
    public void Requests_Expose_Only_Explicit_Tenanted_And_Tenantless_Variants()
    {
        var tenant = new TenantContext(Guid.NewGuid(), "uk", new Dictionary<string, string>());
        var at = DateTimeOffset.UtcNow;

        RetentionSweepRequest.Tenanted(tenant, at)
            .Should()
            .BeOfType<RetentionSweepRequest.TenantedRequest>();
        RetentionSweepRequest.Tenantless(at)
            .Should()
            .BeOfType<RetentionSweepRequest.TenantlessRequest>();
        RetentionPreviewRequest.Tenanted(tenant, at)
            .Should()
            .BeOfType<RetentionPreviewRequest.TenantedRequest>();
        RetentionPreviewRequest.Tenantless(at)
            .Should()
            .BeOfType<RetentionPreviewRequest.TenantlessRequest>();
    }

    private sealed class CapturingSweep : IRetentionSweep
    {
        public RetentionSweepRequest? Request { get; private set; }

        public Task<RetentionSweepResult> ExecuteAsync(
            RetentionSweepRequest request,
            CancellationToken ct = default
        )
        {
            Request = request;
            return Task.FromResult(
                new RetentionSweepResult(Guid.NewGuid(), request.At, request.At, [])
            );
        }
    }
}
