using Cohort.Application;
using Cohort.Domain;
using Cohort.Hosting;

using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Options;

namespace Cohort.Sample;

public static class SampleServiceCollectionExtensions
{
    public static IServiceCollection AddSampleRetentionServices(this IServiceCollection services)
    {
        services
            .AddOptions<SampleOptions>()
            .BindConfiguration(SampleOptions.SectionName)
            .ValidateDataAnnotations()
            .ValidateOnStart();

        services.AddDbContext<SampleDbContext>(
            (sp, opts) =>
            {
                var sampleOptions = sp.GetRequiredService<IOptions<SampleOptions>>().Value;
                opts.UseNpgsql(sampleOptions.ConnectionString);
            }
        );

        services.AddSingleton<IRetentionCategoryRepository, SampleCategoryRepository>();
        services.AddSingleton(CreatePreviewTenant());
        services.AddSingleton<GuidTombstoneFactory>();
        services.AddSingleton<OriginalValueTombstoneFactory>();
        services.AddSingleton<IAnonymiseValueFactory>(sp => sp.GetRequiredService<GuidTombstoneFactory>());
        services.AddSingleton<IAnonymiseValueFactory>(sp =>
            sp.GetRequiredService<OriginalValueTombstoneFactory>()
        );
        services.AddCohort<SampleDbContext>();
        services.AddScoped<SampleRetentionStartupService>();

        return services;
    }

    public static TenantContext CreatePreviewTenant()
    {
        return new TenantContext(
            Guid.Parse("11111111-1111-1111-1111-111111111111"),
            "sample",
            new Dictionary<string, string>()
        );
    }
}
