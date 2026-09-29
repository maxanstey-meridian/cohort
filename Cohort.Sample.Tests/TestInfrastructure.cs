using Microsoft.EntityFrameworkCore;

namespace Cohort.Sample.Tests;

internal static class MetadataModelDbContextOptionsExtensions
{
    private const string NonconnectingConnectionString =
        "Host=127.0.0.1;Port=1;Database=cohort_metadata;Username=cohort;Password=cohort;Timeout=1";

    public static DbContextOptionsBuilder<TContext> UseNpgsqlMetadataModel<TContext>(
        this DbContextOptionsBuilder<TContext> builder,
        string _
    )
        where TContext : DbContext => builder.UseNpgsql(NonconnectingConnectionString);
}
