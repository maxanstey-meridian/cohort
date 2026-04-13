using Cohort.Application;
using Cohort.Domain;

namespace Cohort.Sample;

public sealed class GuidTombstoneFactory : IAnonymiseValueFactory
{
    public static readonly Guid TombstoneValue = Guid.Parse("11111111-1111-1111-1111-111111111111");

    public List<AnonymiseValueContext> Contexts { get; } = [];

    public object? Create(AnonymiseValueContext context)
    {
        Contexts.Add(context);
        return TombstoneValue;
    }
}

public sealed class OriginalValueTombstoneFactory : IAnonymiseValueFactory
{
    public bool RequiresOriginalValue => true;

    public List<AnonymiseValueContext> Contexts { get; } = [];

    public object? Create(AnonymiseValueContext context)
    {
        Contexts.Add(context);
        return $"{context.OriginalValue}-tombstone";
    }
}
