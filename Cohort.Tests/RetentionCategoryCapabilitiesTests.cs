using Cohort.Domain;

namespace Cohort.Tests;

public sealed class RetentionCategoryCapabilitiesTests
{
    [Fact]
    public void Constructor_Defensively_Copies_A_NonEmpty_Strategy_Set()
    {
        var strategies = new HashSet<Strategy> { Strategy.Purge, Strategy.Anonymise };

        var capabilities = new RetentionCategoryCapabilities(strategies);
        strategies.Clear();

        capabilities.Strategies.Should().BeEquivalentTo([Strategy.Purge, Strategy.Anonymise]);
    }

    [Fact]
    public void Constructor_Rejects_An_Empty_Strategy_Set()
    {
        var act = () => new RetentionCategoryCapabilities([]);

        act.Should().Throw<ArgumentException>();
    }

    [Fact]
    public void Constructor_Rejects_An_Undefined_Strategy()
    {
        var act = () => new RetentionCategoryCapabilities([(Strategy)int.MaxValue]);

        act.Should().Throw<ArgumentOutOfRangeException>();
    }

    [Fact]
    public void Strategies_Cannot_Be_Mutated_Through_The_Public_Contract()
    {
        var capabilities = new RetentionCategoryCapabilities([Strategy.Purge]);

        capabilities.Strategies.Should().NotBeAssignableTo<ISet<Strategy>>();
        capabilities.Strategies.Should().Equal(Strategy.Purge);
    }
}
