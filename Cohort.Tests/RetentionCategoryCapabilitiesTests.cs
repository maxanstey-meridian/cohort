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
    public void Capabilities_With_The_Same_Strategies_Are_Equal_Regardless_Of_Order()
    {
        var first = new RetentionCategoryCapabilities([Strategy.Purge, Strategy.Anonymise]);
        var second = new RetentionCategoryCapabilities([Strategy.Anonymise, Strategy.Purge, Strategy.Purge]);

        first.Should().Be(second);
        first.GetHashCode().Should().Be(second.GetHashCode());
        first.Should().NotBe(new RetentionCategoryCapabilities([Strategy.Purge]));
    }
}
