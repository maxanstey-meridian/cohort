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
}
