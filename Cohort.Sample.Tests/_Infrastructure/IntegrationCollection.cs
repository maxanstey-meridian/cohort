namespace Cohort.Sample.Tests;

[CollectionDefinition("Integration", DisableParallelization = true)]
public sealed class IntegrationCollection : ICollectionFixture<PostgresFixture>;
