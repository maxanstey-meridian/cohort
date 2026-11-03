using Cohort.Infrastructure;
using Npgsql;
using NpgsqlTypes;

namespace Cohort.Tests;

// Pattern #2 (pure unit test): ReadinessParameters.From is a static, I/O-free conversion from a
// statement's SqlParams to the parameters readiness sends. Typing matters here and nowhere else:
// a typed NULL keeps an uncast `text = uuid` refused without casting any value, and an untyped
// parameter lets PostgreSQL infer the column's type, so such a comparison would pass.
public sealed class ReadinessParametersTests
{
    [Fact]
    public void A_value_becomes_a_nullable_typed_parameter_with_its_name_and_value()
    {
        var tenant = Guid.NewGuid();

        var parameter = Single(new SqlParams { ["tenantId"] = tenant });

        parameter.Should().BeOfType<NpgsqlParameter<Guid?>>();
        parameter.ParameterName.Should().Be("tenantId");
        parameter.Value.Should().Be(tenant);
    }

    [Fact]
    public void A_typed_null_becomes_a_null_parameter_of_its_own_type()
    {
        var parameter = Single(
            new SqlParams { ["refusedIds"] = new ReadinessTypedNull(typeof(string[])) }
        );

        parameter.Should().BeOfType<NpgsqlParameter<string[]>>();
        parameter.ParameterName.Should().Be("refusedIds");
        parameter.Value.Should().BeNull();
    }

    [Fact]
    public void A_typed_null_of_a_value_type_is_typed_by_its_nullable_form()
    {
        var parameter = Single(
            new SqlParams { ["subject"] = new ReadinessTypedNull(typeof(Guid)) }
        );

        parameter.Should().BeOfType<NpgsqlParameter<Guid?>>();
        parameter.Value.Should().BeNull();
    }

    [Fact]
    public void A_plain_null_stays_untyped_as_the_run_binds_it()
    {
        var parameter = Single(new SqlParams { ["value0"] = null });

        parameter.GetType().Should().Be(typeof(NpgsqlParameter));
        parameter.ParameterName.Should().Be("value0");
        parameter.Value.Should().Be(DBNull.Value);
        parameter.NpgsqlDbType.Should().Be(NpgsqlDbType.Unknown);
    }

    [Fact]
    public void A_date_time_placeholder_binds_as_timestamptz()
    {
        var parameter = Single(
            new SqlParams { ["value0"] = new ReadinessTypedNull(typeof(DateTime)) }
        );

        parameter.Should().BeOfType<NpgsqlParameter<DateTime?>>();
        parameter.NpgsqlDbType.Should().Be(NpgsqlDbType.TimestampTz);
        parameter.Value.Should().BeNull();
    }

    [Fact]
    public void Every_entry_becomes_a_parameter_in_order_under_its_own_name()
    {
        var parameters = ReadinessParameters.From(
            new SqlParams
            {
                ["entity1"] = Guid.Empty,
                ["limit"] = 1,
                ["candidateIds"] = Array.Empty<string>(),
            }
        );

        parameters
            .Select(parameter => parameter.ParameterName)
            .Should()
            .Equal("entity1", "limit", "candidateIds");
        parameters[1].Should().BeOfType<NpgsqlParameter<int?>>();
        parameters[2].Should().BeOfType<NpgsqlParameter<string[]>>();
    }

    private static NpgsqlParameter Single(SqlParams parameters) =>
        ReadinessParameters.From(parameters).Single();
}
