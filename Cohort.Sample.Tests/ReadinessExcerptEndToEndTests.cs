using System.Reflection;
using Cohort.Infrastructure;
using Npgsql;
using NpgsqlTypes;

namespace Cohort.Sample.Tests;

// ─── End-to-end test (copied from PurgeSweepStrategyTests) ──────────────────
//
// Readiness quotes the part of a refused statement that PostgreSQL's error position points at.
// The position indexes the text the server received: "EXPLAIN " plus the statement with each
// @name rewritten to $n by Npgsql, whose rewriter is internal, so ReadinessExcerpt mirrors it.
// The unit rows (ReadinessExcerptTests) pin the mirror to the author's belief about Npgsql; this
// pins it to Npgsql. Each statement names a column that doesn't exist, right after a part that
// tests the rewriter (a longer name that starts with a shorter one, a repeated name, an @name in
// a quoted identifier, a literal, a comment, or a dollar quote, a cast, a name not in the
// collection). The statement goes to a real server as EXPLAIN, as readiness sends it, with a typed
// NULL for each named parameter. The test then reads the text Npgsql actually sent (the command's
// FinalCommandText, an internal member, by reflection), checks that the error position points at
// the bad column in it, and cuts the expected excerpt from that text: the 30 characters either
// side, whitespace collapsed. ReadinessExcerpt.At, which rewrites the statement itself, must give
// the same excerpt, so any difference from Npgsql in the numbering, the skipping, or the matching
// moves the excerpt.
// ────────────────────────────────────────────────────────────────────────────

[Collection("Integration")]
public sealed class ReadinessExcerptEndToEndTests(PostgresFixture fixture)
{
    private const string BadColumn = "nosuchcolumn";
    private const string Prefix = "EXPLAIN ";
    private const int Characters = 30;

    // @entity1 and @entity10 in one text: the longer name is read whole, not as @entity1 and "0".
    [Theory]
    [InlineData(
        "SELECT 1 FROM (SELECT 1 AS a, 2 AS b) AS t WHERE a = @entity1 AND b = @entity10 AND nosuchcolumn = 3",
        "entity1,entity10"
    )]
    // A repeated name reuses its number, and the next new name takes the next one. The names are
    // collected out of order, so numbering by collection order would swap $1 and $2.
    [InlineData(
        "SELECT 1 FROM (SELECT 1 AS a, 2 AS b) AS t WHERE a = @tenantId AND b = @other AND a <> @tenantId AND nosuchcolumn = 4",
        "other,tenantId"
    )]
    // An @ inside a quoted identifier is part of the identifier.
    [InlineData(
        "SELECT @name AS some_long_alias, \"a@name\", nosuchcolumn FROM (SELECT 1 AS \"a@name\") AS t",
        "name"
    )]
    // An @ inside a single-quoted literal is text, not a parameter.
    [InlineData(
        "SELECT @name AS x, 'literal with @name' AS y, nosuchcolumn FROM (SELECT 1) AS t",
        "name"
    )]
    // An @ inside a line comment is text, not a parameter.
    [InlineData(
        "SELECT @name AS xy -- comment with @name\n, nosuchcolumn FROM (SELECT 1) AS t",
        "name"
    )]
    // An @ inside a block comment is text, not a parameter.
    [InlineData(
        "SELECT @name AS x /* comment with @name */, nosuchcolumn FROM (SELECT 1) AS t",
        "name"
    )]
    // An @ inside a dollar-quoted body is text, not a parameter.
    [InlineData(
        "SELECT @name AS x, $$ body with @name $$ AS y, nosuchcolumn FROM (SELECT 1) AS t",
        "name"
    )]
    // 0::bigint is a cast, not a parameter, even when a parameter is named bigint.
    [InlineData(
        "SELECT 0::bigint AS a, @x AS b, 0::bigint AS c, nosuchcolumn FROM (SELECT 1) AS t",
        "x,bigint"
    )]
    // A name that isn't in the collection stays as written (here PostgreSQL reads @missing as the
    // absolute value of a column) and takes no number.
    [InlineData(
        "SELECT @missing AS a, @x AS b, nosuchcolumn FROM (SELECT -1 AS missing) AS t",
        "x"
    )]
    public async Task The_excerpt_is_centred_on_the_position_a_real_server_reports(
        string sql,
        string names
    )
    {
        var parameterNames = names.Split(',');
        var (refusal, sent) = await RefusalAsync(sql, parameterNames);

        sent.Should().StartWith(Prefix);
        var statement = sent[Prefix.Length..];
        var at = refusal.Position - 1 - Prefix.Length;
        statement[at..].Should().StartWith(BadColumn, "the position is into the text Npgsql sent");
        var expected = string.Join(
            ' ',
            statement[Math.Max(0, at - Characters)..Math.Min(statement.Length, at + Characters)]
                .Split((char[]?)null, StringSplitOptions.RemoveEmptyEntries)
        );

        ReadinessExcerpt.At(sql, parameterNames, refusal.Position).Should().Be(expected);
    }

    // The statement as EXPLAIN, with a typed NULL for each parameter (as readiness binds them: the
    // statement is analysed, never run), the refusal for its bad column, and the text Npgsql sent.
    private async Task<(PostgresException Refusal, string Sent)> RefusalAsync(
        string sql,
        string[] parameterNames
    )
    {
        await using var connection = new NpgsqlConnection(fixture.ConnectionString);
        await connection.OpenAsync();
        await using var command = new NpgsqlCommand($"{Prefix}{sql}", connection);
        foreach (var name in parameterNames)
        {
            command.Parameters.Add(
                new NpgsqlParameter(name, NpgsqlDbType.Integer) { Value = DBNull.Value }
            );
        }

        var act = () => command.ExecuteNonQueryAsync();

        var refusal = (await act.Should().ThrowAsync<PostgresException>()).Which;
        refusal.SqlState.Should().Be(PostgresErrorCodes.UndefinedColumn);
        refusal.Message.Should().Contain(BadColumn);
        return (refusal, SentText(command));
    }

    // The statement text after Npgsql's rewrite, which it keeps on the command's one batch
    // command in an internal property.
    private static string SentText(NpgsqlCommand command)
    {
        var property = typeof(NpgsqlBatchCommand).GetProperty(
            "FinalCommandText",
            BindingFlags.Instance | BindingFlags.NonPublic | BindingFlags.Public
        );
        property.Should().NotBeNull("Npgsql keeps the text it sent on the batch command");
#pragma warning disable CS0618 // Statements is the only public way to the command's batch commands.
        var statements = command.Statements;
#pragma warning restore CS0618
        var sent = property!.GetValue(statements.Should().ContainSingle().Which) as string;
        sent.Should().NotBeNullOrEmpty();
        return sent!;
    }
}
