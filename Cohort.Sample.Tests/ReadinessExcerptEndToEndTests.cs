using System.Text.RegularExpressions;
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
// collection). Each row also gives the text Npgsql should send, with $n written out. The
// statement goes to a real server twice as EXPLAIN, as readiness sends it: with named
// parameters, which Npgsql rewrites, and as the numbered text with positional ones, which it
// sends as written. Both refusals must report the same position (so the written-out text is what
// Npgsql sent), that position must point at the bad column in it, and the excerpt must equal the
// 30 characters either side, cut here by the plain rule. A rewrite that numbers or skips anything
// differently from Npgsql shifts the text, so the excerpt lands off centre.
// ────────────────────────────────────────────────────────────────────────────

[Collection("Integration")]
public sealed class ReadinessExcerptEndToEndTests(PostgresFixture fixture)
{
    private const string BadColumn = "nosuchcolumn";
    private const string Prefix = "EXPLAIN ";
    private const int Characters = 30;

    [Theory]
    // @entity1 and @entity10 in one text: the longer name is read whole, not as @entity1 and "0".
    [InlineData(
        "SELECT 1 FROM (SELECT 1 AS a, 2 AS b) AS t WHERE a = @entity1 AND b = @entity10 AND nosuchcolumn = 3",
        "entity1,entity10",
        "SELECT 1 FROM (SELECT 1 AS a, 2 AS b) AS t WHERE a = $1 AND b = $2 AND nosuchcolumn = 3"
    )]
    // A repeated name reuses its number, and the next new name takes the next one.
    [InlineData(
        "SELECT 1 FROM (SELECT 1 AS a, 2 AS b) AS t WHERE a = @tenantId AND b = @other AND a <> @tenantId AND nosuchcolumn = 4",
        "other,tenantId",
        "SELECT 1 FROM (SELECT 1 AS a, 2 AS b) AS t WHERE a = $1 AND b = $2 AND a <> $1 AND nosuchcolumn = 4"
    )]
    // An @ inside a quoted identifier is part of the identifier.
    [InlineData(
        "SELECT @name AS some_long_alias, \"a@name\", nosuchcolumn FROM (SELECT 1 AS \"a@name\") AS t",
        "name",
        "SELECT $1 AS some_long_alias, \"a@name\", nosuchcolumn FROM (SELECT 1 AS \"a@name\") AS t"
    )]
    // An @ inside a single-quoted literal is text, not a parameter.
    [InlineData(
        "SELECT @name AS x, 'literal with @name' AS y, nosuchcolumn FROM (SELECT 1) AS t",
        "name",
        "SELECT $1 AS x, 'literal with @name' AS y, nosuchcolumn FROM (SELECT 1) AS t"
    )]
    // An @ inside a line comment is text, not a parameter.
    [InlineData(
        "SELECT @name AS xy -- comment with @name\n, nosuchcolumn FROM (SELECT 1) AS t",
        "name",
        "SELECT $1 AS xy -- comment with @name\n, nosuchcolumn FROM (SELECT 1) AS t"
    )]
    // An @ inside a block comment is text, not a parameter.
    [InlineData(
        "SELECT @name AS x /* comment with @name */, nosuchcolumn FROM (SELECT 1) AS t",
        "name",
        "SELECT $1 AS x /* comment with @name */, nosuchcolumn FROM (SELECT 1) AS t"
    )]
    // An @ inside a dollar-quoted body is text, not a parameter.
    [InlineData(
        "SELECT @name AS x, $$ body with @name $$ AS y, nosuchcolumn FROM (SELECT 1) AS t",
        "name",
        "SELECT $1 AS x, $$ body with @name $$ AS y, nosuchcolumn FROM (SELECT 1) AS t"
    )]
    // 0::bigint is a cast, not a parameter, even when a parameter is named bigint.
    [InlineData(
        "SELECT 0::bigint AS a, @x AS b, 0::bigint AS c, nosuchcolumn FROM (SELECT 1) AS t",
        "x,bigint",
        "SELECT 0::bigint AS a, $1 AS b, 0::bigint AS c, nosuchcolumn FROM (SELECT 1) AS t"
    )]
    // A name that isn't in the collection stays as written (here PostgreSQL reads @missing as the
    // absolute value of a column) and takes no number.
    [InlineData(
        "SELECT @missing AS a, @x AS b, nosuchcolumn FROM (SELECT -1 AS missing) AS t",
        "x",
        "SELECT @missing AS a, $1 AS b, nosuchcolumn FROM (SELECT -1 AS missing) AS t"
    )]
    public async Task The_excerpt_is_centred_on_the_position_a_real_server_reports(
        string sql,
        string names,
        string numbered
    )
    {
        var parameterNames = names.Split(',');
        var named = await RefusalAsync(sql, parameterNames);
        var written = await RefusalAsync(
            numbered,
            Enumerable.Repeat("", NumbersUsedIn(numbered)).ToArray()
        );

        named.Position.Should().Be(written.Position, "Npgsql sends the written-out text");
        var statement = numbered;
        var at = written.Position - 1 - Prefix.Length;
        statement[at..].Should().StartWith(BadColumn);
        var expected = string.Join(
            ' ',
            statement[Math.Max(0, at - Characters)..Math.Min(statement.Length, at + Characters)]
                .Split((char[]?)null, StringSplitOptions.RemoveEmptyEntries)
        );

        ReadinessExcerpt.At(sql, parameterNames, named.Position).Should().Be(expected);
    }

    // The statement as EXPLAIN, with a typed NULL for each parameter (as readiness binds them: the
    // statement is analysed, never run), and the refusal for its bad column. A parameter with an
    // empty name is positional, so Npgsql sends the text as written.
    private async Task<PostgresException> RefusalAsync(string sql, string[] parameterNames)
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
        return refusal;
    }

    private static int NumbersUsedIn(string numbered) =>
        Regex.Matches(numbered, @"\$(\d+)").Select(match => int.Parse(match.Groups[1].Value)).Max();
}
