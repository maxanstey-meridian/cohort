using Cohort.Infrastructure;

namespace Cohort.Tests;

// Pattern #2 (pure unit test): ReadinessExcerpt.At is a static, I/O-free function. PostgreSQL's
// error position indexes the text the server received: "EXPLAIN " plus the statement with each
// @name rewritten to $n by Npgsql, numbered by first appearance. Each row gives the statement, the
// parameter names, the text Npgsql sends (written out here, not derived), and the token the
// position points at. The excerpt is 30 characters either side of that position, so a rewrite
// that shifts the text by even one character shows as a different excerpt.
public sealed class ReadinessExcerptTests
{
    private const string Prefix = "EXPLAIN ";

    [Theory]
    // @entity1 and @entity10 in one text: the longer name is read whole, not as @entity1 and "0".
    [InlineData(
        "SELECT * FROM t WHERE a = @entity1 AND b = @entity10 AND c = BAD AND d = 6 AND e = 7 AND f = 8",
        "entity1,entity10",
        "SELECT * FROM t WHERE a = $1 AND b = $2 AND c = BAD AND d = 6 AND e = 7 AND f = 8",
        "BAD",
        "ERE a = $1 AND b = $2 AND c = BAD AND d = 6 AND e = 7 AND f"
    )]
    // A repeated name reuses its number, and the next new name takes the next one.
    [InlineData(
        "SELECT * FROM t WHERE a = @tenantId AND b = @other AND c = @tenantId AND d = BAD AND e = 5 AND f = 6 AND g = 7",
        "other,tenantId",
        "SELECT * FROM t WHERE a = $1 AND b = $2 AND c = $1 AND d = BAD AND e = 5 AND f = 6 AND g = 7",
        "BAD",
        "AND b = $2 AND c = $1 AND d = BAD AND e = 5 AND f = 6 AND g"
    )]
    // An @ inside a quoted identifier is part of the identifier.
    // (@name is 5 characters and $1 is 2, so a rewrite inside the quotes shifts the window.)
    [InlineData(
        "SELECT \"a@name\" FROM t WHERE b = BAD AND c = @name AND d = 4 AND e = 5",
        "name",
        "SELECT \"a@name\" FROM t WHERE b = BAD AND c = $1 AND d = 4 AND e = 5",
        "BAD",
        "ECT \"a@name\" FROM t WHERE b = BAD AND c = $1 AND d = 4 AND e"
    )]
    // An @ inside a single-quoted literal is text, not a parameter.
    [InlineData(
        "SELECT 'x@name' FROM t WHERE b = BAD AND c = @name AND d = 4 AND e = 5",
        "name",
        "SELECT 'x@name' FROM t WHERE b = BAD AND c = $1 AND d = 4 AND e = 5",
        "BAD",
        "ECT 'x@name' FROM t WHERE b = BAD AND c = $1 AND d = 4 AND e"
    )]
    // An @ inside a line comment is text, not a parameter.
    [InlineData(
        "SELECT a -- @name\nFROM t WHERE b = BAD AND c = @name AND d = 4 AND e = 5",
        "name",
        "SELECT a -- @name\nFROM t WHERE b = BAD AND c = $1 AND d = 4 AND e = 5",
        "BAD",
        "T a -- @name FROM t WHERE b = BAD AND c = $1 AND d = 4 AND e"
    )]
    // An @ inside a block comment is text, not a parameter.
    [InlineData(
        "SELECT a /* @name */ FROM t WHERE b = BAD AND c = @name AND d = 4 AND e = 5",
        "name",
        "SELECT a /* @name */ FROM t WHERE b = BAD AND c = $1 AND d = 4 AND e = 5",
        "BAD",
        "/* @name */ FROM t WHERE b = BAD AND c = $1 AND d = 4 AND e"
    )]
    // An @ inside a dollar-quoted body is text, not a parameter.
    [InlineData(
        "SELECT $$ @name $$ FROM t WHERE b = BAD AND c = @name AND d = 4 AND e = 5",
        "name",
        "SELECT $$ @name $$ FROM t WHERE b = BAD AND c = $1 AND d = 4 AND e = 5",
        "BAD",
        "$$ @name $$ FROM t WHERE b = BAD AND c = $1 AND d = 4 AND e"
    )]
    // 0::bigint is a cast, not a parameter, even when a parameter is named bigint.
    [InlineData(
        "SELECT 0::bigint AS a, @x AS b, c, d, e, f, g, h, i, j, k, BAD, l, m, n, o, p, q, r, s, t, u",
        "x,bigint",
        "SELECT 0::bigint AS a, $1 AS b, c, d, e, f, g, h, i, j, k, BAD, l, m, n, o, p, q, r, s, t, u",
        "BAD",
        "b, c, d, e, f, g, h, i, j, k, BAD, l, m, n, o, p, q, r, s, t"
    )]
    // A name that isn't in the collection stays as written and takes no number.
    [InlineData(
        "SELECT * FROM t WHERE a = @missing AND b = @x AND c = BAD AND d = 4 AND e = 5 AND f = 6",
        "x",
        "SELECT * FROM t WHERE a = @missing AND b = $1 AND c = BAD AND d = 4 AND e = 5 AND f = 6",
        "BAD",
        "= @missing AND b = $1 AND c = BAD AND d = 4 AND e = 5 AND f"
    )]
    // Position 1 past the prefix is the statement's first character.
    [InlineData(
        "SELECT * FROM t WHERE a = BAD AND b = 2 AND c = 3 AND d = 4 AND e = 5 AND f = 6",
        "",
        "SELECT * FROM t WHERE a = BAD AND b = 2 AND c = 3 AND d = 4 AND e = 5 AND f = 6",
        "SELECT",
        "SELECT * FROM t WHERE a = BAD"
    )]
    public void The_excerpt_is_cut_from_the_text_Npgsql_sends_around_the_position(
        string sql,
        string names,
        string sent,
        string marker,
        string expected
    )
    {
        var position = Prefix.Length + sent.IndexOf(marker, StringComparison.Ordinal) + 1;

        var excerpt = ReadinessExcerpt.At(sql, Names(names), position);

        excerpt.Should().Be(expected);
    }

    [Theory]
    [InlineData(0)]
    [InlineData(1)]
    [InlineData(8)]
    public void A_position_inside_the_prefix_gives_no_excerpt(int position)
    {
        ReadinessExcerpt.At("SELECT 1", Names(""), position).Should().BeEmpty();
    }

    [Fact]
    public void A_position_past_the_end_of_the_statement_gives_no_excerpt()
    {
        const string sql = "SELECT 1";

        ReadinessExcerpt.At(sql, Names(""), Prefix.Length + sql.Length + 1).Should().BeEmpty();
    }

    private static string[] Names(string names) => names.Length == 0 ? [] : names.Split(',');
}
