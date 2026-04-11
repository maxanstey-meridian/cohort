using Cohort.Domain;

namespace Cohort.Tests;

public sealed class RetentionAttributeTests
{
    [Fact]
    public void Retain_Attribute_Remains_Class_Targeted_Single_Instance_And_Not_Inherited()
    {
        var usage = typeof(RetainAttribute).GetCustomAttributes(typeof(AttributeUsageAttribute), inherit: false)
            .Cast<AttributeUsageAttribute>()
            .Single();

        usage.ValidOn.Should().Be(AttributeTargets.Class);
        usage.AllowMultiple.Should().BeFalse();
        usage.Inherited.Should().BeFalse();
    }

    [Fact]
    public void Retain_Attribute_Preserves_Category_And_Anchor_Member()
    {
        var attribute = new RetainAttribute("short-lived", nameof(SampleEntity.CreatedAt));

        attribute.Category.Should().Be("short-lived");
        attribute.AnchorMember.Should().Be(nameof(SampleEntity.CreatedAt));
    }

    [Fact]
    public void Anonymise_Attribute_Is_Property_Targeted_Single_Instance_And_Not_Inherited()
    {
        var usage = typeof(AnonymiseAttribute)
            .GetCustomAttributes(typeof(AttributeUsageAttribute), inherit: false)
            .Cast<AttributeUsageAttribute>()
            .Single();

        usage.ValidOn.Should().Be(AttributeTargets.Property);
        usage.AllowMultiple.Should().BeFalse();
        usage.Inherited.Should().BeFalse();
    }

    [Fact]
    public void Anonymise_Attribute_Preserves_Method_And_Literal()
    {
        var attribute = new AnonymiseAttribute(AnonymiseMethod.FixedLiteral, "[redacted]");

        attribute.Method.Should().Be(AnonymiseMethod.FixedLiteral);
        attribute.Literal.Should().Be("[redacted]");
    }

    [Theory]
    [InlineData(null)]
    [InlineData("")]
    [InlineData(" ")]
    [InlineData("   ")]
    public void Anonymise_Attribute_Rejects_Fixed_Literal_Without_A_Literal_Value(string? literal)
    {
        var act = () => new AnonymiseAttribute(AnonymiseMethod.FixedLiteral, literal);

        act.Should().Throw<ArgumentException>().WithParameterName("literal");
    }

    [Fact]
    public void Anonymise_Attribute_Does_Not_Require_A_Literal_For_Non_Fixed_Methods()
    {
        var attribute = new AnonymiseAttribute(AnonymiseMethod.Null);

        attribute.Method.Should().Be(AnonymiseMethod.Null);
        attribute.Literal.Should().BeNull();
    }

    [Fact]
    public void Exempt_From_Retention_Attribute_Is_Class_Targeted_Single_Instance_And_Not_Inherited()
    {
        var usage = typeof(ExemptFromRetentionAttribute)
            .GetCustomAttributes(typeof(AttributeUsageAttribute), inherit: false)
            .Cast<AttributeUsageAttribute>()
            .Single();

        usage.ValidOn.Should().Be(AttributeTargets.Class);
        usage.AllowMultiple.Should().BeFalse();
        usage.Inherited.Should().BeFalse();
    }

    [Fact]
    public void Exempt_From_Retention_Attribute_Preserves_Reason()
    {
        var attribute = new ExemptFromRetentionAttribute("Statutory record outside retention sweep.");

        attribute.Reason.Should().Be("Statutory record outside retention sweep.");
    }

    private sealed class SampleEntity
    {
        public DateTimeOffset CreatedAt { get; init; }
    }
}
