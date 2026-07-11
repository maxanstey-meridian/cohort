using Cohort.Domain;

namespace Cohort.Tests;

public sealed class RetentionAttributeTests
{
    [Fact]
    public void Retention_Entity_Id_Requires_A_NonEmpty_Uuid()
    {
        var act = () => new RetentionEntityIdAttribute(Guid.Empty.ToString());

        act.Should().Throw<ArgumentException>().WithMessage("*non-empty UUID*");
    }

    [Fact]
    public void Retention_Entity_Id_Exposes_The_Parsed_Uuid()
    {
        var id = Guid.Parse("7cbdfbd2-d19d-496a-ab95-f29266e79e29");

        new RetentionEntityIdAttribute(id.ToString()).Id.Should().Be(id);
    }

    [Fact]
    public void Retain_Attribute_Remains_Class_Targeted_Single_Instance_And_Not_Inherited()
    {
        var usage = typeof(RetainAttribute)
            .GetCustomAttributes(typeof(AttributeUsageAttribute), inherit: false)
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

    [Theory]
    [InlineData(null)]
    [InlineData("")]
    [InlineData(" ")]
    public void Retain_Attribute_Rejects_A_Blank_Category(string? category)
    {
        var act = () => new RetainAttribute(category!, nameof(SampleEntity.CreatedAt));

        act.Should().Throw<ArgumentException>().WithParameterName("category");
    }

    [Theory]
    [InlineData(null)]
    [InlineData("")]
    [InlineData(" ")]
    public void Retain_Attribute_Rejects_A_Blank_Anchor_Member(string? anchorMember)
    {
        var act = () => new RetainAttribute("short-lived", anchorMember!);

        act.Should().Throw<ArgumentException>().WithParameterName("anchorMember");
    }

    [Fact]
    public void Retention_Anonymised_At_Is_Property_Targeted_Single_Instance_And_Not_Inherited()
    {
        var usage = typeof(RetentionAnonymisedAtAttribute)
            .GetCustomAttributes(typeof(AttributeUsageAttribute), inherit: false)
            .Cast<AttributeUsageAttribute>()
            .Single();

        usage.ValidOn.Should().Be(AttributeTargets.Property);
        usage.AllowMultiple.Should().BeFalse();
        usage.Inherited.Should().BeFalse();
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
    public void ErasureSubject_Attribute_Is_Property_Targeted_Multi_Instance_And_Not_Inherited()
    {
        var usage = typeof(ErasureSubjectAttribute)
            .GetCustomAttributes(typeof(AttributeUsageAttribute), inherit: false)
            .Cast<AttributeUsageAttribute>()
            .Single();

        usage.ValidOn.Should().Be(AttributeTargets.Property);
        usage.AllowMultiple.Should().BeTrue();
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

    [Theory]
    [InlineData(AnonymiseMethod.Null)]
    [InlineData(AnonymiseMethod.EmptyString)]
    public void Anonymise_Attribute_Rejects_A_Literal_For_Non_Fixed_Methods(AnonymiseMethod method)
    {
        var act = () => new AnonymiseAttribute(method, "[redacted]");

        act.Should().Throw<ArgumentException>().WithParameterName("literal");
    }

    [Fact]
    public void Anonymise_Attribute_Rejects_An_Undefined_Method()
    {
        var act = () => new AnonymiseAttribute((AnonymiseMethod)99);

        act.Should().Throw<ArgumentOutOfRangeException>().WithParameterName("method");
    }

    [Fact]
    public void Retain_Attribute_Rejects_An_Undefined_Audit_Row_Detail()
    {
        var act = () =>
            new RetainAttribute("short-lived", nameof(SampleEntity.CreatedAt))
            {
                AuditRowDetail = (AuditRowDetail)99,
            };

        act.Should().Throw<ArgumentOutOfRangeException>().WithParameterName("value");
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
        var attribute = new ExemptFromRetentionAttribute(
            "Statutory record outside retention sweep."
        );

        attribute.Reason.Should().Be("Statutory record outside retention sweep.");
    }

    [Theory]
    [InlineData(null)]
    [InlineData("")]
    [InlineData(" ")]
    [InlineData("   ")]
    public void Exempt_From_Retention_Attribute_Rejects_A_Blank_Reason(string? reason)
    {
        var act = () => new ExemptFromRetentionAttribute(reason!);

        act.Should().Throw<ArgumentException>().WithParameterName("reason");
    }

    private sealed class SampleEntity
    {
        public DateTimeOffset CreatedAt { get; init; }
    }
}
