using System.Reflection;

using Cohort.Application;

namespace Cohort.Tests;

public sealed class ReflectionMemberResolverTests
{
    [Fact]
    public void Direct_GetProperty_Throws_Ambiguous_Match_For_New_Shadowed_Generic_Property()
    {
        // Baseline: confirms the .NET reflection behaviour Cohort's old code path crashed on.
        // IdentityUser<TKey>-style: base has `public virtual TKey Id` with a non-sealed type
        // parameter, derived uses `new Guid Id`. Reflection sees both and fails to pick one.
        var clrType = typeof(DerivedWithShadowedGenericId);

        var act = () => clrType.GetProperty("Id", BindingFlags.Public | BindingFlags.Instance);

        act.Should().Throw<AmbiguousMatchException>();
    }

    [Fact]
    public void FindDeclaredOrInherited_Returns_Most_Derived_Property_When_Name_Is_Shadowed()
    {
        var clrType = typeof(DerivedWithShadowedGenericId);

        var resolved = ReflectionMemberResolver.FindPropertyByName(clrType, "Id");

        resolved.Should().NotBeNull();
        resolved!.DeclaringType.Should().Be<DerivedWithShadowedGenericId>();
        resolved.PropertyType.Should().Be<Guid>();
    }

    [Fact]
    public void FindDeclaredOrInherited_Returns_Inherited_Property_When_Not_Shadowed()
    {
        var clrType = typeof(DerivedWithoutShadow);

        var resolved = ReflectionMemberResolver.FindPropertyByName(clrType, "CreatedAt");

        resolved.Should().NotBeNull();
        resolved!.DeclaringType.Should().Be<BaseEntity>();
    }

    [Fact]
    public void FindDeclaredOrInherited_Returns_Null_When_Property_Does_Not_Exist()
    {
        var resolved = ReflectionMemberResolver.FindPropertyByName(
            typeof(DerivedWithoutShadow),
            "DoesNotExist"
        );

        resolved.Should().BeNull();
    }

    [Fact]
    public void FindDeclaredOrInherited_Handles_Non_Generic_Shadow()
    {
        // String shadow: e.g. IdentityUser.Email (string?) → ApplicationUser uses `new string? Email`.
        var clrType = typeof(DerivedWithShadowedStringProperty);

        var resolved = ReflectionMemberResolver.FindPropertyByName(clrType, "Label");

        resolved.Should().NotBeNull();
        resolved!.DeclaringType.Should().Be<DerivedWithShadowedStringProperty>();
    }

    private abstract class GenericBase<TKey>
    {
        public virtual TKey Id { get; set; } = default!;
    }

    private sealed class DerivedWithShadowedGenericId : GenericBase<Guid>
    {
        public new Guid Id { get => base.Id; set => base.Id = value; }
    }

    private class BaseEntity
    {
        public DateTimeOffset CreatedAt { get; init; }
    }

    private sealed class DerivedWithoutShadow : BaseEntity;

    private class BaseWithStringProperty
    {
        public virtual string? Label { get; set; }
    }

    private sealed class DerivedWithShadowedStringProperty : BaseWithStringProperty
    {
        public new string? Label { get => base.Label; set => base.Label = value; }
    }
}
