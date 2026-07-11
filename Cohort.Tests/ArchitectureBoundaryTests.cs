using System.Reflection;
using System.Reflection.Emit;
using System.Xml.Linq;

namespace Cohort.Tests;

public sealed class ArchitectureBoundaryTests
{
    private static readonly IReadOnlyDictionary<ushort, OpCode> _opCodesByValue = typeof(OpCodes)
        .GetFields(BindingFlags.Public | BindingFlags.Static)
        .Where(field => field.FieldType == typeof(OpCode))
        .Select(field => (OpCode)field.GetValue(null)!)
        .ToDictionary(opCode => unchecked((ushort)opCode.Value));

    private static readonly IReadOnlyDictionary<string, string[]> _allowedProductionDependencies =
        new Dictionary<string, string[]>
        {
            ["Cohort.Domain"] = ["Cohort.Domain"],
            ["Cohort.Application"] = ["Cohort.Application", "Cohort.Domain"],
            ["Cohort.Infrastructure"] =
            [
                "Cohort.Infrastructure",
                "Cohort.Application",
                "Cohort.Domain",
            ],
            ["Cohort.Hosting"] =
            [
                "Cohort.Hosting",
                "Cohort.Infrastructure",
                "Cohort.Application",
                "Cohort.Domain",
            ],
        };

    private static readonly IReadOnlyDictionary<string, string[]> _allowedExternalAssemblies =
        new Dictionary<string, string[]>
        {
            ["Cohort.Domain"] =
            [
                "System.Collections",
                "System.ObjectModel",
                "System.Private.CoreLib",
            ],
            ["Cohort.Application"] =
            [
                "System.Collections",
                "System.Linq",
                "System.Private.CoreLib",
            ],
            ["Cohort.Infrastructure"] =
            [
                "Microsoft.EntityFrameworkCore",
                "Microsoft.EntityFrameworkCore.Relational",
                "Microsoft.Extensions.DependencyInjection.Abstractions",
                "Microsoft.Extensions.Hosting.Abstractions",
                "Microsoft.Extensions.Logging.Abstractions",
                "System.Collections",
                "System.Collections.Concurrent",
                "System.Collections.Immutable",
                "System.ComponentModel",
                "System.Data.Common",
                "System.Linq",
                "System.Linq.Expressions",
                "System.ObjectModel",
                "System.Private.CoreLib",
                "System.Text.Json",
                "System.Text.RegularExpressions",
            ],
            ["Cohort.Hosting"] =
            [
                "Cronos",
                "Microsoft.EntityFrameworkCore",
                "Microsoft.EntityFrameworkCore.Relational",
                "Microsoft.Extensions.DependencyInjection.Abstractions",
                "Microsoft.Extensions.Hosting.Abstractions",
                "Microsoft.Extensions.Logging.Abstractions",
                "Microsoft.Extensions.Options",
                "System.ComponentModel",
                "System.Data.Common",
                "System.Private.CoreLib",
            ],
        };

    [Fact]
    public void Production_Types_Belong_To_A_Recognized_Layer_Namespace()
    {
        var violations = typeof(Cohort.Domain.RetentionRule)
            .Assembly.GetTypes()
            .Where(IsProductionType)
            .Where(type => GetLayer(type) is null)
            .Select(type => type.FullName)
            .Order(StringComparer.Ordinal)
            .ToArray();

        violations
            .Should()
            .BeEmpty("every production type must have an explicit architectural layer");
    }

    [Fact]
    public void Production_Layers_Only_Expose_Allowed_Compiled_Type_Dependencies()
    {
        var violations = typeof(Cohort.Domain.RetentionRule)
            .Assembly.GetTypes()
            .Where(type => GetLayer(type) is not null)
            .SelectMany(type =>
                GetDeclaredDependencies(type)
                    .Where(dependency => IsForbiddenDependency(type, dependency))
                    .Select(dependency => $"{type.FullName} -> {dependency.FullName}")
            )
            .Distinct(StringComparer.Ordinal)
            .Order(StringComparer.Ordinal)
            .ToArray();

        violations
            .Should()
            .BeEmpty("compiled production type metadata must follow the layer dependency rule");
    }

    [Fact]
    public void Domain_And_Application_Method_Bodies_Have_No_Forbidden_Dependencies()
    {
        var violations = GetMethodBodyDependencies(typeof(Cohort.Domain.RetentionRule).Assembly)
            .Where(dependency => IsForbiddenMethodBodyDependency(dependency.Source, dependency.Target))
            .Select(dependency => $"{dependency.Source.FullName} -> {dependency.Target.FullName}")
            .Distinct(StringComparer.Ordinal)
            .Order(StringComparer.Ordinal)
            .ToArray();

        violations
            .Should()
            .BeEmpty("compiled Domain and Application method bodies must follow the layer dependency rule");
    }

    [Fact]
    public void Compiled_Method_Body_Check_Detects_A_Forbidden_Member_Dependency()
    {
        var violations = GetMethodBodyDependencies(typeof(ArchitectureBoundaryTests).Assembly)
            .Where(dependency => dependency.Source == typeof(MethodBodyProofFixture))
            .Where(dependency => dependency.Target == typeof(ForbiddenProofDependency))
            .ToArray();

        violations.Should().ContainSingle();
    }

    [Fact]
    public void Compiled_Method_Body_Check_Includes_Static_Constructors()
    {
        var violations = GetMethodBodyDependencies(typeof(ArchitectureBoundaryTests).Assembly)
            .Where(dependency => dependency.Source == typeof(StaticConstructorProofFixture))
            .Where(dependency => dependency.Target == typeof(ForbiddenProofDependency))
            .ToArray();

        violations.Should().NotBeEmpty();
    }

    [Fact]
    public void Declared_Dependency_Check_Detects_A_Forbidden_External_Framework_Dependency()
    {
        GetDeclaredDependencies(typeof(DeclaredDependencyProofFixture))
            .Where(dependency => IsForbiddenDependency("Cohort.Domain", dependency))
            .Should()
            .Contain(typeof(Microsoft.Extensions.Hosting.IHostedService));
    }

    [Fact]
    public void Declared_Dependency_Check_Detects_A_Forbidden_Third_Party_Dependency()
    {
        GetDeclaredDependencies(typeof(ThirdPartyDependencyProofFixture))
            .Where(dependency => IsForbiddenDependency("Cohort.Application", dependency))
            .Should()
            .Contain(typeof(Cronos.CronExpression));
    }

    [Fact]
    public void Declared_Dependency_Check_Fails_Closed_When_A_Layer_Has_No_External_Allowlist()
    {
        IsForbiddenDependency("Cohort.Unlisted", typeof(string)).Should().BeTrue();
    }

    [Fact]
    public void Declared_Dependency_Check_Inspects_Type_Values_In_Custom_Attribute_Arguments()
    {
        GetDeclaredDependencies(typeof(AttributeDependencyProofFixture))
            .Should()
            .Contain([typeof(ForbiddenProofDependency), typeof(ThirdPartyDependencyProofFixture)]);
    }

    [Fact]
    public void Pure_Test_Project_Has_Only_Pure_Test_Packages_And_Production_Project_Reference()
    {
        var root = FindRepoRoot();
        var project = XDocument.Load(Path.Combine(root, "Cohort.Tests", "Cohort.Tests.csproj"));
        var packages = project
            .Descendants("PackageReference")
            .Select(element => (string?)element.Attribute("Include"))
            .Where(name => name is not null)
            .Cast<string>()
            .Order(StringComparer.Ordinal)
            .ToArray();
        var projects = project
            .Descendants("ProjectReference")
            .Select(element => ((string?)element.Attribute("Include"))?.Replace('\\', '/'))
            .Where(name => name is not null)
            .Cast<string>()
            .ToArray();

        packages
            .Should()
            .Equal(
                "FluentAssertions",
                "Microsoft.NET.Test.Sdk",
                "xunit",
                "xunit.runner.visualstudio"
            );
        projects.Should().Equal("../Cohort/Cohort.csproj");
    }

    private static IEnumerable<Type> GetDeclaredDependencies(Type type)
    {
        const BindingFlags flags =
            BindingFlags.Instance
            | BindingFlags.Static
            | BindingFlags.Public
            | BindingFlags.NonPublic
            | BindingFlags.DeclaredOnly;
        var dependencies = new List<Type>();

        AddType(type.BaseType, dependencies);
        foreach (var dependency in type.GetInterfaces())
        {
            AddType(dependency, dependencies);
        }
        foreach (var field in type.GetFields(flags))
        {
            AddType(field.FieldType, dependencies);
        }
        foreach (var property in type.GetProperties(flags))
        {
            AddType(property.PropertyType, dependencies);
        }
        foreach (
            var method in type.GetMethods(flags)
                .Cast<MethodBase>()
                .Concat(type.GetConstructors(flags))
        )
        {
            if (method is MethodInfo methodInfo)
            {
                AddType(methodInfo.ReturnType, dependencies);
                foreach (var argument in methodInfo.GetGenericArguments())
                {
                    foreach (var constraint in argument.GetGenericParameterConstraints())
                    {
                        AddType(constraint, dependencies);
                    }
                }
            }
            foreach (var parameter in method.GetParameters())
            {
                AddType(parameter.ParameterType, dependencies);
            }
        }
        foreach (var argument in type.GetGenericArguments())
        {
            foreach (var constraint in argument.GetGenericParameterConstraints())
            {
                AddType(constraint, dependencies);
            }
        }
        foreach (var attribute in type.CustomAttributes)
        {
            AddType(attribute.AttributeType, dependencies);
            foreach (var argument in attribute.ConstructorArguments)
            {
                AddAttributeArgumentTypes(argument, dependencies);
            }
            foreach (var argument in attribute.NamedArguments)
            {
                AddAttributeArgumentTypes(argument.TypedValue, dependencies);
            }
        }

        return dependencies;
    }

    private static void AddAttributeArgumentTypes(
        CustomAttributeTypedArgument argument,
        ICollection<Type> dependencies
    )
    {
        if (argument.Value is Type type)
        {
            AddType(type, dependencies);
        }
        else if (argument.Value is IEnumerable<CustomAttributeTypedArgument> arguments)
        {
            foreach (var nestedArgument in arguments)
            {
                AddAttributeArgumentTypes(nestedArgument, dependencies);
            }
        }
    }

    private static void AddType(Type? type, ICollection<Type> dependencies)
    {
        if (type is null || type.IsGenericParameter)
        {
            return;
        }

        if (type.HasElementType)
        {
            AddType(type.GetElementType(), dependencies);
        }

        if (type.IsGenericType)
        {
            if (!type.IsGenericTypeDefinition)
            {
                AddType(type.GetGenericTypeDefinition(), dependencies);
            }
            foreach (var argument in type.GetGenericArguments())
            {
                AddType(argument, dependencies);
            }
        }

        dependencies.Add(type);
    }

    private static IEnumerable<(Type Source, Type Target)> GetMethodBodyDependencies(
        Assembly assembly
    )
    {
        const BindingFlags flags =
            BindingFlags.Instance
            | BindingFlags.Static
            | BindingFlags.Public
            | BindingFlags.NonPublic
            | BindingFlags.DeclaredOnly;

        foreach (var type in assembly.GetTypes())
        {
            foreach (
                var method in type.GetMethods(flags)
                    .Cast<MethodBase>()
                    .Concat(type.GetConstructors(flags))
                    .Concat(type.TypeInitializer is null ? [] : [type.TypeInitializer])
            )
            {
                var body = method.GetMethodBody();
                if (body is null)
                {
                    continue;
                }

                foreach (var local in body.LocalVariables)
                {
                    foreach (var dependency in GetTypeDependencies(local.LocalType))
                    {
                        yield return (type, dependency);
                    }
                }
                foreach (var clause in body.ExceptionHandlingClauses)
                {
                    if (
                        clause.Flags == ExceptionHandlingClauseOptions.Clause
                        && clause.CatchType is not null
                    )
                    {
                        foreach (var dependency in GetTypeDependencies(clause.CatchType))
                        {
                            yield return (type, dependency);
                        }
                    }
                }
                foreach (var target in GetReferencedTypes(method, body))
                {
                    yield return (type, target);
                }
            }
        }
    }

    private static IEnumerable<Type> GetReferencedTypes(MethodBase method, MethodBody body)
    {
        var il = body.GetILAsByteArray()!;
        var position = 0;
        while (position < il.Length)
        {
            var value = il[position++];
            var opCodeValue = value == 0xfe ? (ushort)(0xfe00 | il[position++]) : value;
            var opCode = _opCodesByValue[opCodeValue];
            var operandSize = GetOperandSize(opCode.OperandType, il, position);

            if (
                opCode.OperandType
                is OperandType.InlineField
                    or OperandType.InlineMethod
                    or OperandType.InlineTok
                    or OperandType.InlineType
            )
            {
                var member = method.Module.ResolveMember(
                    BitConverter.ToInt32(il, position),
                    method.DeclaringType?.GetGenericArguments(),
                    method.IsGenericMethod ? method.GetGenericArguments() : null
                );
                foreach (var target in GetMemberDependencies(member))
                {
                    yield return target;
                }
            }

            position += operandSize;
        }
    }

    private static IEnumerable<Type> GetMemberDependencies(MemberInfo? member)
    {
        var dependencies = new List<Type>();

        switch (member)
        {
            case Type type:
                AddType(type, dependencies);
                break;
            case FieldInfo field:
                AddType(field.DeclaringType, dependencies);
                AddType(field.FieldType, dependencies);
                break;
            case MethodBase method:
                AddType(method.DeclaringType, dependencies);
                if (method is MethodInfo methodInfo)
                {
                    AddType(methodInfo.ReturnType, dependencies);
                }
                foreach (var parameter in method.GetParameters())
                {
                    AddType(parameter.ParameterType, dependencies);
                }
                if (method.IsGenericMethod)
                {
                    foreach (var argument in method.GetGenericArguments())
                    {
                        AddType(argument, dependencies);
                    }
                }
                break;
        }

        return dependencies;
    }

    private static IEnumerable<Type> GetTypeDependencies(Type type)
    {
        var dependencies = new List<Type>();
        AddType(type, dependencies);
        return dependencies;
    }

    private static int GetOperandSize(OperandType operandType, byte[] il, int position) =>
        operandType switch
        {
            OperandType.InlineNone => 0,
            OperandType.ShortInlineBrTarget
            or OperandType.ShortInlineI
            or OperandType.ShortInlineVar => 1,
            OperandType.InlineVar => 2,
            OperandType.InlineBrTarget
            or OperandType.InlineField
            or OperandType.InlineI
            or OperandType.InlineMethod
            or OperandType.InlineSig
            or OperandType.InlineString
            or OperandType.InlineTok
            or OperandType.InlineType
            or OperandType.ShortInlineR => 4,
            OperandType.InlineI8 or OperandType.InlineR => 8,
            OperandType.InlineSwitch => 4 + BitConverter.ToInt32(il, position) * 4,
            _ => throw new InvalidOperationException($"Unsupported IL operand type {operandType}."),
        };

    private static bool IsForbiddenMethodBodyDependency(Type source, Type target)
    {
        var sourceLayer = GetLayer(source);
        if (sourceLayer is not ("Cohort.Domain" or "Cohort.Application"))
        {
            return false;
        }

        return IsForbiddenDependency(sourceLayer, target);
    }

    private static bool IsForbiddenDependency(Type source, Type target) =>
        IsForbiddenDependency(GetLayer(source)!, target);

    private static bool IsForbiddenDependency(string sourceLayer, Type target)
    {
        var targetLayer = GetLayer(target);
        if (targetLayer is not null)
        {
            return !_allowedProductionDependencies[sourceLayer].Contains(targetLayer);
        }

        return !_allowedExternalAssemblies.TryGetValue(sourceLayer, out var allowedAssemblies)
            || !allowedAssemblies.Contains(target.Assembly.GetName().Name, StringComparer.Ordinal);
    }

    private static string? GetLayer(Type type) =>
        _allowedProductionDependencies.Keys.FirstOrDefault(layer =>
            type.Namespace == layer
            || type.Namespace?.StartsWith(layer + ".", StringComparison.Ordinal) == true
        );

    private static bool IsProductionType(Type type) =>
        !type.IsDefined(typeof(System.Runtime.CompilerServices.CompilerGeneratedAttribute), inherit: false)
        && !type.IsNestedPrivate
        && type.Namespace != "System.Text.RegularExpressions.Generated"
        && !type.Name.StartsWith("<PrivateImplementationDetails>", StringComparison.Ordinal);

    private static string FindRepoRoot()
    {
        var directory = new DirectoryInfo(AppContext.BaseDirectory);
        while (directory is not null)
        {
            if (File.Exists(Path.Combine(directory.FullName, "Cohort.slnx")))
            {
                return directory.FullName;
            }

            directory = directory.Parent;
        }

        throw new InvalidOperationException("Could not locate the repository root.");
    }

    private sealed class MethodBodyProofFixture
    {
        public static void InvokeForbiddenMember() => AllowedProofDependency.GetForbidden();
    }

    private sealed class DeclaredDependencyProofFixture : Microsoft.Extensions.Hosting.IHostedService
    {
        public Task StartAsync(CancellationToken cancellationToken) => Task.CompletedTask;

        public Task StopAsync(CancellationToken cancellationToken) => Task.CompletedTask;
    }

    private static class StaticConstructorProofFixture
    {
        public static object? Dependency { get; } = AllowedProofDependency.GetForbidden();
    }

    private sealed class ThirdPartyDependencyProofFixture
    {
        public Cronos.CronExpression? Expression { get; init; }
    }

    [AttributeDependencyProof(typeof(ForbiddenProofDependency), NamedTypes = [typeof(ThirdPartyDependencyProofFixture)])]
    private sealed class AttributeDependencyProofFixture
    {
    }

    [AttributeUsage(AttributeTargets.Class)]
    private sealed class AttributeDependencyProofAttribute(Type constructorType) : Attribute
    {
        public Type ConstructorType { get; } = constructorType;

        public Type[] NamedTypes { get; init; } = [];
    }

    private static class AllowedProofDependency
    {
        public static ForbiddenProofDependency? GetForbidden() => null;
    }

    private sealed class ForbiddenProofDependency
    {
    }
}
