using System.Diagnostics;
using System.Globalization;
using System.IO.Compression;
using System.Reflection;
using System.Runtime.CompilerServices;
using System.Security.Cryptography;
using System.Xml.Linq;

using Cohort.Application;
using Cohort.Domain;

namespace Cohort.Tests;

public sealed class PackageReleaseContractTests
{
    private static readonly TimeSpan ProcessTimeout = TimeSpan.FromMinutes(2);
    private static readonly NullabilityInfoContext Nullability = new();
    private static readonly Lazy<PackedArtifact> Artifact = new(BuildPackedArtifact);
    private static readonly string GlobalPackagesFolder = GetGlobalPackagesFolder();

    [Fact]
    public void Packed_Package_Uses_A_Version_Greater_Than_The_Latest_Release()
    {
        Artifact.Value.PackageVersion.Should().BeGreaterThan(new Version(0, 5, 0));
    }

    [Fact]
    public void Publish_Workflow_Requires_The_Tag_To_Exactly_Match_The_Project_Version()
    {
        var gateScript = ExtractWorkflowStepScript("Validate tag matches package version");

        gateScript
            .Should()
            .Be(
                """
                tag_version="${GITHUB_REF_NAME#v}"
                project_version="$(dotnet msbuild Cohort/Cohort.csproj -getProperty:Version -nologo)"

                if [[ "$tag_version" != "$project_version" ]]; then
                  echo "Tag version '$tag_version' does not match Cohort.csproj Version '$project_version'." >&2
                  exit 1
                fi
                """
            );
        ExtractWorkflowStepScript("Pack").Should().NotContain("-p:Version=");
    }

    [Fact]
    public void Publish_Workflow_Fails_When_The_Package_Version_Already_Exists()
    {
        ExtractWorkflowStepScript("Push to NuGet")
            .Should()
            .Be(
                "dotnet nuget push ./nupkgs/*.nupkg --source https://api.nuget.org/v3/index.json --api-key ${{ secrets.NUGET_API_KEY }}"
            );
    }

    [Fact]
    public void Assembly_Exports_Only_The_Explicit_Package_API()
    {
        var actual = GetPublicApi(typeof(IRetentionSweep).Assembly);
        actual.Should().Equal(ReadExpectedPublicApi());
    }

    [Fact]
    public void Historical_Migration_Filenames_And_SHA256_Hashes_Match_The_Approved_Baseline()
    {
        var migrationsDirectory = Path.Combine(FindRepoRoot(), "Cohort.Sample", "Migrations");
        var actual = Directory
            .EnumerateFiles(migrationsDirectory, "*.cs", SearchOption.TopDirectoryOnly)
            .Where(path => Path.GetFileName(path) != "SampleDbContextModelSnapshot.cs")
            .Select(path =>
                $"{Path.GetFileName(path)} {Convert.ToHexString(SHA256.HashData(File.ReadAllBytes(path))).ToLowerInvariant()}"
            )
            .Order(StringComparer.Ordinal)
            .ToArray();
        var approved = File.ReadAllLines(
            Path.Combine(AppContext.BaseDirectory, "HistoricalMigrations.approved.txt")
        );

        actual.Should().Equal(approved);
    }

    // Contract policy:
    // - exported top-level and nested types are included;
    // - only members declared by each exported type are included, so inherited object/framework
    //   members cannot enter the package contract accidentally;
    // - public constructors, methods (including extension and default interface methods),
    //   properties, events, fields/constants, and enum members are included;
    // - property/event accessors are represented by their owning member, not duplicated as methods;
    // - public compiler-generated callable members are included because record equality, cloning,
    //   and deconstruction are real source/binary compatibility surface;
    // - nullable reference metadata, generic constraints, inheritance/interfaces, ref returns,
    //   and AttributeUsage are part of the contract;
    // - signatures use CLR type identity and ordinal sorting, independent of reflection ordering.
    private static string[] GetPublicApi(Assembly assembly)
    {
        const BindingFlags declaredPublic =
            BindingFlags.Public
            | BindingFlags.Instance
            | BindingFlags.Static
            | BindingFlags.DeclaredOnly;

        var api = new List<string>();
        foreach (
            var type in assembly
                .GetExportedTypes()
                .Where(member => !IsCompilerGenerated(member))
                .OrderBy(FormatType, StringComparer.Ordinal)
        )
        {
            api.Add(FormatTypeDeclaration(type));

            if (type.GetCustomAttribute<AttributeUsageAttribute>() is { } usage)
            {
                api.Add(
                    $"attributeusage {FormatType(type)} : {usage.ValidOn} [AllowMultiple={usage.AllowMultiple}, Inherited={usage.Inherited}]"
                );
            }

            api.AddRange(
                type.GetConstructors(declaredPublic)
                    .Select(constructor =>
                        $"ctor {FormatType(type)}({FormatParameters(constructor.GetParameters())})"
                    )
            );
            api.AddRange(
                type.GetMethods(declaredPublic)
                    .Where(method =>
                        !method.IsSpecialName
                        || method.Name.StartsWith("op_", StringComparison.Ordinal)
                    )
                    .Select(FormatMethod)
            );
            api.AddRange(
                type.GetProperties(declaredPublic)
                    .Select(FormatProperty)
            );
            api.AddRange(
                type.GetEvents(declaredPublic)
                    .Select(FormatEvent)
            );

            if (type.IsEnum)
            {
                api.AddRange(
                    type.GetFields(declaredPublic)
                        .Where(field => !field.IsSpecialName)
                        .Select(field =>
                            $"enum {FormatType(type)}.{field.Name} = {FormatConstant(field.GetRawConstantValue())}"
                        )
                );
            }
            else
            {
                api.AddRange(
                    type.GetFields(declaredPublic)
                        .Where(member => !member.IsSpecialName)
                        .Select(FormatField)
                );
            }
        }

        return api.Order(StringComparer.Ordinal).ToArray();
    }

    private static string[] ReadExpectedPublicApi()
    {
        return File.ReadAllLines(Path.Combine(AppContext.BaseDirectory, "PublicApi.approved.txt"));
    }

    private static string FormatMethod(MethodInfo method)
    {
        var modifiers = new List<string>();
        if (method.IsStatic)
        {
            modifiers.Add("static");
        }
        if (method.IsDefined(typeof(ExtensionAttribute), inherit: false))
        {
            modifiers.Add("extension");
        }
        if (method.DeclaringType!.IsInterface)
        {
            modifiers.Add(method.IsAbstract ? "abstract" : "default");
        }

        var methodGenericParameters = method.IsGenericMethodDefinition
            ? method.GetGenericArguments()
            : [];
        var genericArguments = methodGenericParameters.Length != 0
            ? $"<{string.Join(", ", methodGenericParameters.Select(argument => argument.Name))}>"
            : string.Empty;
        var prefix = modifiers.Count == 0 ? string.Empty : $"{string.Join(" ", modifiers)} ";
        var returnType = FormatReturnType(method.ReturnParameter);
        return $"method {prefix}{FormatType(method.DeclaringType)}.{method.Name}{genericArguments}({FormatParameters(method.GetParameters())}) -> {returnType}{FormatGenericConstraints(methodGenericParameters)}";
    }

    private static string FormatProperty(PropertyInfo property)
    {
        var accessors = new List<string>();
        if (property.GetMethod?.IsPublic == true)
        {
            accessors.Add("get;");
        }
        if (property.SetMethod?.IsPublic == true)
        {
            var isInit = property
                .SetMethod.ReturnParameter.GetRequiredCustomModifiers()
                .Contains(typeof(IsExternalInit));
            accessors.Add(isInit ? "init;" : "set;");
        }

        var indexParameters = property.GetIndexParameters();
        var name = indexParameters.Length == 0
            ? property.Name
            : $"{property.Name}[{FormatParameters(indexParameters)}]";
        var isStatic = (property.GetMethod ?? property.SetMethod)!.IsStatic ? "static " : string.Empty;
        var nullability = Nullability.Create(property);
        var propertyType = FormatAnnotatedType(property.PropertyType, nullability);
        var returnParameter = property.GetMethod?.ReturnParameter;
        var refPrefix = returnParameter is null ? "" : FormatRefPrefix(returnParameter);
        return $"property {isStatic}{FormatType(property.DeclaringType!)}.{name} : {refPrefix}{propertyType} {{ {string.Join(" ", accessors)} }}";
    }

    private static string FormatEvent(EventInfo eventInfo)
    {
        var isStatic = (eventInfo.AddMethod ?? eventInfo.RemoveMethod)!.IsStatic
            ? "static "
            : string.Empty;
        return $"event {isStatic}{FormatType(eventInfo.DeclaringType!)}.{eventInfo.Name} : {FormatAnnotatedType(eventInfo.EventHandlerType!, Nullability.Create(eventInfo))}";
    }

    private static string FormatField(FieldInfo field)
    {
        var kind = field.IsLiteral
            ? "const"
            : (field.IsStatic, field.IsInitOnly) switch
            {
                (true, true) => "static readonly field",
                (true, false) => "static field",
                (false, true) => "readonly field",
                _ => "field",
            };
        var value = field.IsLiteral ? $" = {FormatConstant(field.GetRawConstantValue())}" : string.Empty;
        return $"{kind} {FormatType(field.DeclaringType!)}.{field.Name} : {FormatAnnotatedType(field.FieldType, Nullability.Create(field))}{value}";
    }

    private static string FormatParameters(IEnumerable<ParameterInfo> parameters)
    {
        return string.Join(
            ", ",
            parameters.Select(parameter =>
            {
                var modifier = parameter.IsOut
                    ? "out "
                    : parameter.ParameterType.IsByRef
                        ? parameter.IsIn
                            ? "in "
                            : "ref "
                        : string.Empty;
                var parameterType = parameter.ParameterType.IsByRef
                    ? parameter.ParameterType.GetElementType()!
                    : parameter.ParameterType;
                var defaultValue = parameter.HasDefaultValue
                    ? $" = {FormatConstant(parameter.DefaultValue)}"
                    : string.Empty;
                return $"{modifier}{FormatAnnotatedType(parameterType, Nullability.Create(parameter))} {parameter.Name}{defaultValue}";
            })
        );
    }

    private static string FormatReturnType(ParameterInfo returnParameter)
    {
        var returnType = returnParameter.ParameterType;
        var effectiveType = returnType.IsByRef ? returnType.GetElementType()! : returnType;
        return $"{FormatRefPrefix(returnParameter)}{FormatAnnotatedType(effectiveType, Nullability.Create(returnParameter))}";
    }

    private static string FormatRefPrefix(ParameterInfo returnParameter)
    {
        if (!returnParameter.ParameterType.IsByRef)
        {
            return string.Empty;
        }

        return returnParameter.GetRequiredCustomModifiers().Contains(typeof(IsReadOnlyAttribute))
            ? "ref readonly "
            : "ref ";
    }

    private static string FormatAnnotatedType(Type type, NullabilityInfo nullability)
    {
        if (type.IsArray)
        {
            var element = FormatAnnotatedType(type.GetElementType()!, nullability.ElementType!);
            var suffix = nullability.ReadState == NullabilityState.Nullable ? "?" : "";
            return $"{element}[{new string(',', type.GetArrayRank() - 1)}]{suffix}";
        }
        if (type.IsGenericType)
        {
            var definitionName = type.IsNested
                ? $"{FormatType(type.DeclaringType!)}+{RemoveGenericArity(type.Name)}"
                : $"{type.Namespace}.{RemoveGenericArity(type.Name)}";
            var declaringArgumentCount = type.DeclaringType?.GetGenericArguments().Length ?? 0;
            var arguments = type.GetGenericArguments().Skip(declaringArgumentCount).ToArray();
            var nullableArguments = nullability.GenericTypeArguments;
            var formatted = $"{definitionName}<{string.Join(", ", arguments.Select((argument, index) => index < nullableArguments.Length ? FormatAnnotatedType(argument, nullableArguments[index]) : FormatType(argument)))}>";
            return nullability.ReadState == NullabilityState.Nullable && !type.IsValueType
                ? $"{formatted}?"
                : formatted;
        }

        var result = FormatType(type);
        return nullability.ReadState == NullabilityState.Nullable && !type.IsValueType
            ? $"{result}?"
            : result;
    }

    private static string FormatTypeDeclaration(Type type)
    {
        var relationships = new List<string>();
        if (type.BaseType is { } baseType && baseType != typeof(object) && !type.IsEnum)
        {
            relationships.Add(FormatType(baseType));
        }
        relationships.AddRange(type.GetInterfaces().Select(FormatType).Order(StringComparer.Ordinal));
        var inheritance = relationships.Count == 0
            ? string.Empty
            : $" : {string.Join(", ", relationships)}";
        var genericParameters = type.IsGenericTypeDefinition ? type.GetGenericArguments() : [];
        return $"type {GetTypeKind(type)} {FormatType(type)}{inheritance}{FormatGenericConstraints(genericParameters)}";
    }

    private static string FormatGenericConstraints(IEnumerable<Type> genericParameters)
    {
        return string.Concat(genericParameters.Select(parameter =>
        {
            var constraints = new List<string>();
            var attributes = parameter.GenericParameterAttributes;
            if ((attributes & GenericParameterAttributes.ReferenceTypeConstraint) != 0)
            {
                constraints.Add("class");
            }
            if ((attributes & GenericParameterAttributes.NotNullableValueTypeConstraint) != 0)
            {
                constraints.Add("struct");
            }
            constraints.AddRange(parameter.GetGenericParameterConstraints().Select(FormatType));
            if (
                (attributes & GenericParameterAttributes.DefaultConstructorConstraint) != 0
                && (attributes & GenericParameterAttributes.NotNullableValueTypeConstraint) == 0
            )
            {
                constraints.Add("new()");
            }

            return constraints.Count == 0
                ? string.Empty
                : $" where {parameter.Name} : {string.Join(", ", constraints)}";
        }));
    }

    private static string FormatType(Type type)
    {
        if (type.IsGenericParameter)
        {
            return type.Name;
        }
        if (type.IsArray)
        {
            return $"{FormatType(type.GetElementType()!)}[{new string(',', type.GetArrayRank() - 1)}]";
        }
        if (type.IsPointer)
        {
            return $"{FormatType(type.GetElementType()!)}*";
        }

        var name = type.IsNested
            ? $"{FormatType(type.DeclaringType!)}+{RemoveGenericArity(type.Name)}"
            : $"{type.Namespace}.{RemoveGenericArity(type.Name)}";
        if (!type.IsGenericType)
        {
            return name;
        }

        var declaringArgumentCount = type.DeclaringType?.GetGenericArguments().Length ?? 0;
        var arguments = type.GetGenericArguments().Skip(declaringArgumentCount);
        return $"{name}<{string.Join(", ", arguments.Select(FormatType))}>";
    }

    private static string RemoveGenericArity(string name)
    {
        var marker = name.IndexOf('`');
        return marker < 0 ? name : name[..marker];
    }

    private static string GetTypeKind(Type type)
    {
        if (type.IsEnum)
        {
            return "enum";
        }
        if (type.IsInterface)
        {
            return "interface";
        }
        if (type.IsValueType)
        {
            return "struct";
        }
        if (typeof(MulticastDelegate).IsAssignableFrom(type.BaseType))
        {
            return "delegate";
        }

        return "class";
    }

    private static string FormatConstant(object? value)
    {
        return value switch
        {
            null => "null",
            string text => $"\"{text.Replace("\\", "\\\\").Replace("\"", "\\\"")}\"",
            char character => $"'{character}'",
            bool boolean => boolean ? "true" : "false",
            IFormattable formattable => formattable.ToString(null, CultureInfo.InvariantCulture),
            _ => value.ToString()!,
        };
    }

    private static bool IsCompilerGenerated(MemberInfo member)
    {
        return member.IsDefined(typeof(CompilerGeneratedAttribute), inherit: false);
    }

    [Fact]
    public void Readme_Explains_The_Product_Surface()
    {
        Artifact.Value.Readme.Should().Contain("Annotation-driven data retention");
        Artifact.Value.Readme.Should().Contain("Postgres-only.");
        Artifact
            .Value.Readme.Should()
            .Contain("Annotations declare membership. Category rules declare policy.");
        Artifact.Value.Readme.Should().Contain("ConfigureCohortTables()");
        Artifact.Value.Readme.Should().Contain("AddCohort<MyDbContext>()");
        Artifact.Value.Readme.Should().Contain("IRetentionPreview");
        Artifact.Value.Readme.Should().Contain("IRetentionSweep");
        Artifact.Value.Readme.Should().Contain("IRetentionErasureService");
        Artifact.Value.Readme.Should().Contain("package test compiles this invocation example verbatim");
        Artifact.Value.Readme.Should().Contain("multiple `[ErasureSubject]` properties");
        Artifact.Value.Readme.Should().Contain("sweep_run_entity_summary");
        Artifact.Value.Readme.Should().Contain("durable correlation metadata");
        Artifact
            .Value.Readme.Should()
            .Contain(
                "`sweep_run_entity_summary` rows are uniquely identified by the durable retention entity ID"
            );
        Artifact.Value.Readme.Should().Contain("RuleSource");
        Artifact.Value.Readme.Should().Contain("RuleReason");
        Artifact.Value.Readme.Should().Contain("RetentionRowDispatcher");
        Artifact
            .Value.Readme.Should()
            .Contain("Complete the configuration by mapping the category through `IRetentionRuleProvider`");
        Artifact.Value.Readme.Should().Contain("sweep_run.DryRun");
        Artifact.Value.Readme.Should().Contain("Status = Started");
        Artifact.Value.Readme.Should().Contain("SettledAt = NULL");
        Artifact.Value.Readme.Should().Contain("IRetentionAuditObserver");
        Artifact.Value.Readme.Should().Contain("Observer delivery is best effort");
        Artifact.Value.Readme.Should().Contain("has no durable outbox");
        Artifact.Value.Readme.Should().Contain("Breaking pre-1.0 contract");
        Artifact.Value.Readme.Should().Contain("must identify an existing row");
        Artifact.Value.Readme.Should().Contain("Failure diagnostics");
        Artifact.Value.Readme.Should().Contain("Tenant passes execute sequentially in first-seen order");
        Artifact.Value.Readme.Should().Contain("Valid range: 1 to 10000");
        Artifact.Value.Readme.Should().Contain("Valid range: 1 to 256");
        Artifact.Value.Readme.Should().Contain("Valid range: 1 to 1000");
        Artifact.Value.Readme.Should().Contain("| `RowHandlerDispatch:BatchSize` | `50` |");
        Artifact.Value.Readme.Should().Contain("| `RowHandlerDispatch:MaxAttempts` | `10` |");
        Artifact.Value.Readme.Should().Contain("`RowHandlerDispatch:PayloadRetention` | `30.00:00:00`");
        Artifact.Value.Readme.Should().NotContain("run row's `FailedAt`");
        Artifact.Value.Readme.Should().NotContain("both `CompletedAt`");
    }

    [Fact]
    public void Readme_Covers_The_Useful_Greenfield_Paths()
    {
        Artifact.Value.Readme.Should().Contain("old `SessionNote` rows are deleted");
        Artifact.Value.Readme.Should().Contain("marked fields are scrubbed");
        Artifact.Value.Readme.Should().Contain("AnonymiseWithAttribute");
        Artifact.Value.Readme.Should().Contain("IRetentionRowDispatcher");
        Artifact.Value.Readme.Should().Contain("RetentionHoldRequest");
        Artifact.Value.Readme.Should().Contain("generate a host-owned EF Core migration");
        Artifact.Value.Readme.Should().Contain("Give long-lived handlers an explicit stable UUID");
        Artifact.Value.Readme.Should().Contain("AnonymisedAtPropertyName");
        Artifact
            .Value.Readme.Should()
            .Contain("[RetentionEntityId(\"a3f467fe-c5d0-4f17-9897-83c373cc1dc8\")]");
        Artifact
            .Value.Readme.Should()
            .Contain("[RetentionEntityId(\"b7316df4-7db5-46ad-aea7-f65c4b430f73\")]");
        Artifact
            .Value.Readme.Should()
            .Contain("[RetentionEntityId(\"6b619c19-6e3c-44e8-a87f-975c68fd3988\")]");
    }

    [Fact]
    public void Packed_Package_Can_Be_Restored_Into_A_Clean_Consumer_And_Expose_Release_Gate_Symbols()
    {
        var workspace = Path.Combine(
            Path.GetTempPath(),
            $"cohort-clean-consumer-{Guid.NewGuid():N}"
        );
        var consumerDirectory = Path.Combine(workspace, "consumer");
        var packageSourceDirectory = Path.Combine(workspace, "feed");
        var packageCacheDirectory = Path.Combine(workspace, "packages");
        Directory.CreateDirectory(consumerDirectory);
        Directory.CreateDirectory(packageSourceDirectory);
        Directory.CreateDirectory(packageCacheDirectory);

        try
        {
            var packageFileName = $"Cohort.{Artifact.Value.PackedVersion}.nupkg";
            File.WriteAllBytes(
                Path.Combine(packageSourceDirectory, packageFileName),
                Artifact.Value.PackageBytes
            );

            File.WriteAllText(
                Path.Combine(workspace, "NuGet.Config"),
                $$"""
                <?xml version="1.0" encoding="utf-8"?>
                <configuration>
                  <packageSources>
                    <clear />
                    <add key="local" value="{{packageSourceDirectory}}" />
                    <add key="nuget.org" value="https://api.nuget.org/v3/index.json" />
                  </packageSources>
                </configuration>
                """
            );

            File.WriteAllText(
                Path.Combine(consumerDirectory, "Consumer.csproj"),
                $$"""
                <Project Sdk="Microsoft.NET.Sdk">
                  <PropertyGroup>
                    <OutputType>Exe</OutputType>
                    <TargetFramework>net10.0</TargetFramework>
                    <ImplicitUsings>enable</ImplicitUsings>
                    <Nullable>enable</Nullable>
                  </PropertyGroup>
                  <ItemGroup>
                    <PackageReference Include="Cohort" Version="{{Artifact.Value.PackedVersion}}" />
                  </ItemGroup>
                </Project>
                """
            );

            File.WriteAllText(
                Path.Combine(consumerDirectory, "Program.cs"),
                """
                using Cohort.Application;
                using Cohort.Domain;
                using Cohort.Hosting;
                using Cohort.Infrastructure.Migrations;
                using Microsoft.EntityFrameworkCore;
                using Microsoft.Extensions.Configuration;
                using Microsoft.Extensions.DependencyInjection;

                var services = new ServiceCollection();
                services.AddLogging();
                services.AddSingleton<IConfiguration>(
                    new ConfigurationBuilder().AddInMemoryCollection().Build()
                );
                services.AddDbContext<ConsumerDbContext>(options => { });
                services.AddSingleton<IRetentionRuleProvider, ConsumerRuleProvider>();
                services.AddSingleton<IRetentionAuditObserver, ConsumerAuditObserver>();
                services.AddSingleton<IAnonymiseValueFactory, ConsumerFactory>();
                services.AddRowHandler<SessionNote, SessionNoteHandler>();
                services.AddCohort<ConsumerDbContext>();

                using var provider = services.BuildServiceProvider();
                var dispatcher = provider.GetRequiredService<IRetentionRowDispatcher>();

                Console.WriteLine(typeof(AnonymiseWithAttribute).FullName);
                Console.WriteLine(typeof(RetentionEntityIdAttribute).FullName);
                Console.WriteLine(dispatcher.GetType().FullName);

                return 0;

                public sealed class ConsumerDbContext(DbContextOptions<ConsumerDbContext> options)
                    : DbContext(options)
                {
                    protected override void OnModelCreating(ModelBuilder modelBuilder) =>
                        modelBuilder.ConfigureCohortTables("retention");

                    private static void ConfigureDefaultSchema(ModelBuilder modelBuilder) =>
                        modelBuilder.ConfigureCohortTables();
                }

                public sealed class ConsumerRuleProvider : IRetentionRuleProvider
                {
                    public RetentionCategoryCapabilities? GetCapabilities(string category) =>
                        category switch
                        {
                            "session-notes" => new([Strategy.Purge]),
                            "case-contacts" => new([Strategy.Anonymise]),
                            "user-data" => new([Strategy.Purge]),
                            _ => null,
                        };

                    public Task<RetentionRule?> ResolveAsync(
                        RetentionResolutionContext context,
                        CancellationToken ct) => Task.FromResult<RetentionRule?>(
                            context.Category switch
                            {
                                "session-notes" => new(TimeSpan.FromDays(30), Strategy.Purge),
                                "case-contacts" => new(TimeSpan.FromDays(365), Strategy.Anonymise),
                                "user-data" => new(TimeSpan.FromDays(30), Strategy.Purge),
                                _ => null,
                            }
                        );
                }

                public sealed class ConsumerFactory : IAnonymiseValueFactory
                {
                    public object? Create(AnonymiseValueContext context) => "value";
                }

                public sealed class ConsumerAuditObserver : IRetentionAuditObserver
                {
                    public Task OnCommittedAsync(SweepEvent evt, CancellationToken ct) =>
                        Task.CompletedTask;
                }

                public sealed class SessionNoteHandler : IRetentionHandler<SessionNote>;

                public static class ReadmeApiCompilation
                {
                    public static async Task ExerciseAsync(
                        IServiceProvider provider,
                        TenantContext tenant,
                        Guid noteId,
                        Guid subject,
                        CancellationToken ct)
                    {
                        var now = DateTimeOffset.UtcNow;
                        await provider.GetRequiredService<IRetentionPreview>()
                            .PreviewAsync(tenant, now, ct);
                        await provider.GetRequiredService<IRetentionSweep>()
                            .SweepAsync(tenant, now, ct);
                        await provider.GetRequiredService<IRetentionErasureService>()
                            .EraseAsync(
                                tenant,
                                new ErasureScope(subject, allowSoftDeleteAsErasure: true),
                                now,
                                ct);
                        await provider.GetRequiredService<IRetentionHoldsRepository>()
                            .CreateAsync(
                                new RetentionHoldRequest(
                                    Guid.NewGuid(),
                                    Guid.Parse("a3f467fe-c5d0-4f17-9897-83c373cc1dc8"),
                                    noteId.ToString(),
                                    tenant.Id,
                                    "Litigation hold - case #12345",
                                    now,
                                    now.AddYears(1)),
                                ct);
                    }
                }
                """
            );
            var readmeExamples = ExtractCompilableReadmeExamples(Artifact.Value.Readme);
            readmeExamples.Should().NotBeEmpty();
            for (var index = 0; index < readmeExamples.Count; index++)
            {
                File.WriteAllText(
                    Path.Combine(consumerDirectory, $"ReadmeExample{index + 1}.cs"),
                    string.Join(
                        Environment.NewLine,
                        "using Cohort.Application;",
                        "using Cohort.Domain;",
                        "using Cohort.Hosting;",
                        "using Cohort.Infrastructure.Migrations;",
                        "using Microsoft.EntityFrameworkCore;",
                        "using Microsoft.Extensions.DependencyInjection;",
                        "",
                        readmeExamples[index]
                    )
                );
            }

            var nugetConfigPath = Path.Combine(workspace, "NuGet.Config");
            var restoreArgs = new[]
            {
                "restore",
                Path.Combine(consumerDirectory, "Consumer.csproj"),
                "--configfile",
                nugetConfigPath,
                "/p:RestoreFallbackFolders=" + GlobalPackagesFolder,
                "/p:RestoreIgnoreFailedSources=true",
            };
            RunDotnet(
                restoreArgs,
                consumerDirectory,
                packageCacheDirectory,
                "clean consumer restore from the packed Cohort package"
            );

            RunDotnet(
                new[]
                {
                    "build",
                    Path.Combine(consumerDirectory, "Consumer.csproj"),
                    "--no-restore",
                    "/p:RestoreFallbackFolders=" + GlobalPackagesFolder,
                },
                consumerDirectory,
                packageCacheDirectory,
                "clean consumer build against the packed Cohort package"
            );

            var run = RunDotnet(
                new[]
                {
                    "run",
                    "--project",
                    Path.Combine(consumerDirectory, "Consumer.csproj"),
                    "--no-build",
                },
                consumerDirectory,
                packageCacheDirectory,
                "clean consumer run against the packed Cohort package"
            );

            run.Should().Contain(typeof(AnonymiseWithAttribute).FullName);
            run.Should().Contain("RetentionRowDispatcher");
        }
        finally
        {
            if (Directory.Exists(workspace))
            {
                Directory.Delete(workspace, recursive: true);
            }
        }
    }

    private static string FindRepoRoot()
    {
        var directory = new DirectoryInfo(AppContext.BaseDirectory);

        while (directory is not null)
        {
            var candidate = Path.Combine(directory.FullName, "Cohort.slnx");
            if (File.Exists(candidate))
            {
                return directory.FullName;
            }

            directory = directory.Parent;
        }

        throw new InvalidOperationException(
            "Could not locate the repository root from the test output directory."
        );
    }

    private static IReadOnlyList<string> ExtractCompilableReadmeExamples(string readme)
    {
        const string compileMarker = "<!-- package-contract:compile -->";
        const string csharpFence = "```csharp";
        var lines = readme.Replace("\r\n", "\n", StringComparison.Ordinal).Split('\n');
        var examples = new List<string>();

        for (var index = 0; index < lines.Length; index++)
        {
            if (lines[index] != csharpFence)
            {
                continue;
            }

            index.Should().BeGreaterThan(
                0,
                "every C# README fence must be explicitly marked as a compilable package contract"
            );
            lines[index - 1]
                .Should()
                .Be(
                    compileMarker,
                    "non-compilable fragments must use a text or pseudocode fence instead of csharp"
                );

            var end = Array.IndexOf(lines, "```", index + 1);
            end.Should().BeGreaterThan(index, "the marked C# README fence must be closed");
            examples.Add(string.Join(Environment.NewLine, lines[(index + 1)..end]));
            index = end;
        }

        return examples;
    }

    private static string ExtractWorkflowStepScript(string stepName)
    {
        var lines = File.ReadAllLines(
            Path.Combine(FindRepoRoot(), ".github", "workflows", "publish.yml")
        );
        var nameLine = Array.FindIndex(lines, line => line.Trim() == $"- name: {stepName}");
        nameLine
            .Should()
            .BeGreaterThanOrEqualTo(0, $"the workflow must contain the '{stepName}' step");

        var stepIndent = lines[nameLine].Length - lines[nameLine].TrimStart().Length;
        var stepEnd = Array.FindIndex(
            lines,
            nameLine + 1,
            line =>
                !string.IsNullOrWhiteSpace(line)
                && line.Length - line.TrimStart().Length == stepIndent
                && line.TrimStart().StartsWith("- ", StringComparison.Ordinal)
        );
        if (stepEnd < 0)
        {
            stepEnd = lines.Length;
        }

        var runLine = Array.FindIndex(
            lines,
            nameLine + 1,
            stepEnd - nameLine - 1,
            line =>
                line.Length - line.TrimStart().Length > stepIndent
                && line.TrimStart().StartsWith("run:", StringComparison.Ordinal)
        );
        runLine.Should().BeGreaterThan(nameLine, $"the '{stepName}' step must have a run script");

        var runValue = lines[runLine].TrimStart()["run:".Length..].TrimStart();
        if (runValue != "|")
        {
            return runValue;
        }

        var scriptLines = lines
            .Skip(runLine + 1)
            .TakeWhile(line =>
                string.IsNullOrWhiteSpace(line)
                || line.Length - line.TrimStart().Length > stepIndent
            )
            .ToArray();
        var contentIndent = scriptLines
            .Where(line => !string.IsNullOrWhiteSpace(line))
            .Min(line => line.Length - line.TrimStart().Length);

        return string.Join(
                Environment.NewLine,
                scriptLines.Select(line =>
                    string.IsNullOrWhiteSpace(line) ? string.Empty : line[contentIndent..]
                )
            )
            .TrimEnd();
    }

    private static PackedArtifact BuildPackedArtifact()
    {
        var repoRoot = FindRepoRoot();
        var projectPath = Path.Combine(repoRoot, "Cohort", "Cohort.csproj");
        var outputDirectory = Path.Combine(
            Path.GetTempPath(),
            $"cohort-package-contract-{Guid.NewGuid():N}"
        );
        Directory.CreateDirectory(outputDirectory);

        try
        {
            using var process = new Process
            {
                StartInfo = new ProcessStartInfo("dotnet")
                {
                    RedirectStandardOutput = true,
                    RedirectStandardError = true,
                    WorkingDirectory = repoRoot,
                },
            };
            process.StartInfo.ArgumentList.Add("pack");
            process.StartInfo.ArgumentList.Add(projectPath);
            process.StartInfo.ArgumentList.Add("--configuration");
            process.StartInfo.ArgumentList.Add("Release");
            process.StartInfo.ArgumentList.Add("-o");
            process.StartInfo.ArgumentList.Add(outputDirectory);
            // Packed under a prerelease suffix so the consumer restore can never be
            // shadowed by an already-published package of the same version sitting in
            // the global packages folder (used as a restore fallback for offline deps).
            process.StartInfo.ArgumentList.Add(
                $"/p:Version={ReadCsprojVersion(projectPath)}-releasegate"
            );

            var result = RunProcess(process, "dotnet pack for the shipped package surface");
            result.ExitCode.Should().Be(0, result.AssertionOutput);

            var packagePath = Directory
                .EnumerateFiles(outputDirectory, "*.nupkg", SearchOption.TopDirectoryOnly)
                .Single(path => !path.EndsWith(".snupkg", StringComparison.OrdinalIgnoreCase));

            using var archive = ZipFile.OpenRead(packagePath);
            var readmeEntry = archive.GetEntry("README.md");
            readmeEntry
                .Should()
                .NotBeNull("the shipped package must include the packed README surface");

            var nuspecEntry = archive.Entries.Single(entry =>
                entry.FullName.EndsWith(".nuspec", StringComparison.OrdinalIgnoreCase)
            );

            string readme;
            using (var reader = new StreamReader(readmeEntry!.Open()))
            {
                readme = reader.ReadToEnd();
            }
            readme.Should().Contain("Annotation-driven data retention");
            readme.Should().Contain("multiple `[ErasureSubject]` properties");
            readme.Should().Contain("Any marked subject column equals the requested subject");
            readme.Should().Contain("RetentionRowDispatcher");

            string nuspec;
            using (var reader = new StreamReader(nuspecEntry.Open()))
            {
                nuspec = reader.ReadToEnd();
            }

            var packageBytes = File.ReadAllBytes(packagePath);
            var packedVersion = XDocument
                .Parse(nuspec)
                .Root!.Descendants()
                .Single(element => element.Name.LocalName == "version")
                .Value;
            var version = Version.Parse(packedVersion.Split('-')[0]);

            return new PackedArtifact(version, packedVersion, readme, packageBytes);
        }
        finally
        {
            if (Directory.Exists(outputDirectory))
            {
                Directory.Delete(outputDirectory, recursive: true);
            }
        }
    }

    private static string RunDotnet(
        IReadOnlyList<string> arguments,
        string workingDirectory,
        string packageCacheDirectory,
        string purpose
    )
    {
        using var process = new Process
        {
            StartInfo = new ProcessStartInfo("dotnet")
            {
                RedirectStandardOutput = true,
                RedirectStandardError = true,
                WorkingDirectory = workingDirectory,
            },
        };
        foreach (var argument in arguments)
        {
            process.StartInfo.ArgumentList.Add(argument);
        }

        process.StartInfo.Environment["NUGET_PACKAGES"] = packageCacheDirectory;

        var result = RunProcess(process, $"dotnet {string.Join(" ", arguments)} for {purpose}");
        result.ExitCode.Should().Be(0, result.AssertionOutput);

        return string.Concat(result.StandardOutput, result.StandardError);
    }

    private static ProcessResult RunProcess(Process process, string description)
    {
        return RunProcessAsync(process, description).GetAwaiter().GetResult();
    }

    private static async Task<ProcessResult> RunProcessAsync(Process process, string description)
    {
        process.Start();
        var standardOutputTask = process.StandardOutput.ReadToEndAsync();
        var standardErrorTask = process.StandardError.ReadToEndAsync();

        using var timeout = new CancellationTokenSource(ProcessTimeout);
        try
        {
            await process.WaitForExitAsync(timeout.Token);
        }
        catch (OperationCanceledException) when (timeout.IsCancellationRequested)
        {
            if (!process.HasExited)
            {
                process.Kill(entireProcessTree: true);
            }

            await process.WaitForExitAsync();
            var timedOutOutput = await standardOutputTask;
            var timedOutError = await standardErrorTask;
            throw new Xunit.Sdk.XunitException(
                FormatProcessOutput(
                    $"{description} timed out after {ProcessTimeout} and was killed.",
                    timedOutOutput,
                    timedOutError
                )
            );
        }

        return new ProcessResult(
            process.ExitCode,
            await standardOutputTask,
            await standardErrorTask,
            description
        );
    }

    private static string FormatProcessOutput(
        string summary,
        string standardOutput,
        string standardError
    )
    {
        return $"{summary}{Environment.NewLine}--- stdout ---{Environment.NewLine}{standardOutput}{Environment.NewLine}--- stderr ---{Environment.NewLine}{standardError}";
    }

    private static string GetGlobalPackagesFolder()
    {
        var profile = Environment.GetFolderPath(Environment.SpecialFolder.UserProfile);
        return Path.Combine(profile, ".nuget", "packages");
    }

    private static string ReadCsprojVersion(string projectPath)
    {
        return XDocument.Load(projectPath).Root!.Descendants("Version").Single().Value;
    }

    private sealed record PackedArtifact(
        Version PackageVersion,
        string PackedVersion,
        string Readme,
        byte[] PackageBytes
    );

    private sealed record ProcessResult(
        int ExitCode,
        string StandardOutput,
        string StandardError,
        string Description
    )
    {
        public string AssertionOutput =>
            FormatProcessOutput(
                $"{Description} exited with code {ExitCode}.",
                StandardOutput,
                StandardError
            );
    }
}
