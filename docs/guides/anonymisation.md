# Anonymisation

`Strategy.Anonymise` keeps the row and scrubs its personal data in place. Use it when the
record still matters structurally, such as for foreign keys, reporting or counts, but the
person behind it no longer does.

## The marker column

Every entity in a category that can anonymise needs an idempotency marker. This is a
nullable `DateTimeOffset` named `AnonymisedAt` by convention, or any property marked
`[RetentionAnonymisedAt]`:

```csharp
public DateTimeOffset? AnonymisedAt { get; set; }
```

`NULL` means not yet anonymised. The sweep filters on it and stamps it, so each row is
scrubbed exactly once rather than on every sweep. Startup validation enforces the marker.

## Marking columns

For straightforward cases, mark columns with `[Anonymise]`:

```csharp
[Anonymise(AnonymiseMethod.Null)]
public string? Email { get; set; }

[Anonymise(AnonymiseMethod.EmptyString)]
public string FullName { get; set; } = "";

[Anonymise(AnonymiseMethod.FixedLiteral, "[redacted]")]
public string Phone { get; set; } = "";
```

| Method | Writes |
|---|---|
| `Null` | `NULL` |
| `EmptyString` | `""` |
| `FixedLiteral` | The literal you pass |

A literal that won't fit its column fails that entity's sweep, rather than being truncated.

## Custom values

For anything else, write an `IAnonymiseValueFactory` and point a column at it:

```csharp
[AnonymiseWith(typeof(TombstoneFactory))]
public string ExternalReference { get; set; } = "";
```

```csharp
public sealed class TombstoneFactory : IAnonymiseValueFactory
{
    public AnonymiseFactoryExecutionMode ExecutionMode => AnonymiseFactoryExecutionMode.PerRow;

    public object? Create(AnonymiseValueContext context) => $"anon-{Guid.NewGuid():N}";
}
```

```csharp
builder.Services.AddSingleton<IAnonymiseValueFactory, TombstoneFactory>();
```

`ExecutionMode` decides how often the factory runs:

| Mode | Behaviour |
|---|---|
| `Static` (default) | Called once per sweep, and the value is written to every row in one statement |
| `PerRow` | Called for each row, so every row can get a distinct value |
| `PerRowWithOriginalValue` | Called for each row with the current value in `context.OriginalValue` |

`AnonymiseValueContext` also carries the entity type, the member name, the sweep time and
the tenant.

Register each factory type exactly once as an `IAnonymiseValueFactory`. Startup fails if a
column names a factory that isn't registered, a type that doesn't implement the interface,
or a factory registered more than once.

Cohort deliberately ships no hashing or format-preserving anonymisation. Those belong in a
factory you own, where you control the keys and the format.
