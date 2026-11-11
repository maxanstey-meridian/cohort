using Cohort.Infrastructure.Sweep;

using Microsoft.EntityFrameworkCore.Metadata;

namespace Cohort.Infrastructure;

internal sealed record ErasureSubjectMetadata(
    Type EntityType,
    string Kind,
    IReadOnlyList<ErasureSubjectMember> Members
)
{
    /// <summary>
    /// The erasure service has already checked the subject against its kind's CLR type.
    /// </summary>
    internal ErasureSubjectPredicate CreatePredicate(object subject) =>
        new(
            Members
                .Select(member =>
                    new ErasureSubjectMatch(
                        member.Name,
                        member.Column,
                        member.StoreType,
                        member.Property.GetTypeMapping().Converter?.ConvertToProvider(subject) ?? subject
                    )
                )
                .ToArray()
        );
}

internal sealed record ErasureSubjectMember(
    string Name,
    Type SubjectType,
    string Column,
    string? StoreType,
    IProperty Property
);
