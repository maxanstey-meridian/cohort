using Cohort.Domain;

namespace Cohort.Sample.Entities;

[Retain("blob-cleanup", nameof(CreatedAt))]
public sealed class BlobBackedFile
{
    public Guid Id { get; set; }
    public Guid TenantId { get; set; }
    public DateTimeOffset CreatedAt { get; set; }
    public string StoragePath { get; set; } = "";
    public string OriginalFileName { get; set; } = "";
    public string ContentType { get; set; } = "";
}
