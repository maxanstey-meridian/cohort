using Cohort.Domain;

namespace Cohort.Sample.Entities;

[Retain("blob-cleanup", nameof(CreatedAt))]
[RetentionEntityId("2fb1804d-9ad8-4543-a177-5d4cd14d62ee")]
public sealed class BlobBackedFile
{
    public Guid Id { get; set; }
    public Guid TenantId { get; set; }
    public DateTimeOffset CreatedAt { get; set; }
    public string StoragePath { get; set; } = "";
    public string OriginalFileName { get; set; } = "";
    public string ContentType { get; set; } = "";
}
