using Cohort.Domain;

namespace Cohort.Application;

public interface IRetentionHandler<TEntity>
{
    public Task OnBeforeAsync(TEntity row, RetentionBeforeContext ctx, CancellationToken ct) =>
        Task.CompletedTask;

    public Task OnAfterAsync(RetentionAfterContext<TEntity> ctx, CancellationToken ct) =>
        Task.CompletedTask;
}
