using System.Data.Common;

using Cohort.Application;

using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Diagnostics;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;

using Npgsql;

namespace Cohort.Sample.Tests;

/// <summary>
/// One non-superuser database role per kind of scope Cohort creates, so a test can tell which
/// scope sent a statement (the role is on the session that ran it) and can narrow one scope's
/// grants without touching another's. <see cref="Install{TContext}"/> adds two things to a host: an
/// <see cref="IRetentionExecutionScope"/> that keeps the work's context in an
/// <see cref="AsyncLocal{T}"/> (as <c>RowLevelSecurityEndToEndTests</c> does), and a connection
/// interceptor that runs <c>SET ROLE</c> for that context whenever a connection opens (as its
/// <c>RowSecurityInterceptor</c> does). A caller's own scope, which has no context, gets
/// <see cref="Caller"/>.
/// </summary>
/// <remarks>
/// The connecting user stays a superuser, so a test's own SQL (seeding, reads, its
/// <c>CohortTestHost.CreateDbContext()</c>) runs as that user and never as one of these roles.
/// Dispose the temporary database before this object: the roles are dropped with the privileges
/// already gone.
/// </remarks>
internal sealed class ScopeRoles : IAsyncDisposable
{
    private readonly string adminConnectionString;
    private readonly AsyncLocal<RetentionExecutionContext?> flow = new();
    private readonly Dictionary<Guid, string> tenantRoles = [];
    private readonly List<string> created = [];
    private string? switchedReadiness;

    private ScopeRoles(string adminConnectionString, string suffix)
    {
        this.adminConnectionString = adminConnectionString;
        Caller = $"cohort_caller_{suffix}";
        Tenant = $"cohort_tenant_{suffix}";
        TenantlessPass = $"cohort_tenantless_{suffix}";
        Readiness = $"cohort_readiness_{suffix}";
        Recovery = $"cohort_recovery_{suffix}";
        WorkerCoordination = $"cohort_worker_{suffix}";
        HistoryPruning = $"cohort_pruning_{suffix}";
    }

    /// <summary>The caller's own scope: a service resolved outside any Cohort scope.</summary>
    public string Caller { get; }

    /// <summary>Every <see cref="RetentionExecutionContext.Tenant"/> scope not assigned a role of its own.</summary>
    public string Tenant { get; }

    public string TenantlessPass { get; }

    public string Readiness { get; }

    public string Recovery { get; }

    public string WorkerCoordination { get; }

    public string HistoryPruning { get; }

    private IEnumerable<string> All =>
        [
            Caller,
            Tenant,
            TenantlessPass,
            Readiness,
            Recovery,
            WorkerCoordination,
            HistoryPruning,
        ];

    /// <summary>Every role this object created, a tenant's own included.</summary>
    public IEnumerable<string> Names => created;

    /// <summary>A role's kind of scope, for a message that shouldn't carry a random name.</summary>
    public string Describe(string role) =>
        role == Caller ? "the caller's scope"
        : role == Tenant ? "a tenant scope"
        : role == TenantlessPass ? "the tenantless pass"
        : role == Readiness ? "the readiness scope"
        : role == Recovery ? "abandoned-run recovery"
        : role == WorkerCoordination ? "the worker"
        : role == HistoryPruning ? "the pruner"
        : $"the role {role}";

    /// <summary>Creates the roles (no login: the connecting superuser switches into them).</summary>
    public static async Task<ScopeRoles> CreateAsync(string baseConnectionString)
    {
        var roles = new ScopeRoles(baseConnectionString, Guid.NewGuid().ToString("N")[..12]);
        foreach (var role in roles.All)
        {
            await roles.CreateRoleAsync(role);
        }

        return roles;
    }

    /// <summary>
    /// Gives every role the privileges Cohort's own statements need on <paramref name="database"/>
    /// and nothing more: the schema, and SELECT, INSERT, UPDATE, and DELETE on every table in
    /// <c>public</c> (which includes row locks). A test revokes what a row takes away.
    /// </summary>
    public async Task GrantAsync(TemporaryDatabase database)
    {
        foreach (var role in All.Concat(tenantRoles.Values).Distinct())
        {
            await GrantToAsync(database, role);
        }
    }

    /// <summary>A role for one tenant's scopes, granted as the others are.</summary>
    public async Task<string> AddTenantRoleAsync(TemporaryDatabase database, Guid tenantId)
    {
        var role = $"{Tenant}_{tenantRoles.Count + 1}";
        await CreateRoleAsync(role);
        await GrantToAsync(database, role);
        tenantRoles[tenantId] = role;
        return role;
    }

    /// <summary>Runs one tenant's scopes as <paramref name="role"/>, one of the roles above.</summary>
    public void AssignTenant(Guid tenantId, string role) => tenantRoles[tenantId] = role;

    /// <summary>
    /// A second role for <see cref="RetentionSystemWork.Readiness"/>, granted as the others are
    /// and then denied <c>SELECT</c> on <c>retention_holds</c>. Call it after the model pass has
    /// been recorded: every later <c>System(Readiness)</c> scope runs as it.
    /// </summary>
    public async Task<string> UseReadinessRoleAsync(TemporaryDatabase database)
    {
        var role = $"{Readiness}_switched";
        await CreateRoleAsync(role);
        await GrantToAsync(database, role);
        await ExecuteAsync(database.ConnectionString, $"REVOKE SELECT ON public.retention_holds FROM \"{role}\"");
        switchedReadiness = role;
        return role;
    }

    /// <summary>The role a scope's work runs as: its tenant's own role if it has one.</summary>
    private string RoleFor(RetentionExecutionContext? context) =>
        context switch
        {
            null => Caller,
            RetentionExecutionContext.Tenant tenant => tenantRoles.GetValueOrDefault(
                tenant.TenantId,
                Tenant
            ),
            RetentionExecutionContext.System system => system.Work switch
            {
                RetentionSystemWork.Readiness => switchedReadiness ?? Readiness,
                RetentionSystemWork.WorkerCoordination => WorkerCoordination,
                RetentionSystemWork.TenantlessPass => TenantlessPass,
                RetentionSystemWork.Recovery => Recovery,
                RetentionSystemWork.HistoryPruning => HistoryPruning,
                _ => throw new ArgumentOutOfRangeException(nameof(context), system.Work, null),
            },
            _ => throw new ArgumentOutOfRangeException(nameof(context), context, null),
        };

    /// <summary>
    /// Replaces the host's execution scope with one that records the work's context, and has
    /// every connection of <typeparamref name="TContext"/> switch to that context's role when it
    /// opens.
    /// </summary>
    public void Install<TContext>(IServiceCollection services)
        where TContext : DbContext
    {
        services.RemoveAll<IRetentionExecutionScope>();
        services.AddSingleton<IRetentionExecutionScope>(new FlowExecutionScope(flow));
        var interceptor = new SetRoleInterceptor(this);
        services.ConfigureDbContext<TContext>(options => options.AddInterceptors(interceptor));
    }

    public async ValueTask DisposeAsync()
    {
        foreach (var role in created)
        {
            await ExecuteAsync(adminConnectionString, $"DROP ROLE IF EXISTS \"{role}\"");
        }
    }

    private async Task CreateRoleAsync(string role)
    {
        await ExecuteAsync(adminConnectionString, $"CREATE ROLE \"{role}\" NOLOGIN");
        created.Add(role);
    }

    private static Task GrantToAsync(TemporaryDatabase database, string role) =>
        ExecuteAsync(
            database.ConnectionString,
            $"""
            GRANT USAGE ON SCHEMA public TO "{role}";
            GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA public TO "{role}";
            """
        );

    private static async Task ExecuteAsync(string connectionString, string sql)
    {
        await using var connection = new NpgsqlConnection(connectionString);
        await connection.OpenAsync();
        await using var command = new NpgsqlCommand(sql, connection);
        await command.ExecuteNonQueryAsync();
    }

    private sealed class FlowExecutionScope(AsyncLocal<RetentionExecutionContext?> flow)
        : IRetentionExecutionScope
    {
        public async Task RunAsync(
            RetentionExecutionContext context,
            Func<Task> work,
            CancellationToken ct
        )
        {
            var outer = flow.Value;
            flow.Value = context;
            try
            {
                await work();
            }
            finally
            {
                flow.Value = outer;
            }
        }
    }

    /// <summary>
    /// Switches the session into the current context's role whenever its connection opens, so a
    /// pooled connection never keeps a previous scope's role.
    /// </summary>
    private sealed class SetRoleInterceptor(ScopeRoles roles) : DbConnectionInterceptor
    {
        public override void ConnectionOpened(
            DbConnection connection,
            ConnectionEndEventData eventData
        )
        {
            using var command = CreateCommand(connection);
            command.ExecuteNonQuery();
        }

        public override async Task ConnectionOpenedAsync(
            DbConnection connection,
            ConnectionEndEventData eventData,
            CancellationToken cancellationToken = default
        )
        {
            await using var command = CreateCommand(connection);
            await command.ExecuteNonQueryAsync(cancellationToken);
        }

        private DbCommand CreateCommand(DbConnection connection)
        {
            var command = connection.CreateCommand();
            command.CommandText = $"SET ROLE \"{roles.RoleFor(roles.flow.Value)}\"";
            return command;
        }
    }
}
