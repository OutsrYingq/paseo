# Central daemon and remote workers

The server and resident build from the fork main source baseline (Paseo 0.18.0). The stock client compatibility target is the v0.17.4 SDK from `2b3559b107243306986176b80beb0e779f9fe53e`.

Central owns public sessions and each worker’s AgentManager, creation transactions, message receipts, and subscriptions. The resident owns native provider and plugin processes, PTYs, file handles, and the original worker volume. Standalone continues to use the existing bootstrap.

## Authorization and identity

The adapter supplies `WorkerAuthority` and the worker TLS configuration. Central calls the adapter’s private `admit(hostname, credential, signal)` and `renew(grant, signal)` methods. The adapter verifies the old password, then returns the canonical Host, worker and Pod identity, original server ID, endpoint, peer certificate fingerprint, principal, lease ID, allowed permissions and expiry. The adapter owns its request encoding and control-plane authentication. Its schemas and types are exported from `@getpaseo/server/remote-workers`.

Central serializes admission per canonical Host. It uses the request’s single Host header for both HTTP and WebSocket, including status, files, and plugin traffic. It accepts the original WebSocket bearer subprotocol and hello password forms. Session permissions are the intersection of verified permissions and permissions requested in hello. One-time download tokens retain the issuing Host and lease. A mismatched Host cannot redeem the token.

Renewal extends the same live grant. Authoritative principal revocation ends the affected lease. Target revocation ends all leases on the affected Pod. A transient renewal failure leaves the last verified expiry in force. Central bounds each authority call and enforces expiry even if the adapter ignores cancellation. After ordinary expiry, a new authenticated admission can reopen access to the same Pod. The optional target revision describes configuration. The Pod UID identifies the execution generation. The authority may admit a replacement Pod at the same revision after fencing the old writer.

The resident validates TLS and the exact worker ID, Pod UID, and server ID before opening an execution channel. Startup checks the server ID in the mounted volume. X25519 material stays with its original owner. Direct clients continue to verify the established server ID. This contract covers direct connections.

Agent configuration requests apply to the connected worker's agent. Renaming and clearing an agent's attention marker require `workspace.write`. Agent lists and timeline snapshots prefer the stored title. When older sessions have no stored title, Central can derive a display title from the first user message in the loaded history.

The history query searches and pages within the selected worker. Permission requests and resolutions reach subscribed clients in that worker's scope. Responding to a permission request requires `workspace.write`.

## Shared session behavior

Standalone and Central share command discovery, agent configuration, workspace settings, and subscription delivery. Central resolves draft command paths against the worker's home. Each worker's provider snapshot manager uses the standalone background refresh interval.

Central authorizes public requests and binds subscriptions to their client socket. Files, Git, scripts, and provider operations run through typed requests on the admitted resident connection. Update the calls, results, and events in the corresponding domain protocol together. The private connection negotiates workspace operation support before Central sends new operations or advertises their public features.

- `central-directory.ts` owns project and workspace records. Directory inspection and Git worktree materialization run on the resident. Central persists placement and trust metadata, then dispatches setup. Registry mutations drive workspace subscriptions, including workspaces created without agents.
- `central-checkout.ts` routes checkout requests. The resident invokes the same checkout handlers and diff manager as standalone. Both JSON and binary diff subscriptions use the original resident watcher. Public delivery controls acknowledgment and release.
- Central archives workspace and agent records before asking the resident to remove an owned checkout. The resident rechecks the archive stamp and live workspace references before removal. Recovery uses the recorded checkout path. Plugin-managed checkouts are retained.
- The file adapters share the standalone file mutation, observation, and transfer services. Upload bytes stay on the selected resident. Chat attachments resolve by upload ID and the authenticated owner before dispatch. The terminal receives the uploaded file's resident path.
- The script adapter uses the existing resident terminal manager and stock automation gate. Workspace recipients subscribe to script status on their own worker. Setup and script paths are validated against the resident's configured roots.
- The worker plugin socket advertises workspace support and answers paginated, non-subscribed workspace reads. Installed plugins use the SDK workspace lookup to prepare the custom OMP runtime. Gallery metadata, source preparation, and reviewed source updates use the resident plugin runtime and its credentials.
- Each worker scope runs the stock agent message queue against its agent manager. Central checks local sender provenance before accepting an agent message. The receipt fences duplicate dispatch even when delivery waits behind an active turn.
- Host profile changes and plugin changes share the resident configuration store. The Host settings RPCs patch agent and terminal profiles. The embedding operator owns listen, authentication, Relay, and native provider settings.
- Usage reports and legacy provider usage come from the resident's usage-source registry. Provider launch environments and credentials stay on that resident. Central streams the report entries to the requesting client.
- Chat search and the prompt index use the same timeline queries as standalone, against the admitted worker's agent history.

## Scoped refresh and restart

`refresh_agent_request` requires `workspace.write`. It resolves the agent in the admitted worker's scope, reopens the native session, and loads its provider timeline. The existing per-agent lifecycle lane handles any active turn before reopening. A v0.17.4 refresh leaves the owning workspace's archive and recovery state alone.

The embedding adapter can call `central.maintenance(grant, operation, signal)` with an already authenticated `WorkerGrant`. The adapter verifies the current target through its private control-plane channel. Central exposes `WorkerMaintenanceOperationSchema` for decoding and checks the grant's expiry, target binding, and scope. Maintenance can fetch agent pages or a single agent (`workspace.read`), refresh one agent (`workspace.write`), or refresh/list the catalog for one Codex or OMP provider and cwd (`daemon.read`). Central returns the native payload. `WorkerOperationError.message` gives the error code, including `worker_maintenance_unconfirmed` for cancellation or loss of confirmation. Calls share the worker's public scope and may keep it open until grant expiry. For a different Pod UID, Central first binds an mTLS execution connection to that exact signed candidate through the per-worker Service. It adopts the connection only if the observed target is still current in Central. Older known revisions and revoked UIDs are rejected. This relies on AP admitting one live volume writer and the Service exposing that single writer across Pod replacement. The adapter owns the transport, projection checks and result mapping.

`restart_server_request` requires `daemon.manage`. Central stops accepting operations for the target, drains accepted requests and flushes its agent state. The resident then stops its provider and plugin processes on the already-bound execution channel. It retains its PTY manager and terminal IDs by default, and waits for its own PTYs to exit when `killTerminals` is true. Central emits `restart_requested` after confirmation from the resident and disconnects the target's sessions. The next connection reconstructs the worker scope. A resident without the negotiated restart capability returns `worker_restart_unsupported`. A failed or unconfirmed reset does not emit a success status. AP's separate Deployment restart replaces the Pod.

## Persistence and recovery

The private execution channel binds one worker Pod and allows one active Central writer. Operations use versioned schemas and bounded frames. Provider processes use the existing Codex app-server and OMP protocols. Central reads and writes original volume schemas through worker operations. Provider credentials, executable resolution, PTY and download handles remain resident-side.

A prompt receipt is made durable before dispatch. A repeated completed request receives the existing result. A pending request with an unknown outcome fails without dispatching a second prompt. Central restart disconnects public clients. Clients can authenticate again, rebuild subscriptions and read history from the original volumes. Native processes and the plugin host survive ordinary execution-link loss. An explicit runtime restart resets them. A lost execution link detaches subscribers and session bridges. The resident holds an active native turn until it settles and rejects a competing resume. Plugin reload and resident shutdown can close the affected native provider. Pod replacement requires the previous volume writer to stop before the next writer mounts it.

The resident acquires the same `paseo.pid` writer lock as the standalone daemon before listening. Its lock record leaves the standalone listen address empty. If a channel failure permanently fences the resident, it stops listening and retains the lock. Normal shutdown releases the lock after runtime cleanup so the next writer can reuse the same home and server ID.

## Entry points and smoke

Install the server checkout's locked dependencies with `npm ci`, then run `npm run build:server-deps` and `npm run build --workspace=@getpaseo/server`. For the compatibility smoke, build `@getpaseo/client` in a separate checkout at the v0.17.4 revision above. Run that client against the 0.18.0 Central and residents in isolated local or WSL infrastructure with synthetic identity materials.

```sh
node packages/server/dist/server/server/remote-workers/remote-entry.js central /absolute/central-adapter.mjs
node packages/server/dist/server/server/remote-workers/remote-entry.js resident /absolute/resident-adapter.mjs
```

The Central adapter exports a default `CentralOptions` object. The resident adapter exports a default `ResidentExecutionOptions` object, and wires one `createNativeExecution(...)` instance into `openRuntime` and `shutdownRuntime`. An embedding resident can call those exports directly. The private WebSocket uses `/execute` and `paseo.worker.v1`.

1. With the stock SDK, authenticate to two vhosts using their original passwords and server IDs.
2. Read projects and workspaces, choose a worker’s provider and model, open history, create a session, and observe a turn after subscribing. Retry its message ID and compare upstream model request counts.
3. Test matching agent, workspace, and plugin IDs in both scopes. Compare read-only session results, plugin bundles and RPC results, terminal output, and download token redemption. Reload one plugin and confirm the other stays active.
4. Restart one worker with each terminal policy. Verify terminal IDs and replay on default restart, provider and plugin recreation, and continuity for another worker.
5. Restart Central during a real turn. Verify the native PID survives, the submission count stays unchanged, and the reconnected client can read the result. Verify target replacement and revocation separately.

Run the focused worker regression with `npm exec -w @getpaseo/server -- vitest run src/server/remote-workers`. This suite uses the server checkout's SDK (0.18.0) and protocol fixtures for Codex and OMP. The test credentials and plugin bundles are generated locally.

The implemented transport is direct connection over the worker Host. Relay, Hub, service-preview ingress, browser automation, and scheduling require their own integration with the worker identity and lifecycle. Worktree destinations must fall under an admitted resident root. Source installation and checkout operations use the provider and forge configurations on the worker.
