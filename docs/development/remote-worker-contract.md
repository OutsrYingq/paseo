# Central daemon and remote workers

The server and resident build from the fork main source baseline (Paseo 0.18.0). The stock client compatibility target is the v0.17.4 SDK from `2b3559b107243306986176b80beb0e779f9fe53e`.

Central owns public sessions and each worker’s AgentManager, creation transactions, message receipts, and subscriptions. The resident owns native provider and plugin processes, PTYs, file handles, and the original worker volume. Standalone continues to use the existing bootstrap.

## Authorization and identity

The adapter supplies `WorkerAuthority` and the worker TLS configuration. Central calls the adapter’s private `admit(hostname, credential, signal)` and `renew(grant, signal)` methods. The adapter verifies the old password, then returns the canonical Host, worker and Pod identity, original server ID, endpoint, peer certificate fingerprint, principal, lease ID, allowed permissions and expiry. The adapter owns its request encoding and control-plane authentication. Its schemas and types are exported from `@getpaseo/server/remote-workers`.

Central serializes admission per canonical Host. It uses the request’s single Host header for both HTTP and WebSocket, including status, files, and plugin traffic. It accepts the original WebSocket bearer subprotocol and hello password forms. Session permissions are the intersection of verified permissions and permissions requested in hello. One-time download tokens retain the issuing Host and lease. A mismatched Host cannot redeem the token.

Renewal extends the same live grant. Authoritative principal revocation ends the affected lease. Target revocation ends all leases on the affected Pod. A transient renewal failure leaves the last verified expiry in force. Central bounds each authority call and enforces expiry even if the adapter ignores cancellation. After ordinary expiry, a new authenticated admission can reopen access to the same Pod. The optional target revision describes configuration. The Pod UID identifies the execution generation. The authority may admit a replacement Pod at the same revision after fencing the old writer.

The resident validates TLS and the exact worker ID, Pod UID, and server ID before opening an execution channel. Startup checks the server ID in the mounted volume. X25519 material stays with its original owner. Direct clients continue to verify the established server ID. This contract covers direct connections.

## Persistence and recovery

The private execution channel binds one worker Pod and allows one active Central writer. Operations use versioned schemas and bounded frames. Provider processes use the existing Codex app-server and OMP protocols. Central reads and writes original volume schemas through worker operations. Provider credentials, executable resolution, PTY and download handles remain resident-side.

A prompt receipt is made durable before dispatch. A repeated completed request receives the existing result. A pending request with an unknown outcome fails without dispatching a second prompt. Central restart disconnects public clients. Clients can authenticate again, rebuild subscriptions and read history from the original volumes. Native processes and the plugin host live for the resident lifetime. A lost execution link detaches subscribers and session bridges. The resident holds an active native turn until it settles and rejects a competing resume. Plugin reload and resident shutdown can close the affected native provider. Pod replacement requires the previous volume writer to stop before the next writer mounts it.

## Entry points and smoke

Install the server checkout?s locked dependencies with `npm ci`, then run `npm run build:server-deps` and `npm run build --workspace=@getpaseo/server`. For the compatibility smoke, build `@getpaseo/client` in a separate checkout at the v0.17.4 revision above. Run that client against the 0.18.0 Central and residents in isolated local or WSL infrastructure with synthetic identity materials.

```sh
node packages/server/dist/server/server/remote-workers/remote-entry.js central /absolute/central-adapter.mjs
node packages/server/dist/server/server/remote-workers/remote-entry.js resident /absolute/resident-adapter.mjs
```

The Central adapter exports a default `CentralOptions` object. The resident adapter exports a default `ResidentExecutionOptions` object, and wires one `createNativeExecution(...)` instance into `openRuntime` and `shutdownRuntime`. An embedding resident can call those exports directly. The private WebSocket uses `/execute` and `paseo.worker.v1`.

1. With the stock SDK, authenticate to two vhosts using their original passwords and server IDs.
2. Read projects and workspaces, choose a worker’s provider and model, open history, create a session, and observe a turn after subscribing. Retry its message ID and compare upstream model request counts.
3. Test matching agent, workspace, and plugin IDs in both scopes. Compare read-only session results, plugin bundles and RPC results, terminal output, and download token redemption. Reload one plugin and confirm the other stays active.
4. Restart Central during a real turn. Verify the native PID survives, the submission count stays unchanged, and the reconnected client can read the result. Verify target replacement and revocation separately.

Run the focused worker regression with `npm exec -w @getpaseo/server -- vitest run src/server/remote-workers`. This suite uses the server checkout?s SDK (0.18.0) and protocol fixtures for Codex and OMP. The test credentials and plugin bundles are generated locally.

This implementation covers direct connections, project and workspace reads, directory workspace creation under an existing project, and agent creation in the selected worker. Installed plugins support listing, logs, reload, enable, disable, and remove with the stock `daemon.manage` permission. Git worktree provisioning, other workspace mutations, plugin source installation and updates, browser automation, and worker-local MCP and control-plane reload compatibility require additional integration.
