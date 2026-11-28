import { mkdtemp, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import path from "node:path";
import { expect, test, vi } from "vitest";
import { loadPersistedConfig, savePersistedConfig } from "../persisted-config.js";
import { createTestLogger } from "../../test-utils/test-logger.js";
import { createCentralHostSettings } from "./central-host-settings.js";
import {
  RemoteHostSettingsOperationSchema,
  remoteHostSettingsResults,
} from "./remote-host-settings-protocol.js";
import { createResidentHostSettings } from "./resident-host-settings.js";

const terminalProfile = { id: "shell", name: "Shell", command: "sh" };
const agentProfile = { id: "codex", name: "Codex", provider: "codex" };

test("persists and reloads only profiles while protecting resident-owned settings", async () => {
  const paseoHome = await mkdtemp(path.join(tmpdir(), "paseo-host-settings-"));
  try {
    savePersistedConfig(paseoHome, {
      daemon: {
        listen: "127.0.0.1:6767",
        relay: { enabled: true },
        agentProfiles: [
          { ...agentProfile, options: { fast_mode: true, sandbox_mode: "read-only" } },
        ],
      },
      agents: { providers: { codex: { env: { CODEX_HOME: "/resident/private-codex" } } } },
    });
    const { execute } = createResidentHostSettings({
      paseoHome,
      logger: createTestLogger(),
      daemonVersion: "0.18.0",
      listNativeProviders: async () => ["codex"],
      getStartedAt: () => "2026-06-09T00:00:00.000Z",
    });
    const fetched = remoteHostSettingsResults.hostSettingsGet.parse(
      await execute({ kind: "hostSettingsGet" }),
    );
    expect(fetched).toMatchObject({
      providers: {},
      agentProfiles: [{ ...agentProfile, options: { service_tier: "fast" } }],
    });
    expect(fetched.terminalProfiles).toBeUndefined();
    expect(fetched.agentProfiles[0]?.options).not.toHaveProperty("sandbox_mode");
    expect(JSON.stringify(fetched)).not.toContain("/resident/private-codex");
    const operation = RemoteHostSettingsOperationSchema.safeParse({
      kind: "hostSettingsPatch",
      config: { terminalProfiles: [terminalProfile], relay: { enabled: false } },
    });
    expect(operation.success).toBe(false);
    const patched = await execute({
      kind: "hostSettingsPatch",
      config: {
        terminalProfiles: [terminalProfile],
        agentProfiles: [{ ...agentProfile, options: { service_tier: "default" } }],
      },
    });
    expect(patched).toMatchObject({ terminalProfiles: [terminalProfile] });
    const persisted = loadPersistedConfig(paseoHome);
    expect(persisted).toMatchObject({
      daemon: {
        listen: "127.0.0.1:6767",
        relay: { enabled: true },
        agentProfiles: [{ options: { service_tier: "default", sandbox_mode: "read-only" } }],
      },
      agents: { providers: { codex: { env: { CODEX_HOME: "/resident/private-codex" } } } },
    });
    savePersistedConfig(paseoHome, {
      ...persisted,
      daemon: { ...persisted.daemon, listen: "127.0.0.1:6789", terminalProfiles: [] },
    });
    expect(await execute({ kind: "hostSettingsReload" })).toMatchObject({
      appliedPaths: ["daemon.terminalProfiles"],
      restartRequiredPaths: [],
      overrideControlledPaths: ["daemon.listen"],
      config: { terminalProfiles: [] },
    });
    expect(await execute({ kind: "hostStatus", includeProviders: false })).toMatchObject({
      version: "0.18.0",
      pid: process.pid,
      startedAt: "2026-06-09T00:00:00.000Z",
      providers: [],
    });
    expect(await execute({ kind: "hostDiagnostics" })).toMatchObject({
      diagnostic: expect.stringContaining("Available native providers: codex"),
    });
  } finally {
    await rm(paseoHome, { recursive: true, force: true });
  }
});

test("rejects a mixed public patch before dispatching to the worker", async () => {
  const call = vi.fn();
  const notify = vi.fn();
  const dispatch = createCentralHostSettings({ call, serverId: "worker", onConfigChanged: notify });
  await expect(
    dispatch({
      type: "set_daemon_config_request",
      requestId: "request",
      config: { terminalProfiles: [terminalProfile], relay: { enabled: false } },
    }),
  ).rejects.toThrow("worker_host_settings_patch_unsupported");
  expect(call).not.toHaveBeenCalled();
  expect(notify).not.toHaveBeenCalled();
});

test("returns stock response and publishes the saved profile snapshot for the worker", async () => {
  const config = remoteHostSettingsResults.hostSettingsPatch.parse({
    mcp: { injectIntoAgents: false },
    agentProfiles: [agentProfile],
    terminalProfiles: [terminalProfile],
    browserTools: { enabled: false },
    providers: {},
    metadataGeneration: { providers: [] },
    autoArchiveAfterMerge: false,
    enableTerminalAgentHooks: false,
    appendSystemPrompt: "",
  });
  const call = vi.fn().mockResolvedValue(config);
  const notify = vi.fn();
  const dispatch = createCentralHostSettings({ call, serverId: "worker", onConfigChanged: notify });
  await expect(
    dispatch({
      type: "set_daemon_config_request",
      requestId: "request",
      config: { agentProfiles: [agentProfile] },
    }),
  ).resolves.toEqual({
    type: "set_daemon_config_response",
    payload: { requestId: "request", config },
  });
  expect(call).toHaveBeenCalledExactlyOnceWith({
    kind: "hostSettingsPatch",
    config: { agentProfiles: [agentProfile] },
  });
  expect(notify).toHaveBeenCalledExactlyOnceWith({
    type: "status",
    payload: { status: "daemon_config_changed", config },
  });

  call.mockResolvedValueOnce({ ...config, relay: { enabled: true } });
  await expect(
    dispatch({
      type: "set_daemon_config_request",
      requestId: "malformed-response",
      config: { agentProfiles: [agentProfile] },
    }),
  ).rejects.toThrow();
  expect(notify).toHaveBeenCalledTimes(1);
});
