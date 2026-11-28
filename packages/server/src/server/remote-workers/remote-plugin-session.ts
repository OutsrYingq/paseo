import { randomUUID } from "node:crypto";
import {
  encodePluginEventFrame,
  encodePluginOutputFrame,
} from "@getpaseo/protocol/binary-frames/index";
import type { SessionInboundMessage, SessionOutboundMessage } from "@getpaseo/protocol/messages";
import { RemotePluginHandlerError, type RemotePluginClient } from "./remote-plugins-client.js";
import type { RemotePluginRef } from "./remote-plugins-protocol.js";

export function createRemotePluginSession(options: {
  client: RemotePluginClient;
  send(message: SessionOutboundMessage): void;
  sendBinary(bytes: Uint8Array): void;
  canReceive(message: SessionOutboundMessage): boolean;
  supportsBinary: boolean;
  signal: AbortSignal;
}) {
  const { client, send, signal } = options;
  const refs = new Map<string, RemotePluginRef>();
  const subscriptions = new Map<string, () => void>();
  const stop = client.subscribe((event) => {
    if (signal.aborted || event.type === "providersChanged" || event.type === "pluginNotification") return;
    refs.set(event.plugin.id, event.plugin);
    if (event.type === "catalogChanged") {
      send({
        type: "status",
        payload: { status: "plugin_catalog_changed", pluginId: event.plugin.id },
      });
    } else {
      send({
        type: "status",
        payload: {
          status: "plugin_settings_changed",
          pluginId: event.plugin.id,
          settingsId: event.settingsId,
        },
      });
    }
  });

  async function list() {
    const entries = await client.request({ action: "list" });
    refs.clear();
    for (const entry of entries) refs.set(entry.plugin.id, entry.plugin);
    return entries.map(({ item }) => item);
  }

  async function plugin(id: string): Promise<RemotePluginRef> {
    if (!refs.has(id)) await list();
    const ref = refs.get(id);
    if (!ref) throw new Error("worker_plugin_missing");
    return ref;
  }

  function release(id: string): boolean {
    const close = subscriptions.get(id);
    if (!close) return false;
    subscriptions.delete(id);
    close();
    return true;
  }

  async function handle(message: SessionInboundMessage): Promise<boolean> {
    if (signal.aborted) return false;
    const pluginMessage = message.type.startsWith("plugin.");
    if (!pluginMessage || !("requestId" in message) || typeof message.requestId !== "string")
      return false;
    const { requestId } = message;
    try {
      switch (message.type) {
        case "plugin.list.request":
          send({ type: "plugin.list.response", payload: { requestId, plugins: await list() } });
          break;
        case "plugin.catalog.get.request": {
          const entries = await client.request({
            action: "catalog",
            known: message.cachedClientBundles,
          });
          const plugins = entries.map(({ plugin, ...entry }) => {
            refs.set(plugin.id, plugin);
            return { id: plugin.id, ...entry };
          });
          send({ type: "plugin.catalog.get.response", payload: { requestId, plugins } });
          break;
        }
        case "plugin.logs.get.request": {
          const entries = await client.request({
            action: "logs",
            plugin: await plugin(message.pluginId),
          });
          send({
            type: "plugin.logs.get.response",
            payload: { requestId, pluginId: message.pluginId, entries },
          });
          break;
        }
        case "plugin.reload.request":
        case "plugin.enable.request":
        case "plugin.disable.request": {
          const ref = await plugin(message.pluginId);
          const action =
            message.type === "plugin.reload.request"
              ? "reload"
              : message.type === "plugin.enable.request"
                ? "enable"
                : "disable";
          const entry = await client.request({ action, plugin: ref });
          refs.set(entry.plugin.id, entry.plugin);
          send({ type: `plugin.${action}.response`, payload: { requestId, plugin: entry.item } });
          break;
        }
        case "plugin.remove.request":
          await client.request({ action: "remove", plugin: await plugin(message.pluginId) });
          refs.delete(message.pluginId);
          send({ type: "plugin.remove.response", payload: { requestId } });
          break;
        case "plugin.directory.inspect.request": {
          const { id } = await client.request({ action: "inspectDirectory", path: message.path });
          send({ type: "plugin.directory.inspect.response", payload: { requestId, id } });
          break;
        }
        case "plugin.directory.install.request": {
          const entry = await client.request({
            action: "installDirectory",
            path: message.path,
            id: message.id,
          });
          refs.set(entry.plugin.id, entry.plugin);
          send({
            type: "plugin.directory.install.response",
            payload: { requestId, plugin: entry.item },
          });
          break;
        }
        case "plugin.source.install.request": {
          const entry = await client.request({
            action: "installSource",
            source: message.source,
            id: message.id,
            ref: message.ref,
            pluginPath: message.pluginPath,
          });
          refs.set(entry.plugin.id, entry.plugin);
          send({ type: "plugin.source.install.response", payload: { requestId, plugin: entry.item } });
          break;
        }
        case "plugin.source.status.request": {
          const plugins = await client.request({ action: "statusSources", pluginId: message.pluginId });
          send({ type: "plugin.source.status.response", payload: { requestId, plugins } });
          break;
        }
        case "plugin.source.update.request": {
          const plugins = await client.request({ action: "updateSources", pluginId: message.pluginId });
          send({ type: "plugin.source.update.response", payload: { requestId, plugins } });
          break;
        }
        case "plugin.source.update.preview.request": {
          const plugins = await client.request({
            action: "previewUpdates",
            pluginId: message.pluginId,
            target: message.target,
          });
          send({ type: "plugin.source.update.preview.response", payload: { requestId, plugins } });
          break;
        }
        case "plugin.source.update.apply.request": {
          const plugins = await client.request({ action: "applyUpdates", proposals: message.proposals });
          send({ type: "plugin.source.update.apply.response", payload: { requestId, plugins } });
          break;
        }
        case "plugin.registry.list.request": {
          const index = await client.request({ action: "listRegistry" });
          send({ type: "plugin.registry.list.response", payload: { requestId, index } });
          break;
        }
        case "plugin.registry.get.request": {
          const plugin = await client.request({ action: "getRegistryPlugin", id: message.id });
          send({ type: "plugin.registry.get.response", payload: { requestId, plugin } });
          break;
        }
        case "plugin.rpc.invoke.request": {
          const output = await client.invokePluginRpc(
            await plugin(message.pluginId),
            message.method,
            message.input,
          );
          const response: SessionOutboundMessage = {
            type: "plugin.rpc.invoke.response",
            payload: { requestId, output },
          };
          if (output instanceof Uint8Array) {
            if (!options.supportsBinary) throw new Error("worker_plugin_binary_unsupported");
            if (options.canReceive(response))
              options.sendBinary(encodePluginOutputFrame(requestId, output));
          } else send(response);
          break;
        }
        case "plugin.rpc.subscribe.request": {
          const id = randomUUID();
          const subscribed = client.subscribePluginRpc(
            await plugin(message.pluginId),
            message.method,
            message.input,
          );
          subscriptions.set(id, subscribed.release);
          try {
            await subscribed.ready;
          } catch (error) {
            release(id);
            throw error;
          }
          if (signal.aborted || !subscriptions.has(id)) {
            release(id);
            break;
          }
          send({
            type: "plugin.rpc.subscribe.response",
            payload: { requestId, subscriptionId: id },
          });
          subscribed.attach({
            event(event) {
              const response: SessionOutboundMessage = {
                type: "plugin.rpc.event",
                payload: { subscriptionId: id, event },
              };
              if (!(event instanceof Uint8Array)) send(response);
              else if (options.canReceive(response))
                options.sendBinary(encodePluginEventFrame(id, event));
            },
            closed(error) {
              if (release(id))
                send({ type: "plugin.rpc.closed", payload: { subscriptionId: id, error } });
            },
          });
          break;
        }
        default:
          return false;
      }
    } catch (error) {
      send({
        type: "rpc_error",
        payload: {
          requestId,
          requestType: message.type,
          ...(error instanceof RemotePluginHandlerError
            ? { error: `Request failed: ${error.message}`, code: "handler_error" }
            : { error: "worker_plugin_operation_failed" }),
        },
      });
    }
    return true;
  }

  return {
    handle,
    release,
    close() {
      stop();
      for (const id of subscriptions.keys()) release(id);
      refs.clear();
    },
  };
}
