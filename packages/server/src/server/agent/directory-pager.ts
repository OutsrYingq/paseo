import { getAgentStatusPriority } from "@getpaseo/protocol/agent-state-bucket";
import type { AgentSnapshotPayload } from "../messages.js";
import { SortablePager } from "../pagination/sortable-pager.js";

const SORT_KEYS = ["status_priority", "created_at", "updated_at", "title"] as const;

export function createAgentDirectoryPager() {
  return new SortablePager<AgentSnapshotPayload, (typeof SORT_KEYS)[number]>({
    validKeys: SORT_KEYS,
    defaultSort: [{ key: "updated_at", direction: "desc" }],
    label: "fetch_agents",
    getId: (agent) => agent.id,
    getSortValue: (agent, key) => {
      switch (key) {
        case "status_priority":
          return getAgentStatusPriority({
            status: agent.status,
            pendingPermissionCount: agent.pendingPermissions?.length ?? 0,
            requiresAttention: agent.requiresAttention,
            attentionReason: agent.attentionReason ?? null,
            backgroundWork: agent.backgroundWork,
          });
        case "created_at":
          return Date.parse(agent.createdAt);
        case "updated_at":
          return Date.parse(agent.updatedAt);
        case "title":
          return agent.title?.toLocaleLowerCase() ?? "";
      }
    },
  });
}
