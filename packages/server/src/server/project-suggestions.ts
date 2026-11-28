import { stat } from "node:fs/promises";
import { homedir } from "node:os";
import { basename, parse, resolve, win32 } from "node:path";
import pLimit from "p-limit";
import type { ProjectSuggestion, ProjectSuggestionsRequest } from "@getpaseo/protocol/messages";
import type { AgentManager, ManagedImportableProviderSession } from "./agent/agent-manager.js";
import { isMetadataGenerationSession } from "./agent/import-sessions.js";
import type { WorkspaceGitService } from "./workspace-git-service.js";
import type { ProjectRegistry } from "./workspace-registry.js";
import {
  looksLikeDefiniteWindowsPath,
  normalizePathForComparison,
  normalizePathForIdentity,
} from "../utils/path.js";

const DEFAULT_WINDOW_MS = 30 * 24 * 60 * 60 * 1000;
const DEFAULT_LIMIT = 20;
// Sessions read per provider. Enough history to find every repo used in the window.
const SESSION_SCAN_LIMIT = 500;
const CHECKOUT_READ_CONCURRENCY = 8;

export class ProjectSuggestionsRequestError extends Error {
  constructor(
    readonly code: string,
    message: string,
  ) {
    super(message);
    this.name = "ProjectSuggestionsRequestError";
  }
}

export type ListProjectSuggestionsInput = {
  request: Pick<ProjectSuggestionsRequest, "providers" | "since" | "limit">;
  agentManager: Pick<AgentManager, "listImportableSessions">;
  projectRegistry: Pick<ProjectRegistry, "list">;
  now?: Date;
} & (
  | { workspaceGitService: Pick<WorkspaceGitService, "getCheckout">; resolveRemoteRoot?: never }
  | { workspaceGitService?: never; resolveRemoteRoot(cwd: string): Promise<string | null> }
);

export interface ListProjectSuggestionsResult {
  suggestions: ProjectSuggestion[];
  providerErrors: Array<{ provider: string; message: string }>;
}

/**
 * Turns the agent sessions a user already ran into projects they could add. Sessions are grouped
 * by repository: a session in a subdirectory or in any worktree counts toward the main checkout,
 * because that is the path a project is keyed by.
 */
export async function listProjectSuggestions(
  input: ListProjectSuggestionsInput,
): Promise<ListProjectSuggestionsResult> {
  const now = input.now ?? new Date();
  const since = parseSince(input.request.since, now);
  const listing = await input.agentManager.listImportableSessions({
    limit: SESSION_SCAN_LIMIT,
    scanLimit: SESSION_SCAN_LIMIT,
    providerFilter: input.request.providers ? new Set(input.request.providers) : undefined,
  });
  const sessions = listing.sessions.filter(
    (session) => session.lastActivityAt.getTime() >= since && !isMetadataGenerationSession(session),
  );

  const cwdKey = input.resolveRemoteRoot ? (cwd: string) => cwd : resolve;
  const pathKey = input.resolveRemoteRoot ? remotePathKey : normalizePathForIdentity;
  const git = input.workspaceGitService;
  const lookup = git ? (cwd: string) => resolveProjectRoot(cwd, git) : input.resolveRemoteRoot;
  const rootByCwd = await resolveProjectRoots(
    new Set(sessions.map((session) => cwdKey(session.cwd))),
    lookup,
  );
  const projects = await input.projectRegistry.list();
  const excludedRoots = new Set(
    projects.filter((project) => !project.archivedAt).map((project) => pathKey(project.rootPath)),
  );
  if (!input.resolveRemoteRoot) {
    const home = homedir();
    excludedRoots.add(pathKey(home));
    excludedRoots.add(pathKey(parse(home).root));
  }
  const groups = new Map<string, SuggestionGroup>();
  for (const session of sessions) {
    const rootPath = rootByCwd.get(cwdKey(session.cwd));
    if (!rootPath) continue;
    const key = pathKey(rootPath);
    if (!excludedRoots.has(key)) addSession(groups, key, rootPath, session);
  }

  const suggestions = Array.from(groups.values())
    .sort((a, b) => b.lastActivityAt - a.lastActivityAt)
    .slice(0, input.request.limit ?? DEFAULT_LIMIT)
    .map(toSuggestion);
  return { suggestions, providerErrors: listing.providerErrors };
}

interface SuggestionGroup {
  rootPath: string;
  sessionCount: number;
  providerIds: Set<string>;
  lastActivityAt: number;
}

function parseSince(since: string | undefined, now: Date): number {
  if (!since) {
    return now.getTime() - DEFAULT_WINDOW_MS;
  }
  const timestamp = Date.parse(since);
  if (Number.isNaN(timestamp)) {
    throw new ProjectSuggestionsRequestError("invalid_since", `Invalid since timestamp: ${since}`);
  }
  return timestamp;
}

/** Maps each session cwd to the folder a project would be added at, or null when it is gone. */
async function resolveProjectRoots(
  cwds: Set<string>,
  lookup: (cwd: string) => Promise<string | null>,
): Promise<Map<string, string | null>> {
  const limit = pLimit(CHECKOUT_READ_CONCURRENCY);
  const entries = await Promise.all(
    Array.from(cwds, (cwd) =>
      limit(async () => [cwd, await lookup(cwd)] as const),
    ),
  );
  return new Map(entries);
}

async function resolveProjectRoot(
  cwd: string,
  workspaceGitService: Pick<WorkspaceGitService, "getCheckout">,
): Promise<string | null> {
  if (!(await isDirectory(cwd))) {
    return null;
  }
  const checkout = await workspaceGitService.getCheckout(cwd);
  // Git reports Windows roots with forward slashes; resolve to the form project.add keys by.
  const rootPath = resolve(checkout.mainRepoRoot ?? checkout.worktreeRoot ?? cwd);
  if (rootPath !== cwd && !(await isDirectory(rootPath))) {
    return null;
  }
  return rootPath;
}

function remotePathKey(path: string): string {
  return normalizePathForComparison(path, looksLikeDefiniteWindowsPath(path));
}

function addSession(
  groups: Map<string, SuggestionGroup>,
  key: string,
  rootPath: string,
  session: ManagedImportableProviderSession,
): void {
  const group = groups.get(key) ?? {
    rootPath,
    sessionCount: 0,
    providerIds: new Set<string>(),
    lastActivityAt: 0,
  };
  group.sessionCount += 1;
  group.providerIds.add(session.provider);
  group.lastActivityAt = Math.max(group.lastActivityAt, session.lastActivityAt.getTime());
  groups.set(key, group);
}

function toSuggestion(group: SuggestionGroup): ProjectSuggestion {
  const rootLabel = looksLikeDefiniteWindowsPath(group.rootPath)
    ? win32.basename(group.rootPath)
    : basename(group.rootPath);
  return {
    rootPath: group.rootPath,
    displayName: rootLabel || group.rootPath,
    sessionCount: group.sessionCount,
    providerIds: Array.from(group.providerIds).sort(),
    lastActivityAt: new Date(group.lastActivityAt).toISOString(),
  };
}

async function isDirectory(path: string): Promise<boolean> {
  try {
    return (await stat(path)).isDirectory();
  } catch {
    return false;
  }
}
