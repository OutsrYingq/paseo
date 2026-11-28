import type {
  ImportProviderSessionContext,
  ImportProviderSessionInput,
  ListImportableSessionsOptions,
} from "../agent/agent-sdk-types.js";
import { OmpAgentClient } from "../agent/providers/omp/agent.js";
import { importSessionFromPersistence } from "../agent/provider-session-import.js";
import type { RemoteOmpImportPort } from "./remote-omp-import-protocol.js";

export class RemoteOmpAgentClient extends OmpAgentClient {
  constructor(
    options: ConstructorParameters<typeof OmpAgentClient>[0],
    private readonly history: RemoteOmpImportPort,
  ) {
    super(options);
  }

  override listImportableSessions(options?: ListImportableSessionsOptions) {
    return this.history.ompImportList({
      kind: "ompImportList",
      cwd: options?.cwd,
      limit: options?.limit,
      scanLimit: options?.scanLimit,
    });
  }

  override async importSession(
    input: ImportProviderSessionInput,
    context: ImportProviderSessionContext,
  ) {
    const { sessionFile, config } = await this.history.ompImportResolve({
      kind: "ompImportResolve",
      providerHandleId: input.providerHandleId,
    });
    return importSessionFromPersistence({
      provider: this.provider,
      request: { ...input, providerHandleId: sessionFile },
      context,
      resumeSession: this.resumeSession.bind(this),
      config,
    });
  }
}
