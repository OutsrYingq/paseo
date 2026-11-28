import { JsonlFrameDecoder } from "../agent/providers/jsonl-frame-decoder.js";
import type { NativeLaunch } from "./execution-protocol.js";

/** Keep an interrupted transport from creating a second writer for a native session. */
export class NativeTurnOwnership {
  private readonly requests = new Set<string | number>();
  private active = false;
  private uncertain = false;
  readonly input: JsonlFrameDecoder;
  readonly output: JsonlFrameDecoder;

  constructor(provider: NativeLaunch["provider"]) {
    const problem = () => {
      this.uncertain = true;
    };
    this.input = new JsonlFrameDecoder({
      problem,
      frame: (frame) => {
        const id = frame.id;
        const starts =
          provider === "codex" ? frame.method === "turn/start" : frame.type === "prompt";
        if (starts && (typeof id === "number" || typeof id === "string")) this.requests.add(id);
      },
    });
    this.output = new JsonlFrameDecoder({
      problem,
      frame: (frame) => {
        const id = frame.id;
        if ((typeof id === "number" || typeof id === "string") && this.requests.delete(id)) {
          const accepted = provider === "codex" ? !frame.error : frame.success === true;
          this.active ||= accepted;
        }
        if (provider === "codex") {
          if (frame.method === "turn/started") this.active = true;
          if (frame.method === "turn/completed") this.active = false;
        } else {
          if (frame.type === "agent_start") this.active = true;
          if (frame.type === "agent_end") this.active = false;
          if (
            frame.type === "prompt_result" &&
            (frame.status === "error" || frame.status === "local")
          )
            this.active = false;
        }
      },
    });
  }

  get mayBeRunning(): boolean {
    return this.active || this.uncertain || this.requests.size > 0;
  }
}
