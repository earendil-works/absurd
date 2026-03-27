# Pi AI Agent Durable Turns

If you use the pi agent SDK with Absurd, you can make agent turns durable with a
simple message log pattern:

1. append every `message_end` event to a durable step log
2. rebuild agent context from that log on retry
3. resume with `runAgentLoopContinue(...)` when the last message is not `assistant`.

```typescript
import { Absurd, type StepHandle, type TaskContext } from "absurd-sdk";
import {
  runAgentLoopContinue,
  type AgentContext,
  type AgentEvent,
  type AgentLoopConfig,
  type AgentMessage,
  type AgentTool,
} from "@mariozechner/pi-agent-core";
import { getModel, Type } from "@mariozechner/pi-ai";

type MessageLogEntry = { message: AgentMessage };

const app = new Absurd();

const tools: AgentTool[] = [
  {
    name: "get_time",
    label: "Get Time",
    description: "Get current UTC time",
    parameters: Type.Object({}),
    execute: async () => ({
      content: [{ type: "text", text: new Date().toISOString() }],
      details: {},
    }),
  },
];

async function loadMessageLog(ctx: TaskContext): Promise<{
  messages: AgentMessage[];
  nextHandle: StepHandle<MessageLogEntry>;
}> {
  const messages: AgentMessage[] = [];
  while (true) {
    const handle = await ctx.beginStep<MessageLogEntry>("message");
    if (!handle.done) return { messages, nextHandle: handle };
    messages.push(handle.state!.message);
  }
}

type Params = {
  systemPrompt: string;
  userMessage: string;
};

app.registerTask({ name: "run-agent" }, async (params: Params, ctx) => {
  let { messages, nextHandle } = await loadMessageLog(ctx);

  const context: AgentContext = {
    systemPrompt: params.systemPrompt,
    tools,
    messages,
  };

  const config: AgentLoopConfig = {
    model: getModel("anthropic", "claude-sonnet-4-6"),
  };

  const persistEvent = async (event: AgentEvent) => {
    if (event.type !== "message_end") return;

    await ctx.completeStep(nextHandle, { message: event.message });
    context.messages.push(event.message);

    nextHandle = await ctx.beginStep<MessageLogEntry>("message");
  };

  const last = context.messages.at(-1);

  if (!last) {
    // First attempt: append input once, then continue the loop.
    const userPrompt: AgentMessage = {
      role: "user",
      content: params.userMessage,
      timestamp: Date.now(),
    };

    await ctx.completeStep(nextHandle, { message: userPrompt });
    context.messages.push(userPrompt);
    nextHandle = await ctx.beginStep<MessageLogEntry>("message");
  } else if (last.role === "assistant") {
    // Already finished on a previous attempt.
    return;
  }

  // continue the agent
  await runAgentLoopContinue(context, config, persistEvent);
});

await app.startWorker();
```
