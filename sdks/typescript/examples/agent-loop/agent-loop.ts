import { Absurd, TaskContext } from "absurd-sdk";
import { anthropic } from "@ai-sdk/anthropic";
import { type ModelMessage, generateText, tool } from "ai";
import "dotenv/config";
import z from "zod";

const absurd = new Absurd();

const getWeather = async ({ location }: { location: string }) => {
  if (Math.random() < 0.5) {
    throw new Error("Simulated weather service failure");
  }
  return `The weather in ${location} is ${Math.floor(Math.random() * 100)} degrees.`;
};

absurd.registerTask(
  {
    name: "weather-agent",
  },
  async (params, ctx: TaskContext) => {
    const initialMessage = await ctx.step("initial-message", async () => {
      return {
        role: "user" as const,
        content: params.prompt,
      };
    });

    let messages: ModelMessage[] = [initialMessage];
    let step = 0;
    while (step < 20) {
      let stepState = await ctx.step("agent-loop", async () => {
        const result = await generateText({
          model: anthropic("claude-haiku-4-5"),
          system: "You are a helpful agent",
          messages,
          tools: {
            getWeather: tool({
              description: "Get the current weather in a given location",
              inputSchema: z.object({
                location: z.string(),
              }),
            }),
          },
        });

        const newMessages = (await result.response).messages;
        const finishReason = await result.finishReason;

        if (finishReason === "tool-calls") {
          const toolCalls = await result.toolCalls;

          // Handle all tool call execution here
          for (const toolCall of toolCalls) {
            if (toolCall.toolName === "getWeather") {
              const toolOutput = await getWeather(toolCall.input as any);
              newMessages.push({
                role: "tool",
                content: [
                  {
                    toolName: toolCall.toolName,
                    toolCallId: toolCall.toolCallId,
                    type: "tool-result",
                    output: { type: "text", value: toolOutput },
                  },
                ],
              });
            }
          }
        }

        return { newMessages, finishReason };
      });

      console.log(stepState);
      messages.push(...stepState.newMessages);
      if (stepState.finishReason !== "tool-calls") {
        return messages[messages.length - 1].content;
      }
    }
  },
);

async function main() {
  await absurd.createQueue();

  const workerShutdowns = await Promise.all(
    Array.from({ length: 2 }, (_, idx) =>
      absurd.startWorker({
        workerId: `demo-worker-${idx + 1}`,
        batchSize: 1,
        onError: (error) => {
          console.log("worker loop error", {
            workerId: `demo-worker-${idx + 1}`,
            error: error.message,
          });
        },
      }),
    ),
  );

  await absurd.spawn("weather-agent", {
    prompt: "Get the weather in New York and San Francisco",
  });

  await sleep(10000);

  for (const shutdown of workerShutdowns) {
    await shutdown();
  }
}

function sleep(ms: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

main().catch(console.error);
