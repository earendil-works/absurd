import { Absurd } from "../src/index.ts";

const app = new Absurd();
await app.createQueue();

app.registerTask({ name: "hello-world" }, async (params, ctx) => {
  console.log("Hello");
  let result = await ctx.step("step-1", async () => {
    console.log("World");
    return "done";
  });
  await ctx.sleepFor("first-sleep", 60 * 60 * 24);
  console.log("step-1 result:", result);
  result = await ctx.step("step-2", async () => {
    console.log("From Absurd");
    return "done too";
  });
  console.log("step-2 result:", result);
});

await app.startWorker();
