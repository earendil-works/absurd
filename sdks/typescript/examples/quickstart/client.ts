import { Absurd } from "absurd-sdk";

const shouldAwait = process.argv.includes("--await");
const args = process.argv.slice(2).filter((arg) => arg !== "--await");
const userID = args[0] ?? "alice";
const email = args[1] ?? `${userID}@example.com`;

const app = new Absurd({ queueName: "default" });

const spawned = await app.spawn("provision-user", {
  user_id: userID,
  email,
});

console.log("spawned:", spawned);
console.log("current snapshot:", await app.fetchTaskResult(spawned.taskID));

if (shouldAwait) {
  console.log(
    `waiting for completion; emit user-activated:${userID} on queue default`,
  );
  console.log(
    "final snapshot:",
    await app.awaitTaskResult(spawned.taskID, { timeout: 300 }),
  );
}

await app.close();
