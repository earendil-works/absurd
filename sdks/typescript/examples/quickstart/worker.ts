import { Absurd } from "absurd-sdk";

type ProvisionUserParams = {
  user_id: string;
  email: string;
};

type ActivationEvent = {
  activated_at: string;
};

const app = new Absurd({ queueName: "default" });

app.registerTask<ProvisionUserParams>(
  {
    name: "provision-user",
    defaultMaxAttempts: 5,
  },
  async (params, ctx) => {
    const user = await ctx.step("create-user-record", async () => {
      console.log(`[${ctx.taskID}] creating user record for ${params.user_id}`);
      return {
        user_id: params.user_id,
        email: params.email,
        created_at: new Date().toISOString(),
      };
    });

    // Demo only: fail once after the first checkpoint so the retry behavior is visible.
    const outage = await ctx.beginStep<{ simulated: boolean }>(
      "demo-transient-outage",
    );
    if (!outage.done) {
      console.log(
        `[${ctx.taskID}] simulating a temporary email provider outage`,
      );
      await ctx.completeStep(outage, { simulated: true });
      throw new Error("temporary email provider outage");
    }

    const delivery = await ctx.step("send-activation-email", async () => {
      console.log(`[${ctx.taskID}] sending activation email to ${user.email}`);
      return {
        sent: true,
        provider: "demo-mail",
        to: user.email,
      };
    });

    console.log(`[${ctx.taskID}] waiting for user-activated:${user.user_id}`);
    const activation = (await ctx.awaitEvent(`user-activated:${user.user_id}`, {
      timeout: 3600,
    })) as ActivationEvent;

    return {
      user_id: user.user_id,
      email: user.email,
      delivery,
      status: "active",
      activated_at: activation.activated_at,
    };
  },
);

console.log("worker listening on queue default");
await app.startWorker({ concurrency: 4 });
