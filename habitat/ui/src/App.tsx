import { type JSX } from "solid-js";
import { Router, Route, A } from "@solidjs/router";

import { IdDisplayProvider } from "@/components/IdDisplay";
import Overview from "@/views/Overview";
import Tasks from "@/views/Tasks";
import TaskRuns from "@/views/TaskRuns";
import Queues from "@/views/Queues";
import EventLog from "@/views/EventLog";

export default function App() {
  return (
    <IdDisplayProvider>
      <Router>
        <Route
          path="/"
          component={() => (
            <Layout>
              <Overview />
            </Layout>
          )}
        />
        <Route
          path="/tasks"
          component={() => (
            <Layout>
              <Tasks />
            </Layout>
          )}
        />
        <Route
          path="/tasks/:taskId"
          component={() => (
            <Layout>
              <TaskRuns />
            </Layout>
          )}
        />
        <Route
          path="/events"
          component={() => (
            <Layout>
              <EventLog />
            </Layout>
          )}
        />
        <Route
          path="/queues"
          component={() => (
            <Layout>
              <Queues />
            </Layout>
          )}
        />
      </Router>
    </IdDisplayProvider>
  );
}

function Layout(props: { children: JSX.Element }) {
  return (
    <div class="flex min-h-screen bg-background text-foreground">
      <aside class="hidden w-64 flex-col border-r bg-muted/40 lg:flex">
        <div class="flex items-center gap-2 px-6 py-5">
          <p class="text-sm font-semibold leading-none">Absurd Habitat</p>
        </div>
        <nav class="flex-1 px-3 space-y-1">
          <A
            href="/"
            class="flex w-full items-center gap-2 rounded-md px-3 py-2 text-left text-sm font-medium transition-colors hover:bg-muted focus-visible:outline-none focus-visible:ring-1 focus-visible:ring-ring"
            activeClass="bg-muted"
            end
          >
            <span class="h-2 w-2 rounded-full bg-emerald-500" />
            Overview
          </A>
          <A
            href="/tasks"
            class="flex w-full items-center gap-2 rounded-md px-3 py-2 text-left text-sm font-medium transition-colors hover:bg-muted focus-visible:outline-none focus-visible:ring-1 focus-visible:ring-ring"
            activeClass="bg-muted"
          >
            <span class="h-2 w-2 rounded-full bg-blue-500" />
            Tasks
          </A>
          <A
            href="/events"
            class="flex w-full items-center gap-2 rounded-md px-3 py-2 text-left text-sm font-medium transition-colors hover:bg-muted focus-visible:outline-none focus-visible:ring-1 focus-visible:ring-ring"
            activeClass="bg-muted"
          >
            <span class="h-2 w-2 rounded-full bg-purple-500" />
            Events
          </A>
          <A
            href="/queues"
            class="flex w-full items-center gap-2 rounded-md px-3 py-2 text-left text-sm font-medium transition-colors hover:bg-muted focus-visible:outline-none focus-visible:ring-1 focus-visible:ring-ring"
            activeClass="bg-muted"
          >
            <span class="h-2 w-2 rounded-full bg-amber-500" />
            Queues
          </A>
        </nav>
      </aside>

      <main class="flex flex-1 flex-col">{props.children}</main>
    </div>
  );
}
