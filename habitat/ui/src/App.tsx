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
    <div class="flex min-h-screen flex-col bg-background text-foreground">
      <header class="border-b bg-muted/40">
        <div class="flex flex-col gap-3 px-4 py-4 sm:flex-row sm:items-center sm:justify-between lg:px-6">
          <p class="text-sm font-semibold leading-none">Absurd Habitat</p>
          <nav class="flex flex-wrap items-center gap-2">
            <A
              href="/"
              class="inline-flex items-center gap-2 rounded-md border border-transparent px-3 py-2 text-sm font-medium text-muted-foreground transition-colors hover:border-transparent hover:bg-background/60 hover:text-foreground focus-visible:outline-none focus-visible:ring-1 focus-visible:ring-ring"
              activeClass="border-border bg-background text-foreground shadow-sm"
              end
            >
              <span class="h-2 w-2 rounded-full bg-emerald-500" />
              Overview
            </A>
            <A
              href="/tasks"
              class="inline-flex items-center gap-2 rounded-md border border-transparent px-3 py-2 text-sm font-medium text-muted-foreground transition-colors hover:border-transparent hover:bg-background/60 hover:text-foreground focus-visible:outline-none focus-visible:ring-1 focus-visible:ring-ring"
              activeClass="border-border bg-background text-foreground shadow-sm"
            >
              <span class="h-2 w-2 rounded-full bg-blue-500" />
              Tasks
            </A>
            <A
              href="/events"
              class="inline-flex items-center gap-2 rounded-md border border-transparent px-3 py-2 text-sm font-medium text-muted-foreground transition-colors hover:border-transparent hover:bg-background/60 hover:text-foreground focus-visible:outline-none focus-visible:ring-1 focus-visible:ring-ring"
              activeClass="border-border bg-background text-foreground shadow-sm"
            >
              <span class="h-2 w-2 rounded-full bg-purple-500" />
              Events
            </A>
            <A
              href="/queues"
              class="inline-flex items-center gap-2 rounded-md border border-transparent px-3 py-2 text-sm font-medium text-muted-foreground transition-colors hover:border-transparent hover:bg-background/60 hover:text-foreground focus-visible:outline-none focus-visible:ring-1 focus-visible:ring-ring"
              activeClass="border-border bg-background text-foreground shadow-sm"
            >
              <span class="h-2 w-2 rounded-full bg-amber-500" />
              Queues
            </A>
          </nav>
        </div>
      </header>
      <main class="flex flex-1 flex-col">{props.children}</main>
    </div>
  );
}
