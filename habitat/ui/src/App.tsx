import {
  createEffect,
  createResource,
  createSignal,
  Show,
  type JSX,
} from "solid-js";
import { Router, Route, A } from "@solidjs/router";
import { Button } from "@/components/ui/button";
import {
  Dialog,
  DialogContent,
  DialogDescription,
  DialogFooter,
  DialogHeader,
  DialogTitle,
} from "@/components/ui/dialog";
import {
  TextField,
  TextFieldDescription,
  TextFieldLabel,
  TextFieldRoot,
} from "@/components/ui/textfield";
import {
  APIError,
  type ConfigMetadata,
  fetchConfig,
  login,
  UnauthorizedError,
} from "@/lib/api";
import { logout } from "@/lib/api";
import { IdDisplayProvider } from "@/components/IdDisplay";
import Overview from "@/views/Overview";
import Tasks from "@/views/Tasks";
import TaskRuns from "@/views/TaskRuns";

export default function App() {
  const [config] = createResource<ConfigMetadata>(fetchConfig);
  const [authenticated, setAuthenticated] = createSignal(false);
  const [authDialogOpen, setAuthDialogOpen] = createSignal(false);
  const [authError, setAuthError] = createSignal<string | null>(null);
  const [isSubmitting, setIsSubmitting] = createSignal(false);
  const [username, setUsername] = createSignal("");
  const [password, setPassword] = createSignal("");

  const authIsMandatory = () =>
    (config()?.authRequired ?? false) && !authenticated();

  createEffect(() => {
    if ((config()?.authRequired ?? false) && !authenticated()) {
      setAuthDialogOpen(true);
    }
  });

  const handleAuthRequired = () => {
    setAuthenticated(false);
    setAuthDialogOpen(true);
  };

  const handleSubmit = async (event: Event) => {
    event.preventDefault();
    setAuthError(null);
    setIsSubmitting(true);

    try {
      await login(username().trim(), password());
      setAuthenticated(true);
      setAuthDialogOpen(false);
      setPassword("");
    } catch (error) {
      if (error instanceof UnauthorizedError) {
        setAuthError("Invalid username or password.");
      } else if (error instanceof APIError) {
        setAuthError(error.message);
      } else {
        setAuthError("Failed to sign in. Please try again.");
      }
    } finally {
      setIsSubmitting(false);
    }
  };

  const handleLogout = async () => {
    await logout();
    setAuthenticated(false);
    setAuthDialogOpen(authIsMandatory());
    setPassword("");
    setUsername("");
  };

  return (
    <IdDisplayProvider>
      <Router>
        <Route
          path="/"
          component={() => (
            <Layout>
              <Overview
                authenticated={authenticated}
                onAuthRequired={handleAuthRequired}
                onLogout={handleLogout}
              />
            </Layout>
          )}
        />
        <Route
          path="/tasks"
          component={() => (
            <Layout>
              <Tasks
                authenticated={authenticated}
                onAuthRequired={handleAuthRequired}
                onLogout={handleLogout}
              />
            </Layout>
          )}
        />
        <Route
          path="/tasks/:taskId"
          component={() => (
            <Layout>
              <TaskRuns
                authenticated={authenticated}
                onAuthRequired={handleAuthRequired}
                onLogout={handleLogout}
              />
            </Layout>
          )}
        />
      </Router>

      <Dialog
        open={authIsMandatory() || authDialogOpen()}
        onOpenChange={(next) => {
          if (authIsMandatory()) {
            setAuthDialogOpen(true);
            return;
          }
          setAuthDialogOpen(next);
        }}
        modal
        preventScroll
      >
        <DialogContent closable={!authIsMandatory()}>
          <DialogHeader>
            <DialogTitle>Sign in to Habitat</DialogTitle>
            <DialogDescription>
              Provide the credentials configured for this dashboard.
            </DialogDescription>
          </DialogHeader>
          <form class="space-y-4" onSubmit={handleSubmit}>
            <div class="grid gap-3">
              <TextFieldRoot>
                <TextFieldLabel>Username</TextFieldLabel>
                <TextField
                  type="text"
                  placeholder="dashboard-user"
                  value={username()}
                  onInput={(event) => setUsername(event.currentTarget.value)}
                  required
                  autocomplete="username"
                />
              </TextFieldRoot>
              <TextFieldRoot>
                <TextFieldLabel>Password</TextFieldLabel>
                <TextField
                  type="password"
                  placeholder="••••••••"
                  value={password()}
                  onInput={(event) => setPassword(event.currentTarget.value)}
                  required
                  autocomplete="current-password"
                />
                <TextFieldDescription>
                  Passwords are verified using the hashed credentials provided
                  in your Habitat configuration.
                </TextFieldDescription>
              </TextFieldRoot>
            </div>
            <Show when={authError()}>
              {(error) => (
                <p class="rounded-md border border-destructive/30 bg-destructive/10 p-3 text-sm text-destructive">
                  {error()}
                </p>
              )}
            </Show>
            <DialogFooter>
              <Button
                type="submit"
                class="w-full sm:w-auto"
                disabled={isSubmitting()}
              >
                {isSubmitting() ? "Signing in…" : "Sign in"}
              </Button>
            </DialogFooter>
          </form>
        </DialogContent>
      </Dialog>
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
        </nav>
      </aside>

      <main class="flex flex-1 flex-col">{props.children}</main>
    </div>
  );
}
