import {
	createMemo,
	createResource,
	createSignal,
	For,
	Index,
	Show,
	createEffect,
	onCleanup,
} from 'solid-js'
import {
	Card,
	CardContent,
	CardDescription,
	CardHeader,
	CardTitle,
} from '@/components/ui/card'
import { Button } from '@/components/ui/button'
import {
	type QueueSummary,
	type TaskSummary,
	type TaskDetail,
	fetchQueues,
	fetchQueueTasks,
	fetchTask,
	UnauthorizedError,
} from '@/lib/api'
import { TaskStatusBadge } from '@/components/TaskStatusBadge'
import { AutoRefreshToggle } from '@/components/AutoRefreshToggle'
import { TaskDetailView } from '@/components/TaskDetailView'
import { IdDisplay } from '@/components/IdDisplay'

const REFRESH_INTERVAL_MS = 15_000

interface QueuesProps {
	authenticated: () => boolean
	onAuthRequired: () => void
	onLogout: () => void
}

function formatTimestamp(value: Date | null): string {
	if (!value || Number.isNaN(value.getTime())) {
		return '—'
	}

	return new Intl.DateTimeFormat(undefined, {
		dateStyle: 'medium',
		timeStyle: 'medium',
	}).format(value)
}

export default function Queues(props: QueuesProps) {
	const [queues, { refetch: refetchQueues }] = createResource<QueueSummary[]>(fetchQueues)
	const [queuesError, setQueuesError] = createSignal<string | null>(null)
	const [expandedQueueName, setExpandedQueueName] = createSignal<string | null>(null)
	const [queueTasks, setQueueTasks] = createSignal<Record<string, TaskSummary[]>>({})
	const [expandedRunId, setExpandedRunId] = createSignal<string | null>(null)
	const [taskDetails, setTaskDetails] = createSignal<Record<string, TaskDetail>>({})
	const [autoRefreshEnabled, setAutoRefreshEnabled] = createSignal(true)

	const allQueues = createMemo(() => queues() ?? [])

	const lastUpdated = createMemo(() => {
		const items = allQueues()
		if (!items.length) {
			return null
		}
		return new Date()
	})

	createEffect(() => {
		const error = queues.error
		if (!error) {
			return
		}

		if (error instanceof UnauthorizedError) {
			props.onAuthRequired()
			setQueuesError(null)
			return
		}

		setQueuesError(error.message)
	})

	createEffect(() => {
		if (!props.authenticated() || !autoRefreshEnabled()) {
			return
		}

		const timer = setInterval(async () => {
			await refetchQueues()
			// Refetch tasks for expanded queue
			const expanded = expandedQueueName()
			if (expanded) {
				try {
					const tasks = await fetchQueueTasks(expanded)
					setQueueTasks({ ...queueTasks(), [expanded]: tasks })
				} catch (error) {
					if (error instanceof UnauthorizedError) {
						props.onAuthRequired()
					}
				}
			}
		}, REFRESH_INTERVAL_MS)

		onCleanup(() => clearInterval(timer))
	})

	const handleRefresh = async () => {
		try {
			await refetchQueues()
			const expanded = expandedQueueName()
			if (expanded) {
				const tasks = await fetchQueueTasks(expanded)
				setQueueTasks({ ...queueTasks(), [expanded]: tasks })
			}
		} catch (error) {
			if (error instanceof UnauthorizedError) {
				props.onAuthRequired()
				return
			}
			console.error('refresh failed', error)
		}
	}

	const handleQueueClick = async (queueName: string) => {
		if (expandedQueueName() === queueName) {
			setExpandedQueueName(null)
			setExpandedRunId(null)
			return
		}

		setExpandedQueueName(queueName)
		setExpandedRunId(null)

		// Fetch queue tasks if not already loaded
		if (!queueTasks()[queueName]) {
			try {
				const tasks = await fetchQueueTasks(queueName)
				setQueueTasks({ ...queueTasks(), [queueName]: tasks })
			} catch (error) {
				if (error instanceof UnauthorizedError) {
					props.onAuthRequired()
				}
				console.error('Failed to fetch queue tasks:', error)
			}
		}
	}

	const handleTaskClick = async (_taskId: string, runId: string, _attempt: number) => {
		console.log('handleTaskClick called with:', { _taskId, runId, _attempt })
		if (expandedRunId() === runId) {
			setExpandedRunId(null)
			return
		}

		setExpandedRunId(runId)

		// Fetch task details if not already loaded
		if (!taskDetails()[runId]) {
			try {
				console.log('Fetching task details for runId:', runId)
				const detail = await fetchTask(runId)
				setTaskDetails({ ...taskDetails(), [runId]: detail })
			} catch (error) {
				if (error instanceof UnauthorizedError) {
					props.onAuthRequired()
				}
				console.error('Failed to fetch task details:', error)
			}
		}
	}

	const formatAge = (timestamp: string): string => {
		const date = new Date(timestamp)
		const now = new Date()
		const diffMs = now.getTime() - date.getTime()
		const diffSec = Math.floor(diffMs / 1000)

		if (diffSec < 60) return `${diffSec}s`
		const diffMin = Math.floor(diffSec / 60)
		if (diffMin < 60) return `${diffMin}m`
		const diffHour = Math.floor(diffMin / 60)
		if (diffHour < 24) return `${diffHour}h`
		const diffDay = Math.floor(diffHour / 24)
		return `${diffDay}d`
	}

	return (
		<>
			<header class="flex flex-col gap-4 border-b bg-background px-6 py-6 sm:flex-row sm:items-center sm:justify-between">
				<div>
					<h1 class="text-2xl font-semibold tracking-tight">Queues</h1>
					<p class="text-sm text-muted-foreground">
						Job execution status and task details by queue.
					</p>
				</div>
				<div class="flex flex-col-reverse gap-3 sm:flex-row sm:items-center">
					<Show when={lastUpdated()}>
						{(value) => (
							<p class="text-xs text-muted-foreground">
								Last updated {formatTimestamp(value())}
							</p>
						)}
					</Show>
					<div class="flex items-center gap-2">
						<AutoRefreshToggle onToggle={setAutoRefreshEnabled} />
						<Show when={props.authenticated()}>
							<Button
								variant="ghost"
								class="text-xs text-muted-foreground"
								onClick={props.onLogout}
							>
								Log out
							</Button>
						</Show>
						<Button
							variant="outline"
							class="min-w-[96px]"
							onClick={handleRefresh}
							disabled={queues.loading}
						>
							{queues.loading ? 'Refreshing…' : 'Refresh'}
						</Button>
					</div>
				</div>
			</header>

			<section class="flex-1 space-y-6 px-6 py-6">
				<Show
					when={!queues.loading || allQueues().length}
					fallback={<LoadingPlaceholder />}
				>
					<Show
						when={allQueues().length > 0}
						fallback={
							<Card>
								<CardContent class="pt-6">
									<p class="rounded-md border border-dashed p-6 text-center text-sm text-muted-foreground">
										No queues discovered yet. Queues will appear here once tasks are spawned.
									</p>
								</CardContent>
							</Card>
						}
					>
						<div class="grid gap-4 md:grid-cols-2 lg:grid-cols-3">
							<For each={allQueues()}>
								{(queue) => (
									<>
										<Card
											class="cursor-pointer hover:shadow-md transition-shadow"
											onClick={() => handleQueueClick(queue.queueName)}
										>
											<CardHeader class="pb-3">
												<div class="flex items-center justify-between">
													<CardTitle class="text-lg">{queue.queueName}</CardTitle>
													<span class="text-muted-foreground text-sm">
														{expandedQueueName() === queue.queueName ? '▲' : '▼'}
													</span>
												</div>
											</CardHeader>
											<CardContent class="space-y-2">
												<div class="grid grid-cols-2 gap-2 text-sm">
													<div class="flex items-center gap-2">
														<span class="inline-block h-2 w-2 rounded-full bg-gray-400" />
														<span class="text-muted-foreground">Unclaimed:</span>
														<span class="font-medium">{queue.pendingCount}</span>
													</div>
													<div class="flex items-center gap-2">
														<span class="inline-block h-2 w-2 rounded-full bg-blue-400" />
														<span class="text-muted-foreground">Running:</span>
														<span class="font-medium">{queue.runningCount}</span>
													</div>
													<div class="flex items-center gap-2">
														<span class="inline-block h-2 w-2 rounded-full bg-yellow-400" />
														<span class="text-muted-foreground">Sleeping:</span>
														<span class="font-medium">{queue.sleepingCount}</span>
													</div>
													<div class="flex items-center gap-2">
														<span class="inline-block h-2 w-2 rounded-full bg-green-400" />
														<span class="text-muted-foreground">Completed:</span>
														<span class="font-medium">{queue.completedCount}</span>
													</div>
													<div class="flex items-center gap-2">
														<span class="inline-block h-2 w-2 rounded-full bg-red-400" />
														<span class="text-muted-foreground">Failed:</span>
														<span class="font-medium">{queue.failedCount}</span>
													</div>
												</div>
											</CardContent>
										</Card>

										<Show when={expandedQueueName() === queue.queueName}>
											<div class="md:col-span-2 lg:col-span-3">
												<Card>
													<CardHeader>
														<CardTitle>Tasks in {queue.queueName}</CardTitle>
														<CardDescription>
															Click on a task row to view detailed information.
														</CardDescription>
													</CardHeader>
													<CardContent>
														<Show
															when={queueTasks()[queue.queueName]}
															fallback={<div class="text-sm text-muted-foreground">Loading tasks...</div>}
														>
															<Show
																when={queueTasks()[queue.queueName]?.length > 0}
																fallback={
																	<p class="rounded-md border border-dashed p-6 text-center text-sm text-muted-foreground">
																		No tasks in this queue.
																	</p>
																}
															>
																<div class="overflow-x-auto">
																	<table class="min-w-full divide-y divide-border text-sm">
																		<thead>
																			<tr class="text-left text-xs uppercase text-muted-foreground">
																				<th class="px-3 py-2 font-medium">Task ID</th>
																				<th class="px-3 py-2 font-medium">Task Name</th>
																				<th class="px-3 py-2 font-medium">Status</th>
																				<th class="px-3 py-2 font-medium">Attempt</th>
																				<th class="px-3 py-2 font-medium">Age</th>
																				<th class="px-3 py-2 font-medium w-10"></th>
																			</tr>
																		</thead>
																		<tbody class="divide-y divide-border">
																			<Index each={queueTasks()[queue.queueName]}>
																				{(task) => {
																					const t = task()
																					return (
																						<>
																								<tr
																									class="hover:bg-muted/40 cursor-pointer"
																									onClick={() => {
																										console.log('ROW CLICKED!', t.taskId, t.runId, t.attempt)
																										handleTaskClick(t.taskId, t.runId, t.attempt)
																									}}
																								>
																									<td class="px-3 py-2">
																										<IdDisplay value={t.taskId} />
																									</td>
																									<td class="px-3 py-2 font-medium">{t.taskName}</td>
																								<td class="px-3 py-2">
																									<TaskStatusBadge status={t.status} />
																								</td>
																								<td class="px-3 py-2 tabular-nums">
																									{t.attempt}
																									{t.maxAttempts ? ` / ${t.maxAttempts}` : ' / ∞'}
																								</td>
																								<td class="px-3 py-2">{formatAge(t.createdAt)}</td>
																								<td class="px-3 py-2 text-center">
																									<span class="text-muted-foreground">
																										{expandedRunId() === t.runId ? '▲' : '▼'}
																									</span>
																								</td>
																							</tr>
																							<Show when={expandedRunId() === t.runId}>
																								<tr>
																									<td colspan="6" class="bg-muted/20 p-0">
																										<div class="animate-slide-down">
																											<TaskDetailView
																												task={t}
																												detail={taskDetails()[t.runId]}
																												taskLink={`/tasks/${t.taskId}`}
																											/>
																										</div>
																									</td>
																								</tr>
																							</Show>
																						</>
																					)
																				}}
																			</Index>
																		</tbody>
																	</table>
																</div>
															</Show>
														</Show>
													</CardContent>
												</Card>
											</div>
										</Show>
									</>
								)}
							</For>
						</div>
					</Show>
				</Show>
				<Show when={queuesError()}>
					{(error) => (
						<Card>
							<CardContent class="pt-6">
								<p class="rounded-md border border-destructive/30 bg-destructive/10 p-3 text-sm text-destructive">
									{error()}
								</p>
							</CardContent>
						</Card>
					)}
				</Show>
			</section>
		</>
	)
}

function LoadingPlaceholder() {
	return (
		<div class="grid gap-4 md:grid-cols-2 lg:grid-cols-3">
			<Card>
				<CardContent class="pt-6 space-y-3">
					<div class="h-6 w-2/3 rounded bg-muted animate-pulse" />
					<div class="h-4 w-full rounded bg-muted animate-pulse" />
					<div class="h-4 w-full rounded bg-muted animate-pulse" />
				</CardContent>
			</Card>
			<Card>
				<CardContent class="pt-6 space-y-3">
					<div class="h-6 w-2/3 rounded bg-muted animate-pulse" />
					<div class="h-4 w-full rounded bg-muted animate-pulse" />
					<div class="h-4 w-full rounded bg-muted animate-pulse" />
				</CardContent>
			</Card>
			<Card>
				<CardContent class="pt-6 space-y-3">
					<div class="h-6 w-2/3 rounded bg-muted animate-pulse" />
					<div class="h-4 w-full rounded bg-muted animate-pulse" />
					<div class="h-4 w-full rounded bg-muted animate-pulse" />
				</CardContent>
			</Card>
		</div>
	)
}
