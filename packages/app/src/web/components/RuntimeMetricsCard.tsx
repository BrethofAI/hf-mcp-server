import type { RuntimeMetricsResponse, RuntimeWindow } from '../../shared/runtime-metrics.js';
import { formatCompactNumber } from '../lib/dashboard-utils';
import { SectionHeader } from './DashboardPrimitives';
import { Badge } from './ui/badge';
import { Card, CardContent } from './ui/card';

interface RuntimeMetricsCardProps {
	metrics: RuntimeMetricsResponse;
}

function formatBytes(bytes: number): string {
	if (bytes >= 1024 ** 3) return `${(bytes / 1024 ** 3).toFixed(2)} GB`;
	return `${Math.round(bytes / 1024 ** 2).toString()} MB`;
}

function percent(value: number): string {
	return `${(value * 100).toFixed(1)}%`;
}

function windowLine(window: RuntimeWindow | null, format: (w: RuntimeWindow) => string): string {
	return window ? format(window) : '—';
}

function Tile({ label, value, detail }: { label: string; value: string; detail?: string }) {
	return (
		<div className="rounded-xl border bg-muted/25 p-3">
			<p className="text-xs font-medium text-muted-foreground">{label}</p>
			<p className="mt-1 font-mono text-xl font-semibold">{value}</p>
			{detail ? <p className="mt-1 text-xs text-muted-foreground">{detail}</p> : null}
		</div>
	);
}

/** Main-thread load and connection reuse for this server process. */
export function RuntimeMetricsCard({ metrics }: RuntimeMetricsCardProps) {
	const { current, windows, memory, http, forwardedForHops } = metrics;
	const fifteen = windows['15m'];
	const hour = windows['1h'];
	const requestsPerSocket =
		http.closedSockets > 0 ? (http.closedSocketRequests / http.closedSockets).toFixed(1) : undefined;
	const busy = (hour?.eventLoopUtilization.max ?? 0) >= 0.8;

	return (
		<Card>
			<CardContent className="space-y-4">
				<SectionHeader
					title="Process runtime"
					description={`pid ${String(metrics.pid)} · Node ${metrics.nodeVersion} · ${String(metrics.availableParallelism)} CPUs available · sampled every ${String(metrics.sampleIntervalMs / 1000)}s`}
					aside={
						<Badge variant={busy ? 'destructive' : 'outline'}>
							{current ? `Event loop ${percent(current.eventLoopUtilization)} busy` : 'Waiting for first sample'}
						</Badge>
					}
				/>
				<div className="grid grid-cols-2 gap-3 lg:grid-cols-4">
					<Tile
						label="Event loop utilisation"
						value={current ? percent(current.eventLoopUtilization) : '—'}
						detail={`15m avg ${windowLine(fifteen, (w) => percent(w.eventLoopUtilization.avg))} · 1h max ${windowLine(hour, (w) => percent(w.eventLoopUtilization.max))}`}
					/>
					<Tile
						label="Process CPU (% of one core)"
						value={current ? `${current.cpuPercent.toFixed(0)}%` : '—'}
						detail={`15m avg ${windowLine(fifteen, (w) => `${w.cpuPercent.avg.toFixed(0)}%`)} · 1h max ${windowLine(hour, (w) => `${w.cpuPercent.max.toFixed(0)}%`)}`}
					/>
					<Tile
						label="Event loop delay p99"
						value={current ? `${current.eventLoopDelayP99Ms.toFixed(1)} ms` : '—'}
						detail={`max ${current ? current.eventLoopDelayMaxMs.toFixed(0) : '—'} ms · 1h worst p99 ${windowLine(hour, (w) => `${w.eventLoopDelayP99Ms.max.toFixed(0)} ms`)}`}
					/>
					<Tile
						label="Memory (RSS)"
						value={formatBytes(memory.rssBytes)}
						detail={`heap ${formatBytes(memory.heapUsedBytes)} of ${formatBytes(memory.heapLimitBytes)} limit`}
					/>
					<Tile
						label="HTTP requests/s"
						value={current ? current.httpRequestsPerSecond.toFixed(1) : '—'}
						detail={`1h max ${windowLine(hour, (w) => w.httpRequestsPerSecond.max.toFixed(1))} · ${formatCompactNumber(http.totalRequests)} total`}
					/>
					<Tile
						label="Open sockets"
						value={formatCompactNumber(http.openSockets)}
						detail={`${current ? current.newConnectionsPerSecond.toFixed(1) : '—'} new/s · ${formatCompactNumber(http.totalConnections)} total`}
					/>
					<Tile
						label="Requests per socket"
						value={requestsPerSocket ?? '—'}
						detail={`average over ${formatCompactNumber(http.closedSockets)} closed sockets`}
					/>
					<Tile
						label="Legacy sessions held"
						value={metrics.legacySessions === null ? '—' : formatCompactNumber(metrics.legacySessions)}
						detail="in-memory analytics sessions"
					/>
				</div>
				<div className="flex flex-wrap gap-2 text-xs">
					<span className="text-muted-foreground">Requests per socket:</span>
					{Object.entries(http.requestsPerSocket).map(([bucket, count]) => (
						<Badge key={bucket} variant="secondary">
							{bucket} · {formatCompactNumber(count)}
						</Badge>
					))}
				</div>
				<div className="flex flex-wrap gap-2 text-xs">
					<span className="text-muted-foreground">X-Forwarded-For entries on /mcp:</span>
					{Object.entries(forwardedForHops).map(([hops, count]) => (
						<Badge key={hops} variant="outline">
							{hops} · {formatCompactNumber(count)}
						</Badge>
					))}
				</div>
			</CardContent>
		</Card>
	);
}
