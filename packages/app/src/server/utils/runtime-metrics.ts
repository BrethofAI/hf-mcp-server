import type { Server } from 'node:http';
import type { Socket } from 'node:net';
import { availableParallelism } from 'node:os';
import { monitorEventLoopDelay, performance, type EventLoopUtilization } from 'node:perf_hooks';
import { getHeapStatistics } from 'node:v8';
import type {
	ForwardedForHops,
	RequestsPerSocketBucket,
	RuntimeMetricsResponse,
	RuntimeSample,
	RuntimeWindow,
	RuntimeWindowStat,
} from '../../shared/runtime-metrics.js';

export const RUNTIME_SAMPLE_INTERVAL_MS = 10_000;
const HISTORY_SIZE = 360; // one hour of 10 s samples
const WINDOW_SAMPLES = { '1m': 6, '15m': 90, '1h': 360 } as const;
const DELAY_RESOLUTION_MS = 10;

/** Sources of process measurements; injectable so tests can drive the collector deterministically. */
export interface RuntimeProbes {
	/** Monotonic milliseconds, used for elapsed time between samples. */
	now(): number;
	eventLoopUtilization(previous?: EventLoopUtilization): EventLoopUtilization;
	cpuUsage(): NodeJS.CpuUsage;
	memoryUsage(): NodeJS.MemoryUsage;
	heapLimitBytes(): number;
	/**
	 * Event-loop delay histogram (nanoseconds), reset after every sample. Its
	 * values include the sampling resolution, which is subtracted to report lateness.
	 */
	delayResolutionMs: number;
	delayHistogram: Pick<ReturnType<typeof monitorEventLoopDelay>, 'percentile' | 'max' | 'reset' | 'enable' | 'disable'>;
}

function defaultProbes(): RuntimeProbes {
	return {
		now: () => performance.now(),
		eventLoopUtilization: (previous) =>
			previous ? performance.eventLoopUtilization(previous) : performance.eventLoopUtilization(),
		cpuUsage: () => process.cpuUsage(),
		memoryUsage: () => process.memoryUsage(),
		heapLimitBytes: () => getHeapStatistics().heap_size_limit,
		delayResolutionMs: DELAY_RESOLUTION_MS,
		delayHistogram: monitorEventLoopDelay({ resolution: DELAY_RESOLUTION_MS }),
	};
}

function emptySocketBuckets(): Record<RequestsPerSocketBucket, number> {
	return { '1': 0, '2-10': 0, '11-100': 0, '101-1000': 0, '1001+': 0 };
}

function emptyHops(): Record<ForwardedForHops, number> {
	return { '0': 0, '1': 0, '2': 0, '3': 0, '4+': 0 };
}

export function requestsPerSocketBucket(requests: number): RequestsPerSocketBucket {
	if (requests <= 1) return '1';
	if (requests <= 10) return '2-10';
	if (requests <= 100) return '11-100';
	if (requests <= 1000) return '101-1000';
	return '1001+';
}

export function forwardedForHops(header: string | string[] | undefined): ForwardedForHops {
	const value = Array.isArray(header) ? header.join(',') : header;
	if (!value?.trim()) return '0';
	const entries = value.split(',').filter((entry) => entry.trim().length > 0).length;
	return entries >= 4 ? '4+' : (String(entries) as ForwardedForHops);
}

const round = (value: number, digits: number): number => {
	const factor = 10 ** digits;
	return Math.round(value * factor) / factor;
};

function windowStat(
	samples: RuntimeSample[],
	pick: (sample: RuntimeSample) => number,
	digits: number
): RuntimeWindowStat {
	let total = 0;
	let max = 0;
	for (const sample of samples) {
		const value = pick(sample);
		total += value;
		max = Math.max(max, value);
	}
	return { avg: round(total / samples.length, digits), max: round(max, digits) };
}

/**
 * Samples main-thread load, CPU, event-loop delay, memory and HTTP connection
 * reuse every 10 s, keeping one hour of history in memory.
 */
export class RuntimeMetricsCollector {
	private readonly probes: RuntimeProbes;
	private readonly history: RuntimeSample[] = [];
	private timer?: NodeJS.Timeout;
	private lastSampleAt = 0;
	private lastElu?: EventLoopUtilization;
	private lastCpu?: NodeJS.CpuUsage;
	private sessionCount?: () => number;

	private openSockets = 0;
	private totalConnections = 0;
	private totalRequests = 0;
	private connectionsAtLastSample = 0;
	private requestsAtLastSample = 0;
	private closedSockets = 0;
	private closedSocketRequests = 0;
	private requestsPerSocket = emptySocketBuckets();
	private readonly socketRequests = new WeakMap<Socket, number>();
	private hops = emptyHops();

	constructor(
		private readonly sampleIntervalMs = RUNTIME_SAMPLE_INTERVAL_MS,
		probes?: RuntimeProbes
	) {
		this.probes = probes ?? defaultProbes();
	}

	/** Begin periodic sampling. Safe to call more than once. */
	start(options: { sessionCount?: () => number } = {}): void {
		if (options.sessionCount) this.sessionCount = options.sessionCount;
		if (this.timer) return;
		this.probes.delayHistogram.enable();
		this.resetBaseline();
		this.timer = setInterval(() => this.sample(), this.sampleIntervalMs);
		this.timer.unref();
	}

	stop(): void {
		if (this.timer) clearInterval(this.timer);
		this.timer = undefined;
		this.probes.delayHistogram.disable();
	}

	/** Count connections and requests per socket on the HTTP server. */
	attachServer(server: Server): void {
		// Prepend so counts include the request currently being handled.
		server.prependListener('connection', (socket: Socket) => {
			this.openSockets++;
			this.totalConnections++;
			this.socketRequests.set(socket, 0);
			socket.once('close', () => {
				this.openSockets--;
				const requests = this.socketRequests.get(socket) ?? 0;
				// Sockets that never carried a request (probes, preconnects) are not reuse data.
				if (requests > 0) {
					this.closedSockets++;
					this.closedSocketRequests += requests;
					this.requestsPerSocket[requestsPerSocketBucket(requests)]++;
				}
			});
		});
		server.prependListener('request', (req: { socket: Socket }) => {
			this.totalRequests++;
			this.socketRequests.set(req.socket, (this.socketRequests.get(req.socket) ?? 0) + 1);
		});
	}

	recordForwardedFor(header: string | string[] | undefined): void {
		this.hops[forwardedForHops(header)]++;
	}

	private resetBaseline(): void {
		this.lastSampleAt = this.probes.now();
		this.lastElu = this.probes.eventLoopUtilization();
		this.lastCpu = this.probes.cpuUsage();
		this.connectionsAtLastSample = this.totalConnections;
		this.requestsAtLastSample = this.totalRequests;
		this.probes.delayHistogram.reset();
	}

	/** Take one sample now (called by the timer; public for tests). */
	sample(): RuntimeSample {
		const now = this.probes.now();
		const elapsedMs = Math.max(1, now - this.lastSampleAt);
		const elu = this.probes.eventLoopUtilization(this.lastElu);
		const cpu = this.probes.cpuUsage();
		const cpuMicros = this.lastCpu ? cpu.user - this.lastCpu.user + (cpu.system - this.lastCpu.system) : 0;
		const memory = this.probes.memoryUsage();
		const histogram = this.probes.delayHistogram;
		const lateness = (ns: number): number =>
			Number.isFinite(ns) ? round(Math.max(0, ns / 1e6 - this.probes.delayResolutionMs), 2) : 0;
		const seconds = elapsedMs / 1000;

		const sample: RuntimeSample = {
			at: new Date().toISOString(),
			eventLoopUtilization: round(elu.utilization, 4),
			cpuPercent: round((cpuMicros / 1000 / elapsedMs) * 100, 1),
			eventLoopDelayP50Ms: lateness(histogram.percentile(50)),
			eventLoopDelayP99Ms: lateness(histogram.percentile(99)),
			eventLoopDelayMaxMs: lateness(histogram.max),
			httpRequestsPerSecond: round((this.totalRequests - this.requestsAtLastSample) / seconds, 2),
			newConnectionsPerSecond: round((this.totalConnections - this.connectionsAtLastSample) / seconds, 2),
			openSockets: this.openSockets,
			rssBytes: memory.rss,
			heapUsedBytes: memory.heapUsed,
		};

		this.history.push(sample);
		if (this.history.length > HISTORY_SIZE) this.history.shift();
		this.resetBaseline();
		return sample;
	}

	private window(sampleCount: number): RuntimeWindow | null {
		const samples = this.history.slice(-sampleCount);
		if (samples.length === 0) return null;
		return {
			samples: samples.length,
			eventLoopUtilization: windowStat(samples, (s) => s.eventLoopUtilization, 4),
			cpuPercent: windowStat(samples, (s) => s.cpuPercent, 1),
			eventLoopDelayP99Ms: windowStat(samples, (s) => s.eventLoopDelayP99Ms, 2),
			httpRequestsPerSecond: windowStat(samples, (s) => s.httpRequestsPerSecond, 2),
			newConnectionsPerSecond: windowStat(samples, (s) => s.newConnectionsPerSecond, 2),
		};
	}

	snapshot(): RuntimeMetricsResponse {
		const memory = this.probes.memoryUsage();
		return {
			reportingSchema: 'runtime_v1',
			pid: process.pid,
			nodeVersion: process.version,
			availableParallelism: availableParallelism(),
			sampleIntervalMs: this.sampleIntervalMs,
			current: this.history.at(-1) ?? null,
			windows: {
				'1m': this.window(WINDOW_SAMPLES['1m']),
				'15m': this.window(WINDOW_SAMPLES['15m']),
				'1h': this.window(WINDOW_SAMPLES['1h']),
			},
			memory: {
				rssBytes: memory.rss,
				heapUsedBytes: memory.heapUsed,
				heapTotalBytes: memory.heapTotal,
				heapLimitBytes: this.probes.heapLimitBytes(),
				externalBytes: memory.external,
			},
			http: {
				openSockets: this.openSockets,
				totalConnections: this.totalConnections,
				totalRequests: this.totalRequests,
				requestsPerSocket: { ...this.requestsPerSocket },
				closedSockets: this.closedSockets,
				closedSocketRequests: this.closedSocketRequests,
			},
			forwardedForHops: { ...this.hops },
			legacySessions: this.sessionCount ? this.sessionCount() : null,
		};
	}
}

export const runtimeMetrics = new RuntimeMetricsCollector();
