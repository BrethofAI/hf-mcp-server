import { createServer, request, Agent, type Server } from 'node:http';
import type { AddressInfo } from 'node:net';
import { afterEach, describe, expect, it } from 'vitest';
import {
	RuntimeMetricsCollector,
	forwardedForHops,
	requestsPerSocketBucket,
	type RuntimeProbes,
} from '../../../src/server/utils/runtime-metrics.js';
import { runtimeSampleLogIntervalMs } from '../../../src/server/utils/runtime-sample-logger.js';

function fakeProbes() {
	const state = { now: 0, eluActive: 0, cpuMicros: 0, delayNs: { p50: 1e6, p99: 20e6, max: 40e6 } };
	const probes: RuntimeProbes = {
		now: () => state.now,
		// Mimic perf_hooks: cumulative counters, delta when a previous reading is passed.
		eventLoopUtilization: (previous) => {
			const total = { idle: state.now - state.eluActive, active: state.eluActive, utilization: 0 };
			if (!previous) return total;
			const idle = total.idle - previous.idle;
			const active = total.active - previous.active;
			return { idle, active, utilization: active / (idle + active) };
		},
		cpuUsage: () => ({ user: state.cpuMicros, system: 0 }),
		memoryUsage: () => ({ rss: 300e6, heapUsed: 100e6, heapTotal: 150e6, external: 5e6, arrayBuffers: 1e6 }),
		heapLimitBytes: () => 4e9,
		delayResolutionMs: 0,
		delayHistogram: {
			percentile: (p: number) => (p === 50 ? state.delayNs.p50 : state.delayNs.p99),
			get max() {
				return state.delayNs.max;
			},
			reset: () => undefined,
			enable: () => true,
			disable: () => true,
		},
	};
	return { state, probes };
}

describe('runtime metric helpers', () => {
	it('buckets requests per socket', () => {
		expect([1, 2, 10, 11, 100, 101, 1000, 1001].map(requestsPerSocketBucket)).toEqual([
			'1',
			'2-10',
			'2-10',
			'11-100',
			'11-100',
			'101-1000',
			'101-1000',
			'1001+',
		]);
	});

	it('counts X-Forwarded-For entries', () => {
		expect(forwardedForHops(undefined)).toBe('0');
		expect(forwardedForHops(' ')).toBe('0');
		expect(forwardedForHops('203.0.113.1')).toBe('1');
		expect(forwardedForHops('203.0.113.1, 10.0.0.1')).toBe('2');
		expect(forwardedForHops(['1.1.1.1, 2.2.2.2', '3.3.3.3'])).toBe('3');
		expect(forwardedForHops('a,b,c,d,e')).toBe('4+');
	});

	it('parses the dataset sample interval', () => {
		expect(runtimeSampleLogIntervalMs({})).toBe(60_000);
		expect(runtimeSampleLogIntervalMs({ RUNTIME_METRICS_LOG_INTERVAL_MS: '0' })).toBe(0);
		expect(runtimeSampleLogIntervalMs({ RUNTIME_METRICS_LOG_INTERVAL_MS: '15000' })).toBe(15_000);
		expect(runtimeSampleLogIntervalMs({ RUNTIME_METRICS_LOG_INTERVAL_MS: 'x' })).toBe(60_000);
	});
});

describe('RuntimeMetricsCollector', () => {
	it('computes per-interval utilisation, CPU and delay, and windows over history', () => {
		const { state, probes } = fakeProbes();
		const collector = new RuntimeMetricsCollector(10_000, probes);
		collector.start({ sessionCount: () => 42 });
		collector.stop();

		// 10 s with 2.5 s busy and 4 s of CPU time.
		state.now = 10_000;
		state.eluActive = 2_500;
		state.cpuMicros = 4_000_000;
		const first = collector.sample();
		expect(first).toMatchObject({
			eventLoopUtilization: 0.25,
			cpuPercent: 40,
			eventLoopDelayP50Ms: 1,
			eventLoopDelayP99Ms: 20,
			eventLoopDelayMaxMs: 40,
		});

		// Next 10 s fully busy.
		state.now = 20_000;
		state.eluActive = 12_500;
		state.cpuMicros = 14_000_000;
		collector.sample();

		const snapshot = collector.snapshot();
		expect(snapshot.current?.eventLoopUtilization).toBe(1);
		expect(snapshot.windows['1m']).toMatchObject({
			samples: 2,
			eventLoopUtilization: { avg: 0.625, max: 1 },
			cpuPercent: { avg: 70, max: 100 },
		});
		expect(snapshot.memory.heapLimitBytes).toBe(4e9);
		expect(snapshot.legacySessions).toBe(42);
	});

	it('reports event-loop lateness net of the histogram resolution', () => {
		const { state, probes } = fakeProbes();
		const collector = new RuntimeMetricsCollector(10_000, { ...probes, delayResolutionMs: 10 });
		state.now = 10_000;
		// p50 at exactly the resolution means no lateness at all.
		state.delayNs = { p50: 10e6, p99: 25e6, max: 60e6 };
		expect(collector.sample()).toMatchObject({
			eventLoopDelayP50Ms: 0,
			eventLoopDelayP99Ms: 15,
			eventLoopDelayMaxMs: 50,
		});
	});

	describe('with an HTTP server', () => {
		let server: Server | undefined;
		afterEach(async () => {
			await new Promise((resolve) => server?.close(resolve));
		});

		it('counts connections and requests per socket', async () => {
			const { probes } = fakeProbes();
			const collector = new RuntimeMetricsCollector(10_000, probes);
			server = createServer((_req, res) => res.end('ok'));
			collector.attachServer(server);
			await new Promise<void>((resolve) => server?.listen(0, resolve));
			const { port } = server.address() as AddressInfo;

			const get = (agent: Agent) =>
				new Promise<void>((resolve, reject) => {
					request({ port, agent }, (res) => {
						res.resume();
						res.on('end', resolve);
					})
						.on('error', reject)
						.end();
				});

			// One keep-alive socket carrying three requests, then one single-request socket.
			const keepAlive = new Agent({ keepAlive: true, maxSockets: 1 });
			await get(keepAlive);
			await get(keepAlive);
			await get(keepAlive);
			expect(collector.snapshot().http.openSockets).toBe(1);
			keepAlive.destroy();
			const single = new Agent({ keepAlive: false });
			await get(single);
			await new Promise((resolve) => setTimeout(resolve, 50));

			const { http } = collector.snapshot();
			expect(http).toMatchObject({
				openSockets: 0,
				totalConnections: 2,
				totalRequests: 4,
				closedSockets: 2,
				closedSocketRequests: 4,
				requestsPerSocket: { '1': 1, '2-10': 1 },
			});
		});
	});
});
