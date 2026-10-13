import { request } from 'node:http';
import type { AddressInfo } from 'node:net';
import { describe, expect, it } from 'vitest';
import { DEFAULT_SHUTDOWN_GRACE_MS, httpDrainMs, shutdownGraceMs } from '../../src/server/run-application.js';
import { WebServer } from '../../src/server/web-server.js';

describe('shutdown budget', () => {
	it('defaults the grace period and ignores invalid values', () => {
		expect(shutdownGraceMs({})).toBe(DEFAULT_SHUTDOWN_GRACE_MS);
		expect(shutdownGraceMs({ SHUTDOWN_GRACE_MS: 'soon' })).toBe(DEFAULT_SHUTDOWN_GRACE_MS);
		expect(shutdownGraceMs({ SHUTDOWN_GRACE_MS: '0' })).toBe(DEFAULT_SHUTDOWN_GRACE_MS);
		expect(shutdownGraceMs({ SHUTDOWN_GRACE_MS: '8000' })).toBe(8000);
	});

	it('keeps most of the budget for flushing logs', () => {
		expect(httpDrainMs(25_000)).toBe(5_000);
		expect(httpDrainMs(6_000)).toBe(2_000);
	});
});

describe('WebServer.stop', () => {
	it('cuts off requests still running after the drain deadline', async () => {
		const webServer = new WebServer();
		// A tool call that outlives the drain budget.
		let markReceived: () => void = () => undefined;
		const received = new Promise<void>((resolve) => (markReceived = resolve));
		webServer.getApp().get('/hang', () => markReceived());
		await webServer.start(0);
		const server = (webServer as unknown as { server: import('node:http').Server }).server;
		const { port } = server.address() as AddressInfo;

		const clientResult = new Promise<string>((resolve) => {
			request({ port, path: '/hang' }, (res) => resolve(`status ${String(res.statusCode)}`))
				.on('error', (error) => resolve(error.message))
				.end();
		});
		await received;

		const startedAt = Date.now();
		await webServer.stop(200);
		const elapsed = Date.now() - startedAt;

		expect(elapsed).toBeGreaterThanOrEqual(150);
		expect(elapsed).toBeLessThan(2_000);
		await expect(clientResult).resolves.toMatch(/socket hang up|ECONNRESET/);
	});
});
