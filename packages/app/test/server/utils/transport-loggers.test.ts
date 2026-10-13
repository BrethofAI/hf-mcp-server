import { EventEmitter } from 'node:events';
import { mkdtempSync, readFileSync, existsSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { fileURLToPath } from 'node:url';
import pino, { type Logger } from 'pino';
import { describe, expect, it, vi } from 'vitest';
import {
	DATASET_FLUSH_REQUEST,
	DATASET_FLUSHED_EVENT,
	handleDatasetFlushRequests,
} from '../../../src/server/utils/dataset-flush-protocol.js';
import { closeTransportLoggers, registerTransportLogger } from '../../../src/server/utils/transport-loggers.js';

const fixture = fileURLToPath(new URL('../../fixtures/flush-ack-transport.mjs', import.meta.url));

function transportLogger(options: { ack: boolean; uploadMs: number; file: string }): Logger {
	return pino({ transport: { target: fixture, options } });
}

async function waitForReady(logger: Logger): Promise<void> {
	const stream = (logger as unknown as Record<symbol, EventEmitter>)[pino.symbols.streamSym];
	await new Promise((resolve) => stream?.once('ready', resolve));
}

describe('dataset flush protocol', () => {
	it('matches the literals used by the test transport', () => {
		expect(DATASET_FLUSH_REQUEST).toBe('HF_DATASET_FLUSH');
		expect(DATASET_FLUSHED_EVENT).toBe('hfDatasetFlushed');
	});

	it('runs the flush and reports remaining entries back to the main thread', async () => {
		const port = Object.assign(new EventEmitter(), { postMessage: vi.fn() });
		const flush = vi.fn(async () => 3);
		handleDatasetFlushRequests(port, 'System', flush);

		port.emit('message', { code: 'something-else' });
		port.emit('message', { code: DATASET_FLUSH_REQUEST });
		await vi.waitFor(() => expect(port.postMessage).toHaveBeenCalled());

		expect(flush).toHaveBeenCalledTimes(1);
		expect(port.postMessage).toHaveBeenCalledWith({
			code: 'EVENT',
			name: DATASET_FLUSHED_EVENT,
			args: [{ logType: 'System', remaining: 3 }],
		});
	});
});

describe('closeTransportLoggers (real pino transport workers)', () => {
	it('uploads everything logged before shutdown, then ends the worker', async () => {
		const file = join(mkdtempSync(join(tmpdir(), 'flush-ack-')), 'uploaded.json');
		const logger = registerTransportLogger(transportLogger({ ack: true, uploadMs: 300, file }), {
			datasetTargets: 1,
		});
		await waitForReady(logger);
		for (let i = 0; i < 50; i++) logger.info(`entry-${String(i)}`);

		// The main event loop must keep running while the upload is in progress.
		let ticks = 0;
		const ticker = setInterval(() => ticks++, 10);
		const startedAt = Date.now();
		await expect(closeTransportLoggers(5_000)).resolves.toEqual({ timedOut: 0, unsent: 0 });
		clearInterval(ticker);

		expect(Date.now() - startedAt).toBeLessThan(3_000);
		expect(ticks).toBeGreaterThanOrEqual(10);
		const uploaded = JSON.parse(readFileSync(file, 'utf8')) as string[];
		expect(uploaded).toHaveLength(50);
		expect(uploaded.at(-1)).toBe('entry-49');
		// Late writes after shutdown must not throw.
		expect(() => logger.info('late')).not.toThrow();
	});

	it('gives up on a transport that never acknowledges, within the deadline', async () => {
		const file = join(mkdtempSync(join(tmpdir(), 'flush-ack-')), 'uploaded.json');
		const logger = registerTransportLogger(transportLogger({ ack: false, uploadMs: 0, file }), {
			datasetTargets: 1,
		});
		await waitForReady(logger);
		logger.info('never uploaded');

		const startedAt = Date.now();
		await expect(closeTransportLoggers(400)).resolves.toEqual({ timedOut: 1, unsent: 0 });
		expect(Date.now() - startedAt).toBeLessThan(2_000);
		expect(existsSync(file)).toBe(false);
	});

	it('ignores plain destinations such as stdout', async () => {
		const destination = Object.assign(new EventEmitter(), { end: () => expect.unreachable('stdout ended') });
		registerTransportLogger({ [pino.symbols.streamSym]: destination } as unknown as Logger, { datasetTargets: 1 });

		await expect(closeTransportLoggers(50)).resolves.toEqual({ timedOut: 0, unsent: 0 });
	});
});
