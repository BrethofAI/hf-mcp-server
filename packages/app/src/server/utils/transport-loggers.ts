import pino, { type Logger } from 'pino';
import { DATASET_FLUSH_REQUEST, DATASET_FLUSHED_EVENT, type DatasetFlushedEvent } from './dataset-flush-protocol.js';

/**
 * Pino transport streams (worker threads) that must be shut down explicitly.
 *
 * Transport threads never receive process signals and pino's exit hook cannot
 * wait for asynchronous work, so buffered dataset logs are only uploaded if the
 * main thread asks for it during shutdown. The stream is captured at
 * registration because logger objects can be reconfigured later
 * (`forceLoggerToStderr`).
 */
interface TransportStream {
	worker: { postMessage(message: unknown): void };
	closed?: boolean;
	flush(callback: (error?: Error | null) => void): void;
	end(): void;
	on(event: string, listener: (...args: unknown[]) => void): unknown;
	once(event: string, listener: (...args: unknown[]) => void): unknown;
	off(event: string, listener: (...args: unknown[]) => void): unknown;
}

interface Registration {
	stream: TransportStream;
	/** Dataset targets hosted by this worker; each answers one flush request. */
	datasetTargets: number;
}

const registrations = new Set<Registration>();

function transportStreamOf(logger: Logger): TransportStream | undefined {
	// Registration must never break logger creation, even with a stubbed pino.
	const streamSym = (pino as { symbols?: { streamSym?: symbol } }).symbols?.streamSym;
	if (!streamSym) return undefined;
	const stream = (logger as unknown as Record<symbol, unknown>)[streamSym];
	if (typeof stream !== 'object' || stream === null) return undefined;
	const candidate = stream as Partial<TransportStream>;
	// Only thread-stream transports own a worker; never end a stdout destination.
	if (
		typeof candidate.worker?.postMessage !== 'function' ||
		typeof candidate.end !== 'function' ||
		typeof candidate.flush !== 'function' ||
		typeof candidate.once !== 'function'
	) {
		return undefined;
	}
	return candidate as TransportStream;
}

export function registerTransportLogger<T extends Logger>(logger: T, options: { datasetTargets?: number } = {}): T {
	const stream = transportStreamOf(logger);
	if (stream) registrations.add({ stream, datasetTargets: options.datasetTargets ?? 0 });
	return logger;
}

interface FlushResult {
	completed: boolean;
	/** Entries the transports reported as not uploaded (-1 entries mean unknown). */
	unsent: number;
}

function requestDatasetFlush(registration: Registration, deadline: number): Promise<FlushResult> {
	const { stream } = registration;
	return new Promise<FlushResult>((resolve) => {
		let acknowledgements = 0;
		let unsent = 0;
		const finish = (completed: boolean): void => {
			clearTimeout(timer);
			stream.off(DATASET_FLUSHED_EVENT, onFlushed);
			resolve({ completed, unsent });
		};
		const onFlushed = (...args: unknown[]): void => {
			const remaining = (args[0] as Partial<DatasetFlushedEvent> | undefined)?.remaining;
			if (typeof remaining === 'number' && remaining > 0) unsent += remaining;
			if (++acknowledgements >= registration.datasetTargets) finish(true);
		};
		const timer = setTimeout(() => finish(false), Math.max(0, deadline - Date.now()));
		stream.on(DATASET_FLUSHED_EVENT, onFlushed);
		// Hand buffered lines to the worker before asking it to upload.
		stream.flush((error) => {
			if (error) {
				finish(false);
				return;
			}
			stream.worker.postMessage({ code: DATASET_FLUSH_REQUEST });
		});
	});
}

export interface CloseTransportsResult {
	/** Transports whose final upload did not finish before the deadline. */
	timedOut: number;
	/** Entries reported as not uploaded by transports that did finish (e.g. upload failed). */
	unsent: number;
}

/**
 * Upload buffered dataset logs (all transports in parallel, bounded by
 * `timeoutMs`), then end every transport stream.
 */
export async function closeTransportLoggers(timeoutMs: number): Promise<CloseTransportsResult> {
	const deadline = Date.now() + Math.max(0, timeoutMs);
	const pending = [...registrations];
	registrations.clear();

	const flushed = await Promise.all(
		pending.map((registration) =>
			registration.datasetTargets > 0 && !registration.stream.closed
				? requestDatasetFlush(registration, deadline)
				: Promise.resolve<FlushResult>({ completed: true, unsent: 0 })
		)
	);

	for (const { stream } of pending) {
		// Late log calls write to an ending stream, which emits 'error'; never crash shutdown on it.
		stream.on('error', () => undefined);
		// Uploads are done (or abandoned), so the worker's close hook returns immediately.
		if (!stream.closed) stream.end();
	}
	return {
		timedOut: flushed.filter((result) => !result.completed).length,
		unsent: flushed.reduce((total, result) => total + result.unsent, 0),
	};
}
