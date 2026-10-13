/**
 * Shutdown handshake between the main thread and HF dataset transport workers.
 *
 * thread-stream's `end()` blocks the main thread while the worker runs its close
 * hook and destroys the worker after ~10 s, which is too short and too blocking
 * for a final dataset upload. So shutdown first asks each dataset transport to
 * upload (asynchronously, under the caller's deadline) and only then ends it.
 */
export const DATASET_FLUSH_REQUEST = 'HF_DATASET_FLUSH';
/** Emitted on the main-thread ThreadStream via thread-stream's `{ code: 'EVENT' }` messages. */
export const DATASET_FLUSHED_EVENT = 'hfDatasetFlushed';

export interface DatasetFlushedEvent {
	logType: string;
	/** Entries still buffered after the final upload attempt (non-zero means they were lost). */
	remaining: number;
}

export function isDatasetFlushRequest(message: unknown): boolean {
	return (
		typeof message === 'object' && message !== null && (message as { code?: unknown }).code === DATASET_FLUSH_REQUEST
	);
}

interface MessagePortLike {
	on(event: 'message', listener: (message: unknown) => void): unknown;
	postMessage(message: unknown): void;
}

/**
 * Worker side: answer flush requests by running `flush` and reporting back.
 * `flush` resolves to the number of entries left unsent.
 */
export function handleDatasetFlushRequests(
	port: MessagePortLike | null | undefined,
	logType: string,
	flush: () => Promise<number>
): void {
	port?.on('message', (message) => {
		if (!isDatasetFlushRequest(message)) return;
		// Let entries already handed to this transport reach the logger before uploading.
		setImmediate(() => {
			void flush()
				.catch(() => -1)
				.then((remaining) => {
					const event: DatasetFlushedEvent = { logType, remaining };
					port.postMessage({ code: 'EVENT', name: DATASET_FLUSHED_EVENT, args: [event] });
				});
		});
	});
}
