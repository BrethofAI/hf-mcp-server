// Test pino transport that speaks the dataset flush handshake
// (see src/server/utils/dataset-flush-protocol.ts). On a flush request it
// "uploads" everything received so far to opts.file after opts.uploadMs.
import build from 'pino-abstract-transport';
import { appendFileSync } from 'node:fs';
import { parentPort } from 'node:worker_threads';

export default async function (opts) {
	const received = [];
	if (opts.ack) {
		parentPort.on('message', (message) => {
			if (message?.code !== 'HF_DATASET_FLUSH') return;
			setImmediate(() => {
				setTimeout(() => {
					appendFileSync(opts.file, JSON.stringify(received.map((entry) => entry.msg)));
					parentPort.postMessage({
						code: 'EVENT',
						name: 'hfDatasetFlushed',
						args: [{ logType: 'test', remaining: 0 }],
					});
				}, opts.uploadMs);
			});
		});
	}
	return build(async (source) => {
		for await (const entry of source) received.push(entry);
	});
}
