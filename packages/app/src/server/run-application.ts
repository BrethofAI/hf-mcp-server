import { type TransportType } from '../shared/constants.js';
import { Application } from './application.js';
import { logger } from './utils/logger.js';
import { closeTransportLoggers } from './utils/transport-loggers.js';
import { WebServer } from './web-server.js';

interface RunApplicationOptions {
	transportType: Exclude<TransportType, 'unknown'>;
	port: number;
}

export const DEFAULT_SHUTDOWN_GRACE_MS = 25_000;
const MAX_HTTP_DRAIN_MS = 5_000;

/**
 * Total time allowed for a graceful shutdown before the process exits anyway.
 * Keep it below the orchestrator's stop timeout so the final log upload is not
 * cut off by SIGKILL.
 */
export function shutdownGraceMs(env: NodeJS.ProcessEnv = process.env): number {
	const parsed = Number.parseInt(env.SHUTDOWN_GRACE_MS ?? '', 10);
	return Number.isFinite(parsed) && parsed > 0 ? parsed : DEFAULT_SHUTDOWN_GRACE_MS;
}

/** Drain HTTP briefly so most of the budget remains for flushing dataset logs. */
export function httpDrainMs(graceMs: number): number {
	return Math.min(MAX_HTTP_DRAIN_MS, Math.floor(graceMs / 3));
}

function attachShutdownHandlers(app: Application): void {
	let shutdownStartedAt: number | undefined;

	const shutdown = async (signal: NodeJS.Signals) => {
		shutdownStartedAt = Date.now();
		const graceMs = shutdownGraceMs();
		// warn: production runs at LOG_LEVEL=warn and these lines measure the platform's stop grace period.
		logger.warn({ signal, graceMs }, 'Shutdown signal received');

		const deadline = setTimeout(() => {
			console.error(`[shutdown] grace period of ${graceMs}ms exceeded; exiting`);
			process.exit(1);
		}, graceMs);
		deadline.unref();

		let exitCode = 0;
		try {
			await app.stop({ drainMs: httpDrainMs(graceMs) });
			logger.warn({ elapsedMs: Date.now() - shutdownStartedAt }, 'HTTP drained; flushing log transports');
		} catch (error) {
			exitCode = 1;
			logger.error({ error }, 'Error during shutdown');
		}

		// Ending the transports performs the final dataset upload; the main
		// logger is one of them, so use stderr from here on.
		const remainingMs = graceMs - (Date.now() - shutdownStartedAt) - 500;
		const { timedOut, unsent } = await closeTransportLoggers(remainingMs);
		console.error(
			`[shutdown] complete in ${String(Date.now() - shutdownStartedAt)}ms` +
				(timedOut > 0 ? `; ${String(timedOut)} dataset log transport(s) timed out` : '') +
				(unsent > 0 ? `; ${String(unsent)} dataset log entries not uploaded` : '')
		);
		process.exit(exitCode);
	};

	const onSignal = (signal: NodeJS.Signals) => {
		if (shutdownStartedAt !== undefined) {
			if (signal === 'SIGINT') {
				console.error('[shutdown] force exit requested');
				process.exit(1);
			}
			return;
		}
		void shutdown(signal);
	};

	process.on('SIGINT', onSignal);
	process.on('SIGTERM', onSignal);
}

export async function runApplication(options: RunApplicationOptions): Promise<void> {
	const webServer = new WebServer();
	const app = new Application({
		transportType: options.transportType,
		webAppPort: options.port,
		webServerInstance: webServer,
	});

	await app.start();
	attachShutdownHandlers(app);
}
