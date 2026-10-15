/** Logging types and helpers with no platform dependency. */
export type LogLevel = "debug" | "info" | "warning" | "error";
export type LogContext = Readonly<Record<string, unknown>>;

/** Application loggers can implement this interface without a logging dependency. */
export interface RpcLogger {
  bind(context: LogContext): RpcLogger;
  log(level: LogLevel, message: string, ...args: unknown[]): void;
  debug(message: string, ...args: unknown[]): void;
  info(message: string, ...args: unknown[]): void;
  warning(message: string, ...args: unknown[]): void;
  error(message: string, ...args: unknown[]): void;
}

/** A logging sink must not interfere with call cleanup or terminal delivery. */
export function safelyLog(action: () => void): void {
  try {
    action();
  } catch {
    // A failing application-provided logger cannot change the RPC result.
  }
}
