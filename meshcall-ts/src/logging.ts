import { format } from "node:util";

import type { LogContext, LogLevel, RpcLogger } from "./log.js";

export { safelyLog, type LogContext, type LogLevel, type RpcLogger } from "./log.js";

export class ConsoleRpcLogger implements RpcLogger {
  public constructor(
    private readonly context: LogContext = {},
    private readonly colorize = false,
  ) {}

  public bind(context: LogContext): RpcLogger {
    return new ConsoleRpcLogger({ ...this.context, ...context }, this.colorize);
  }

  public log(level: LogLevel, message: string, ...args: unknown[]): void {
    const text = level.toUpperCase() + " " + format(message, ...args);
    const colors = { debug: 36, info: 32, warning: 33, error: 31 };
    // stderr keeps application stdout usable by headless clients and scripts.
    console.error(
      this.colorize ? "\u001b[" + colors[level] + "m" + text + "\u001b[0m" : text,
      JSON.stringify(this.context),
    );
  }

  public debug(message: string, ...args: unknown[]): void {
    this.log("debug", message, ...args);
  }
  public info(message: string, ...args: unknown[]): void {
    this.log("info", message, ...args);
  }
  public warning(message: string, ...args: unknown[]): void {
    this.log("warning", message, ...args);
  }
  public error(message: string, ...args: unknown[]): void {
    this.log("error", message, ...args);
  }
}
