export class MeshCallError extends Error {
  public readonly code: string;
  public readonly retryable: boolean;
  public readonly details: Record<string, unknown> | null;

  public constructor(
    code: string,
    message: string,
    options: {
      retryable?: boolean;
      details?: Record<string, unknown> | null;
    } = {},
  ) {
    super(message);
    this.name = "MeshCallError";
    this.code = code;
    this.retryable = options.retryable ?? false;
    this.details = options.details ?? null;
  }
}
