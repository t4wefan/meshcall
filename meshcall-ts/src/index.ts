export {
  INITIAL_STREAM_CREDIT,
  MeshCallClient,
  MeshCallDuplexCall,
  MeshCallServerStream,
  MeshCallSubscription,
  type CallOptions,
  type DuplexCallOptions,
  type SubscriptionOptions,
  type ClientCallInfo,
  type ClientCallNext,
  type ClientInterceptor,
  type MeshCallClientOptions,
} from "./client.js";
export {
  contractEntry,
  method,
  mutates,
  normalizeServiceName,
  query,
  renderContract,
  service,
  stream,
  writeContract,
  type ClientStreamOptions,
  type DuplexOptions,
  type InputDeclaration,
  type InputMap,
  type JsonSchema,
  type MethodSemantics,
  type MutatesOptions,
  type NamedServiceOptions,
  type QueryOptions,
  type ServerStreamOptions,
  type ServiceOptions,
  type StreamOptions,
  type SubscriptionOptions as SubscriptionMethodOptions,
  type UnaryOptions,
} from "./contract.js";
export {
  DependencyScope,
  dependency,
  meta,
  type Dependency,
  type DependencyResult,
  type Injectable,
  type MetaDependency,
  type MetaOptions,
} from "./di.js";
/** The zod this package validates with; use it for your schemas so there is one copy. */
export { z } from "zod";
export { contractWarnings, lintService, renderWarning, type ContractWarning } from "./diagnostics.js";
export {
  CallCancelledError, DeadlineExceededError, ErrorCode, InternalError, InvalidArgumentError, MeshCallError,
  type MeshCallErrorOptions, MethodNotFoundError, PermissionDeniedError, ProtocolError, ResourceExhaustedError,
  UnauthenticatedError, UnavailableError, type Violation,
} from "./errors.js";
export {
  DEFAULT_IDLE_TIMEOUT_MS,
  DEFAULT_PING_INTERVAL_MS,
  DEFAULT_PING_TIMEOUT_MS,
  DEFAULT_RECONNECT,
  DEFAULT_RETRY,
  clientDeadlineGraceMs,
  type ReconnectPolicy,
  type RetryPolicy,
} from "./liveness.js";
export { ConsoleRpcLogger, type RpcLogger, type LogContext, type LogLevel } from "./logging.js";
export type { WebSocketEndpoint } from "./endpoint.js";
export {
  type BalanceKind,
  type BalancePolicy,
  type Frame,
  type Metadata,
  PROTOCOL_VERSION,
  type StreamKind,
} from "./protocol.js";
export {
  MeshCallServer,
  type MeshCallServerOptions,
  type RouterConnectionOptions,
  type RpcContext,
  type ServerCallNext,
  type ServerInterceptor,
  type DuplexChannel,
} from "./server.js";

export {
  DEFAULT_ROUTER_URL,
  findCliBinary,
  findRouterBinary,
  installRouter,
  platformPackageName,
  Router,
  routerCacheDir,
  routerVersion,
  run,
  RunningServer,
  serve,
  WebSocketRouter,
  type InstallRouterOptions,
  type RouterOptions,
  type ServeOptions,
  type WebSocketRouterOptions,
} from "./router.js";
export {
  REGISTRY_SERVICE,
  RouterAuthClient,
  RouterCredentials,
  ROUTER_AUTH_SERVICE,
  type IssuedToken,
  type RegistryBalance,
  type RegistryInstance,
  type RegistryMethod,
  type RegistryService,
  type RouterIdentity,
  type RouterScope,
} from "./router-auth.js";
export { Topic, type TopicOptions } from "./topic.js";
export { mergeCallOptions, type MeshCallRpc, type RpcStream } from "./rpc.js";
