package io.github.t4wefan.meshcall

import kotlinx.serialization.SerialName
import kotlinx.serialization.Serializable
import kotlinx.serialization.json.JsonObject
import kotlinx.serialization.json.JsonPrimitive
import kotlinx.serialization.json.buildJsonObject
import kotlinx.serialization.json.put

internal const val REGISTRY_SERVICE = "meshcall.router.v1.Registry"
internal const val ROUTER_AUTH_SERVICE = "meshcall.router.v1.AuthService"
internal const val TUNNEL_SERVICE = "meshcall.router.v1.TunnelService"

/** The Router normalises a service name without a version segment to `.v0`. */
internal fun normalizeScopeService(service: String): String =
    if (service == "*" || service.isEmpty() || Regex("""\.v\d""").containsMatchIn(service)) service else "$service.v0"

private val READ = CallOptions.DEFAULT.withIdempotent(true)

@Serializable
public data class RegistryBalance(val kind: String, val key: String? = null)

@Serializable
public data class RegistryInstance(
    val id: String,
    val inflight: Long = 0,
    @SerialName("max_inflight") val maxInflight: Long? = null,
    val breaker: String = "",
    val draining: Boolean = false,
)

@Serializable
public data class RegistryMethod(
    val name: String,
    val description: String? = null,
    val stream: String,
    val balance: RegistryBalance? = null,
    val annotations: JsonObject? = null,
    val parameters: List<String>? = null,
    @SerialName("duplex_reason") val duplexReason: String? = null,
    val instances: List<String>? = null,
)

@Serializable
public data class RegistryService(
    val name: String,
    val version: String? = null,
    val description: String? = null,
    val targeted: Boolean = false,
    val instances: List<RegistryInstance> = emptyList(),
    val methods: List<RegistryMethod> = emptyList(),
    val contract: JsonObject? = null,
)

@Serializable
private data class RegistryList(val services: List<RegistryService> = emptyList())

/** The Router's built-in `meshcall.router.v1.Registry` (`mesh.registry`). */
public class RegistryClient internal constructor(private val rpc: MeshCallRpc) {
    public suspend fun list(options: CallOptions = CallOptions.DEFAULT): List<RegistryService> =
        rpc.unary(REGISTRY_SERVICE, "list", JsonObject(emptyMap()), JsonObject.serializer(), RegistryList.serializer(), READ.merge(options)).services

    public suspend fun describe(service: String, options: CallOptions = CallOptions.DEFAULT): RegistryService = rpc.unary(
        REGISTRY_SERVICE, "describe", buildJsonObject { put("service", normalizeScopeService(service)) },
        JsonObject.serializer(), RegistryService.serializer(), READ.merge(options),
    )
}

/** One service of the target instance and the methods a tunnel allows (`"*"`: all). */
@Serializable
public data class TunnelScope(val service: String, val methods: List<String>) {
    /** Every method of [service]. */
    public constructor(service: String) : this(service, listOf("*"))
}

/** A tunnel as the Router lists it (never with its credential). */
@Serializable
public data class TunnelInfo(
    @SerialName("tunnel_id") val tunnelId: String,
    @SerialName("instance_id") val instanceId: String,
    val principal: String,
    val scopes: List<TunnelScope> = emptyList(),
    @SerialName("expires_at_unix_ms") val expiresAtUnixMs: Long,
    @SerialName("max_connections") val maxConnections: Long? = null,
    /** The instance public key the issuer pinned: the tunnel carries sealed calls only. */
    @SerialName("e2e_key") val e2eKey: String? = null,
    val connections: Long = 0,
    val online: Boolean = false,
)

/** The result of [TunnelsClient.open]: hand [credential] to the client once. */
@Serializable
public data class OpenedTunnel(
    val credential: String,
    @SerialName("tunnel_id") val tunnelId: String,
    @SerialName("instance_id") val instanceId: String,
    val principal: String,
    val scopes: List<TunnelScope> = emptyList(),
    @SerialName("expires_at_unix_ms") val expiresAtUnixMs: Long,
    @SerialName("max_connections") val maxConnections: Long? = null,
    @SerialName("e2e_key") val e2eKey: String? = null,
    val connections: Long = 0,
    val online: Boolean = false,
)

@Serializable
private data class OpenTunnelBody(
    @SerialName("instance_id") val instanceId: String,
    val scopes: List<TunnelScope>,
    val principal: String,
    @SerialName("ttl_seconds") val ttlSeconds: Long,
    @SerialName("max_connections") val maxConnections: Long? = null,
    @SerialName("e2e_key") val e2eKey: String? = null,
)

@Serializable
private data class TunnelList(val tunnels: List<TunnelInfo> = emptyList())

@Serializable
private data class Revoked(val revoked: Boolean = false)

/**
 * The Router's built-in `meshcall.router.v1.TunnelService` (`mesh.tunnels`).
 * An issuer (an account or API key) opens tunnels; an app normally receives
 * the credential from its issuer instead.
 */
public class TunnelsClient internal constructor(private val rpc: MeshCallRpc) {
    /**
     * Opens a tunnel to one instance; returns once the instance accepted it.
     * Fails with `PermissionDeniedException` (`tunnel_rejected`) when the
     * instance refuses and `UnavailableException` (`instance_unknown`) when it
     * is not registered. [e2eKey] pins the instance's public key: every call on
     * the tunnel is then sealed end to end.
     */
    public suspend fun open(
        instance: String,
        scopes: List<TunnelScope>,
        principal: String,
        ttlSeconds: Long = 3600,
        maxConnections: Long? = null,
        e2eKey: String? = null,
        options: CallOptions = CallOptions.DEFAULT,
    ): OpenedTunnel = rpc.unary(
        TUNNEL_SERVICE, "open",
        // The Router checks the request (invalid_argument), so every runtime fails alike.
        OpenTunnelBody(instance, scopes.map { it.copy(service = normalizeScopeService(it.service)) }, principal, ttlSeconds, maxConnections, e2eKey),
        OpenTunnelBody.serializer(), OpenedTunnel.serializer(), options,
    )

    /** Closes a tunnel and every connection on it; false if it is not yours or gone. */
    public suspend fun revoke(tunnelId: String, options: CallOptions = CallOptions.DEFAULT): Boolean = rpc.unary(
        TUNNEL_SERVICE, "revoke", buildJsonObject { put("tunnel_id", tunnelId) },
        JsonObject.serializer(), Revoked.serializer(), READ.merge(options),
    ).revoked

    /** The tunnels this identity opened that are still open. */
    public suspend fun list(options: CallOptions = CallOptions.DEFAULT): List<TunnelInfo> = rpc.unary(
        TUNNEL_SERVICE, "list", JsonObject(emptyMap()), JsonObject.serializer(), TunnelList.serializer(), READ.merge(options),
    ).tunnels
}

/** A scope of a temporary token. */
@Serializable
public data class RouterScope(
    val service: String,
    val methods: List<String>? = null,
    val register: Boolean? = null,
    /** Restrict a registration scope to these instance ids. */
    @SerialName("instance_ids") val instanceIds: List<String>? = null,
)

@Serializable
public data class IssuedToken(
    val token: String,
    @SerialName("token_id") val tokenId: String,
    @SerialName("expires_at_unix_ms") val expiresAtUnixMs: Long,
    val scopes: List<RouterScope> = emptyList(),
)

@Serializable
public data class RenewedToken(
    @SerialName("token_id") val tokenId: String,
    @SerialName("expires_at_unix_ms") val expiresAtUnixMs: Long,
)

/** An outstanding temporary token as its issuer sees it (never the secret). */
@Serializable
public data class TokenInfo(
    @SerialName("token_id") val tokenId: String,
    val issuer: String,
    @SerialName("instance_ids") val instanceIds: List<String>? = null,
    @SerialName("expires_at_unix_ms") val expiresAtUnixMs: Long,
    /** Past its expiry: it admits no new connection, but these stay up until revoked. */
    val expired: Boolean = false,
    val connections: Long = 0,
)

@Serializable
public data class RouterIdentity(
    val username: String,
    val roles: List<String> = emptyList(),
    val register: List<String> = emptyList(),
    val call: List<String> = emptyList(),
    @SerialName("expires_at_unix_ms") val expiresAtUnixMs: Long? = null,
    @SerialName("instance_ids") val instanceIds: List<String>? = null,
    /** The tunnel this connection belongs to, on a tunnel connection. */
    val tunnel: TunnelInfo? = null,
)

@Serializable
private data class IssueTokenBody(val scopes: List<RouterScope>, @SerialName("ttl_seconds") val ttlSeconds: Long)

@Serializable
private data class TokenList(val tokens: List<TokenInfo> = emptyList())

/** The Router's built-in `meshcall.router.v1.AuthService` (`mesh.auth`). */
public class AuthClient internal constructor(private val rpc: MeshCallRpc) {
    public suspend fun whoami(options: CallOptions = CallOptions.DEFAULT): RouterIdentity = rpc.unary(
        ROUTER_AUTH_SERVICE, "whoami", JsonObject(emptyMap()), JsonObject.serializer(), RouterIdentity.serializer(), READ.merge(options),
    )

    /** Issues a scoped Bearer token; expiry bounds new connections only. */
    public suspend fun issueToken(
        scopes: List<RouterScope>,
        ttlSeconds: Long = 900,
        options: CallOptions = CallOptions.DEFAULT,
    ): IssuedToken = rpc.unary(
        ROUTER_AUTH_SERVICE, "issue_token",
        IssueTokenBody(scopes.map { it.copy(service = normalizeScopeService(it.service)) }, ttlSeconds),
        IssueTokenBody.serializer(), IssuedToken.serializer(), options,
    )

    public suspend fun renewToken(tokenId: String, ttlSeconds: Long, options: CallOptions = CallOptions.DEFAULT): RenewedToken =
        rpc.unary(
            ROUTER_AUTH_SERVICE, "renew_token",
            buildJsonObject {
                put("token_id", tokenId)
                put("ttl_seconds", JsonPrimitive(ttlSeconds))
            },
            JsonObject.serializer(), RenewedToken.serializer(), options,
        )

    /** Revokes a token and closes every connection made with it (4401 `token_revoked`). */
    public suspend fun revokeToken(tokenId: String, options: CallOptions = CallOptions.DEFAULT): Boolean = rpc.unary(
        ROUTER_AUTH_SERVICE, "revoke_token", buildJsonObject { put("token_id", tokenId) },
        JsonObject.serializer(), Revoked.serializer(), options,
    ).revoked

    public suspend fun listTokens(options: CallOptions = CallOptions.DEFAULT): List<TokenInfo> = rpc.unary(
        ROUTER_AUTH_SERVICE, "list_tokens", JsonObject(emptyMap()), JsonObject.serializer(), TokenList.serializer(), options,
    ).tokens
}
