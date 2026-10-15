import tls from "node:tls";
import { Agent, ProxyAgent, fetch as undiciFetch } from "undici";
import { SocksClient } from "socks";
import { config } from "../../../config/config.js";

// ========================= //
// = Copyright (c) NullDev = //
// ========================= //

export const BASE_URL = "https://gapi.svc.krunker.io";
export const TURF_EPOCH_UTC = Date.UTC(2026, 2, 17);

const RETRY_STATUSES = new Set([408, 500, 502, 503, 504, 522, 524]);

/**
 * undici Agent whose connections are opened through a SOCKS4/5 proxy
 *
 * @param {URL} proxy
 * @return {Agent}
 */
const socksAgent = function(proxy){
    const options = {
        host: proxy.hostname,
        port: Number(proxy.port) || 1080,
        type: /** @type {4 | 5} */ (proxy.protocol.startsWith("socks4") ? 4 : 5),
        userId: proxy.username ? decodeURIComponent(proxy.username) : undefined,
        password: proxy.password ? decodeURIComponent(proxy.password) : undefined,
    };
    return new Agent({
        connect(opts, callback){
            const destination = { host: String(opts.hostname), port: Number(opts.port) || (opts.protocol === "https:" ? 443 : 80) };
            SocksClient.createConnection({ proxy: options, command: "connect", destination })
                .then(({ socket }) => {
                    if (opts.protocol !== "https:"){
                        callback(null, socket);
                        return;
                    }
                    const secure = tls.connect({ socket, servername: String(opts.servername || opts.hostname), ALPNProtocols: ["http/1.1"] });
                    secure.once("secureConnect", () => callback(null, secure));
                    secure.once("error", err => callback(err, null));
                })
                .catch(err => callback(err, null));
        },
    });
};

/**
 * Dispatcher for an optional proxy URL: http(s) proxies use a CONNECT tunnel, socks4/socks5 a SOCKS tunnel
 *
 * @param {string} proxyUrl empty for a direct connection
 * @return {import("undici").Dispatcher | undefined}
 */
export const createDispatcher = function(proxyUrl){
    if (!proxyUrl) return undefined;
    const proxy = new URL(proxyUrl);
    if (proxy.protocol === "http:" || proxy.protocol === "https:") return new ProxyAgent(proxyUrl);
    if (/^socks(4a?|5h?):$/.test(proxy.protocol)) return socksAgent(proxy);
    throw new Error(`Unsupported proxy scheme "${proxy.protocol}" in clan.api_proxy (use http, https, socks4 or socks5)`);
};
const MAX_ATTEMPTS = 4;
const BACKOFF_MS = 300;
const RATE_LIMIT_PAUSE_MS = 5000;
const RATE_LIMIT_MAX_WAIT_MS = 60000;
const RATE_LIMIT_MAX_WAITS = 5;
const MAX_SPACING_MS = 2000;

/**
 * Whole UTC days since the turf season epoch
 *
 * @param {Date} [date=new Date()]
 * @return {number}
 */
export const turfDay = function(date = new Date()){
    return Math.floor((date.getTime() - TURF_EPOCH_UTC) / 86400000);
};

/**
 * In-game level from total score
 *
 * @param {number} score
 * @return {number}
 */
export const levelFromScore = function(score){
    return Math.floor(0.03 * Math.sqrt(Math.max(0, Number(score) || 0)));
};

/**
 * Error thrown when gapi does not answer with a usable status
 *
 * @class KrunkerApiError
 * @extends {Error}
 */
export class KrunkerApiError extends Error {
    /**
     * @param {string} message
     * @param {number} status
     */
    constructor(message, status){
        super(message);
        this.name = "KrunkerApiError";
        this.status = status;
    }
}

/**
 * Minimal client for the public Krunker gapi
 *
 * @class KrunkerApi
 */
export class KrunkerApi {
    /**
     * @param {{ fetchFn?: typeof fetch, sleep?: (ms: number) => Promise<void>, spacingMs?: number, proxyUrl?: string }} [options]
     *   proxyUrl: optional http(s) or socks proxy for every request, for hosts whose own IP is blocked by Krunker's edge
     */
    constructor({ fetchFn = undiciFetch, sleep = ms => new Promise(r => setTimeout(r, ms)), spacingMs = 200, proxyUrl = "" } = {}){
        this.fetchFn = fetchFn;
        this.sleep = sleep;
        this.spacingMs = spacingMs;
        this.lastCall = 0;
        this.dispatcher = createDispatcher(proxyUrl);
    }

    #queue = Promise.resolve();

    /**
     * Wait so that calls, even concurrent ones, are at least spacingMs apart
     *
     * @return {Promise<void>}
     */
    #space(){
        const turn = this.#queue.then(async() => {
            const wait = this.lastCall + this.spacingMs - Date.now();
            if (wait > 0) await this.sleep(wait);
            this.lastCall = Date.now();
        });
        this.#queue = turn.catch(() => {});
        return turn;
    }

    /**
     * GET a gapi path
     *
     * @param {string} path
     * @return {Promise<{ status: number, data: any, error: any }>}
     */
    async get(path){
        let lastStatus = 0;
        let rateLimitWaits = 0;
        for (let attempt = 0; attempt < MAX_ATTEMPTS; attempt++){
            await this.#space();
            let res;
            try {
                res = await this.fetchFn(BASE_URL + path, { headers: { Accept: "application/json" }, dispatcher: this.dispatcher });
            }
            catch {
                lastStatus = 0;
                await this.sleep(BACKOFF_MS * (2 ** attempt));
                continue;
            }
            lastStatus = res.status;
            if (res.status === 429){
                if (rateLimitWaits >= RATE_LIMIT_MAX_WAITS) break;
                const retryAfter = Number(res.headers?.get?.("retry-after")) * 1000;
                const wait = Math.min(retryAfter > 0 ? retryAfter : RATE_LIMIT_PAUSE_MS * (2 ** rateLimitWaits), RATE_LIMIT_MAX_WAIT_MS);
                rateLimitWaits++;
                this.spacingMs = Math.min(this.spacingMs * 2, MAX_SPACING_MS);
                await this.sleep(wait);
                attempt--;
                continue;
            }
            if (RETRY_STATUSES.has(res.status)){
                await this.sleep(BACKOFF_MS * (2 ** attempt));
                continue;
            }
            const body = await res.json().catch(() => ({}));
            if (res.status === 404) return { status: 404, data: null, error: body?.error ?? null };
            if (res.status < 200 || res.status >= 300) throw new KrunkerApiError(`gapi ${path} answered ${res.status}`, res.status);
            return { status: res.status, data: body?.data ?? null, error: body?.error ?? null };
        }
        throw new KrunkerApiError(`gapi ${path} failed after ${MAX_ATTEMPTS} attempts and ${rateLimitWaits} rate-limit waits (last status ${lastStatus})`, lastStatus);
    }

    /**
     * Public clan profile
     *
     * @param {string} name
     * @return {Promise<any | null>}
     */
    async getClan(name){
        return (await this.get(`/clans/by-name/${encodeURIComponent(name)}`)).data;
    }

    /**
     * Player profile, case-insensitive name
     *
     * @param {string} name
     * @return {Promise<any | null>}
     */
    async getPlayer(name){
        return (await this.get(`/players/${encodeURIComponent(name)}`)).data;
    }

    /**
     * Player name search
     *
     * @param {string} query
     * @param {number} [limit=5]
     * @return {Promise<Array<{ player_id: number, player_name: string, player_clan?: string }>>}
     */
    async searchPlayers(query, limit = 5){
        const res = await this.get(`/players/search?q=${encodeURIComponent(query)}&limit=${limit}`);
        return res.data?.results ?? [];
    }

    /**
     * Turf rows of a clan for one day
     *
     * @param {number} clanId
     * @param {number} day
     * @return {Promise<Array<{ player_id: number, player_name: string, kills: number, deaths: number, games: number, kd: number }>>}
     */
    async getTurfPlayers(clanId, day){
        return (await this.get(`/territorial-wars/clan/${clanId}/players?day=${day}`)).data ?? [];
    }

    /**
     * Season turf payouts of a clan
     *
     * @param {number} clanId
     * @return {Promise<{ payouts: Array<{ player_id: number, player_name: string, total_kr: number }>, total: number }>}
     */
    async getTurfPayouts(clanId){
        return (await this.get(`/territorial-wars/clan/${clanId}/payouts`)).data ?? { payouts: [], total: 0 };
    }

    /**
     * Turf profile of a player, resolves an id to the current name
     *
     * @param {number} playerId
     * @return {Promise<any | null>}
     */
    async getTurfPlayer(playerId){
        return (await this.get(`/territorial-wars/player/${playerId}`)).data;
    }

    /**
     * Turf rows of a player for every day and map of the season, whichever clan they played for
     *
     * @param {number} playerId
     * @return {Promise<Array<{ day: number, clan_id: number, clan_name: string, map_id: number, map_name: string, kills: number, deaths: number, games: number, kd: number }>>}
     */
    async getTurfContributions(playerId){
        return (await this.get(`/territorial-wars/player/${playerId}/contributions`)).data ?? [];
    }
}

export const api = new KrunkerApi({ proxyUrl: config.clan?.api_proxy || "" });
