import express from "express";
import cors from "cors";
import dotenv from "dotenv";
import http from "node:http";
import https from "node:https";
import dgram from "node:dgram";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { registerSupportChatRoutes } from "./support-chat-routes.js";

dotenv.config();

const app = express();
const __dirname = path.dirname(fileURLToPath(import.meta.url));

const HOST = process.env.HOST || "0.0.0.0";
const PORT = clampInteger(process.env.PORT, 3000, 1, 65535);
const CORS_ORIGIN = process.env.CORS_ORIGIN || "*";
const ETHERSCAN_API_KEY = String(process.env.ETHERSCAN_KEY || "").trim();
const DEFAULT_LIMIT = clampInteger(process.env.DEFAULT_TX_LIMIT, 80, 10, 150);
const DEFAULT_WHALE_THRESHOLD = clampNumber(process.env.WHALE_THRESHOLD_ETH, 40, 1, 5000);
const REQUEST_TIMEOUT_MS = clampInteger(process.env.REQUEST_TIMEOUT_MS, 12000, 2000, 60000);
const CACHE_TTL_MS = clampInteger(process.env.CACHE_TTL_MS, 15000, 1000, 300000);
const DEFAULT_MTA_HOST = String(process.env.MTA_HOST || "46.174.50.52").trim();
const DEFAULT_MTA_PORT = clampInteger(process.env.MTA_PORT, 22101, 1, 65535);
const DEFAULT_MTA_QUERY_PORT = clampInteger(process.env.MTA_QUERY_PORT, DEFAULT_MTA_PORT + 123, 1, 65535);
const MTA_QUERY_TIMEOUT_MS = clampInteger(process.env.MTA_QUERY_TIMEOUT_MS, 4000, 1000, 15000);
const MTA_STATUS_CACHE_TTL_MS = clampInteger(process.env.MTA_STATUS_CACHE_TTL_MS, 10000, 1000, 120000);
const MTA_MASTERLIST_URL = String(process.env.MTA_MASTERLIST_URL || "https://master.multitheftauto.com/ase/mta/").trim();
const MTA_MASTERLIST_TIMEOUT_MS = clampInteger(process.env.MTA_MASTERLIST_TIMEOUT_MS, 12000, 2000, 60000);
const MTA_MASTERLIST_CACHE_TTL_MS = clampInteger(process.env.MTA_MASTERLIST_CACHE_TTL_MS, 300000, 5000, 3600000);
const DEFAULT_MTA_MASTERLIST_LIMIT = clampInteger(process.env.DEFAULT_MTA_MASTERLIST_LIMIT, 5000, 1, 10000);
const VK_ACCESS_TOKEN = String(process.env.VK_ACCESS_TOKEN || "").trim();
const VK_API_VERSION = String(process.env.VK_API_VERSION || "5.199").trim();
const DEFAULT_VK_DOMAIN = sanitizeVkDomain(process.env.VK_WALL_DOMAIN || "mta.miami");
const DEFAULT_VK_POST_COUNT = clampInteger(process.env.VK_WALL_POST_COUNT, 6, 1, 20);
const VK_WALL_CACHE_TTL_MS = clampInteger(process.env.VK_WALL_CACHE_TTL_MS, 60000, 5000, 600000);
const COINGECKO_MARKETS_URL = "https://api.coingecko.com/api/v3/coins/markets?vs_currency=usd&order=market_cap_desc&per_page=250&page=1&sparkline=false&price_change_percentage=1h,24h";
const COINGECKO_MARKETS_CACHE_TTL_MS = clampInteger(
  process.env.COINGECKO_MARKETS_CACHE_TTL_MS,
  20000,
  5000,
  300000
);
const RADIO_METADATA_TIMEOUT_MS = clampInteger(process.env.RADIO_METADATA_TIMEOUT_MS, 10000, 2000, 30000);
const RADIO_METADATA_CACHE_TTL_MS = clampInteger(process.env.RADIO_METADATA_CACHE_TTL_MS, 15000, 2000, 120000);
const SUPPORT_CHAT_ADMIN_TOKEN = String(process.env.SUPPORT_CHAT_ADMIN_TOKEN || "").trim();
const SUPPORT_CHAT_OPERATOR_NAME = String(process.env.SUPPORT_CHAT_OPERATOR_NAME || "Support").trim() || "Support";
const SUPPORT_CHAT_MAX_CONVERSATIONS = clampInteger(process.env.SUPPORT_CHAT_MAX_CONVERSATIONS, 1500, 100, 20000);
const SUPPORT_CHAT_MAX_MESSAGES = clampInteger(process.env.SUPPORT_CHAT_MAX_MESSAGES, 200, 20, 2000);

app.use(cors({ origin: CORS_ORIGIN }));
app.use(express.json());

const supportChatConfig = registerSupportChatRoutes({
  app,
  baseDir: __dirname,
  adminToken: SUPPORT_CHAT_ADMIN_TOKEN,
  operatorName: SUPPORT_CHAT_OPERATOR_NAME,
  maxConversations: SUPPORT_CHAT_MAX_CONVERSATIONS,
  maxMessagesPerConversation: SUPPORT_CHAT_MAX_MESSAGES
});

const EXCHANGES = new Set([
  "0x28c6c06298d514db089934071355e5743bf21d60",
  "0x21a31ee1afc51d94c2efccaa2092ad1028285549",
  "0xdef1c0ded9bec7f1a1670819833240f027b25eff"
]);

const responseCache = new Map();
const RADIO_STATIONS = new Map([
  [
    "bbc-world-service",
    {
      key: "bbc-world-service",
      name: "BBC World Service",
      streamUrl: "https://stream.live.vc.bbcmedia.co.uk/bbc_world_service"
    }
  ],
  [
    "npr-news",
    {
      key: "npr-news",
      name: "NPR News",
      streamUrl: "https://npr-ice.streamguys1.com/live.mp3"
    }
  ],
  [
    "somafm-space-station-soma",
    {
      key: "somafm-space-station-soma",
      name: "SomaFM Space Station Soma",
      streamUrl: "https://ice2.somafm.com/spacestation-128-mp3"
    }
  ],
  [
    "lofi-girl",
    {
      key: "lofi-girl",
      name: "Lofi Girl",
      streamUrl: "https://play.streamafrica.net/lofiradio"
    }
  ],
  [
    "somafm-deep-space-one",
    {
      key: "somafm-deep-space-one",
      name: "SomaFM Deep Space One",
      streamUrl: "https://ice2.somafm.com/deepspaceone-128-mp3"
    }
  ],
  [
    "chillhop",
    {
      key: "chillhop",
      name: "Chillhop",
      streamUrl: "https://stream.zeno.fm/0r0xa792kwzuv"
    }
  ]
]);
const MASTERLIST_TEXT_COLLATOR = new Intl.Collator("en", {
  sensitivity: "base",
  numeric: true
});
const MTA_MASTERLIST_FLAGS = {
  ASE_PLAYER_COUNT: 0x0004,
  ASE_MAX_PLAYER_COUNT: 0x0008,
  ASE_GAME_NAME: 0x0010,
  ASE_SERVER_NAME: 0x0020,
  ASE_GAME_MODE: 0x0040,
  ASE_MAP_NAME: 0x0080,
  ASE_SERVER_VER: 0x0100,
  ASE_PASSWORDED: 0x0200,
  ASE_SERIALS: 0x0400,
  ASE_PLAYER_LIST: 0x0800,
  ASE_RESPONDING: 0x1000,
  ASE_RESTRICTION: 0x2000,
  ASE_SEARCH_IGNORE_SECTIONS: 0x4000,
  ASE_KEEP_FLAG: 0x8000,
  ASE_HTTP_PORT: 0x080000,
  ASE_SPECIAL: 0x100000
};

class MtaMasterlistReader {
  constructor(buffer) {
    this.buffer = Buffer.isBuffer(buffer) ? buffer : Buffer.from(buffer || []);
    this.position = 0;
  }

  read(count) {
    let hex = "";
    const end = Math.min(this.position + count, this.buffer.length);

    for (let index = this.position; index < end; index += 1) {
      const value = this.buffer[index];
      if (value !== 0) {
        hex += value.toString(16).padStart(2, "0");
      }
    }

    this.position += count;
    return hex ? Number.parseInt(hex, 16) : 0;
  }

  readString() {
    const size = this.read(1);
    const end = Math.min(this.position + size, this.buffer.length);
    const value = this.buffer
      .subarray(this.position, end)
      .toString("utf8")
      .replace(/[\u0000-\u001f]+/g, " ")
      .replace(/\s+/g, " ")
      .trim();

    this.position += size;
    return value;
  }

  step(count) {
    return this.position + count <= this.buffer.length;
  }

  tell() {
    return this.position;
  }

  seek(position) {
    if (position < this.buffer.length) {
      this.position = position;
    }
  }
}

function sanitizeHost(value) {
  const normalized = String(value || "").trim().toLowerCase();
  if (!normalized) {
    return "";
  }

  if (normalized === "localhost") {
    return normalized;
  }

  if (/^\d{1,3}(?:\.\d{1,3}){3}$/.test(normalized)) {
    const octets = normalized.split(".").map((part) => Number.parseInt(part, 10));
    return octets.every((octet) => Number.isInteger(octet) && octet >= 0 && octet <= 255)
      ? normalized
      : "";
  }

  if (/^(?=.{1,253}$)(?!-)[a-z0-9-]{1,63}(?<!-)(?:\.(?!-)[a-z0-9-]{1,63}(?<!-))*$/i.test(normalized)) {
    return normalized;
  }

  return "";
}

function sanitizeLabel(value) {
  return String(value || "")
    .replace(/\s+/g, " ")
    .trim()
    .slice(0, 80);
}

function sanitizeMasterlistText(value) {
  return String(value || "")
    .replace(/[\u0000-\u001f]+/g, " ")
    .replace(/\s+/g, " ")
    .trim();
}

function sanitizeMasterlistSearch(value) {
  return String(value || "")
    .trim()
    .toLowerCase()
    .slice(0, 120);
}

function sanitizeMasterlistSort(value) {
  const normalized = String(value || "players-desc")
    .trim()
    .toLowerCase();

  return new Set(["players-desc", "players-asc", "name-asc", "name-desc", "address-asc"]).has(normalized)
    ? normalized
    : "players-desc";
}

function isTruthyQueryValue(value) {
  return new Set(["1", "true", "yes", "on"]).has(String(value || "").trim().toLowerCase());
}

function normalizeMasterlistLimit(rawValue, fallback = DEFAULT_MTA_MASTERLIST_LIMIT) {
  const normalized = String(rawValue || "").trim().toLowerCase();
  if (!normalized) {
    return fallback;
  }

  if (normalized === "all") {
    return 10000;
  }

  return clampInteger(normalized, fallback, 1, 10000);
}

function normalizePortValue(rawValue, fallback, fieldName) {
  if (rawValue === undefined || rawValue === null || String(rawValue).trim() === "") {
    return fallback;
  }

  const normalized = String(rawValue).trim();
  if (!/^\d{1,5}$/.test(normalized)) {
    throw new Error(`${fieldName} must be a number between 1 and 65535.`);
  }

  const numeric = Number.parseInt(normalized, 10);
  if (numeric < 1 || numeric > 65535) {
    throw new Error(`${fieldName} must be a number between 1 and 65535.`);
  }

  return numeric;
}

function buildMtaTarget({ host, port, queryPort, label } = {}) {
  const safeHost = sanitizeHost(host) || DEFAULT_MTA_HOST;
  const safePort = clampInteger(port, DEFAULT_MTA_PORT, 1, 65535);
  const safeQueryPort = clampInteger(queryPort, safePort + 123, 1, 65535);
  const safeLabel = sanitizeLabel(label);

  return {
    host: safeHost,
    port: safePort,
    queryPort: safeQueryPort,
    label: safeLabel,
    serverAddress: `${safeHost}:${safePort}`
  };
}

const DEFAULT_MTA_TARGET = buildMtaTarget({
  host: DEFAULT_MTA_HOST,
  port: DEFAULT_MTA_PORT,
  queryPort: DEFAULT_MTA_QUERY_PORT,
  label: "Miami RP"
});

function clampInteger(rawValue, fallback, min, max) {
  const parsed = Number.parseInt(rawValue, 10);
  const safeValue = Number.isFinite(parsed) ? parsed : fallback;
  return Math.min(max, Math.max(min, safeValue));
}

function clampNumber(rawValue, fallback, min, max) {
  const parsed = Number(rawValue);
  const safeValue = Number.isFinite(parsed) ? parsed : fallback;
  return Math.min(max, Math.max(min, safeValue));
}

function getRequestedMtaTarget(query = {}) {
  const hasCustomTarget =
    query.host !== undefined ||
    query.port !== undefined ||
    query.queryPort !== undefined ||
    query.label !== undefined;

  if (!hasCustomTarget) {
    return DEFAULT_MTA_TARGET;
  }

  const hostValue = query.host === undefined || query.host === null || String(query.host).trim() === ""
    ? DEFAULT_MTA_HOST
    : sanitizeHost(query.host);

  if (!hostValue) {
    throw new Error("Provide a valid MTA host.");
  }

  const portValue = normalizePortValue(query.port, DEFAULT_MTA_PORT, "MTA port");
  const queryPortValue = normalizePortValue(query.queryPort, portValue + 123, "MTA query port");

  return buildMtaTarget({
    host: hostValue,
    port: portValue,
    queryPort: queryPortValue,
    label: query.label
  });
}

function parseMtaServerSpec(rawSpec) {
  const spec = String(rawSpec || "").trim();
  if (!spec) {
    return null;
  }

  let label = "";
  let address = spec;

  const labelSeparatorIndex = spec.indexOf("@");
  if (labelSeparatorIndex > 0) {
    label = sanitizeLabel(spec.slice(0, labelSeparatorIndex));
    address = spec.slice(labelSeparatorIndex + 1).trim();
  }

  const parts = address.split(":");
  if (parts.length < 2 || parts.length > 3) {
    throw new Error(`Invalid MTA server spec: ${spec}. Use host:port, host:port:queryPort, or label@host:port.`);
  }

  const [rawHost, rawPort, rawQueryPort] = parts;
  const host = sanitizeHost(rawHost);
  if (!host) {
    throw new Error(`Invalid MTA host in server spec: ${spec}.`);
  }

  const port = normalizePortValue(rawPort, DEFAULT_MTA_PORT, "MTA port");
  const queryPort = normalizePortValue(rawQueryPort, port + 123, "MTA query port");

  return buildMtaTarget({
    host,
    port,
    queryPort,
    label
  });
}

function dedupeMtaTargets(targets) {
  const seen = new Set();
  return targets.filter((target) => {
    const cacheKey = `${target.host}:${target.port}:${target.queryPort}`;
    if (seen.has(cacheKey)) {
      return false;
    }

    seen.add(cacheKey);
    return true;
  });
}

function getRequestedMtaTargets(rawValue) {
  const values = Array.isArray(rawValue)
    ? rawValue
    : rawValue === undefined || rawValue === null
      ? []
      : [rawValue];

  const targets = values
    .map((value) => parseMtaServerSpec(value))
    .filter(Boolean);

  return dedupeMtaTargets(targets);
}

function buildMtaOfflinePayload(target, error) {
  const requestedTarget = buildMtaTarget(target);

  return {
    ...requestedTarget,
    protocol: "ase",
    online: false,
    cached: false,
    serverName: requestedTarget.label || "MTA server",
    gameName: "mta",
    gameType: "",
    mapName: "",
    version: "",
    passworded: false,
    playersOnline: 0,
    maxPlayers: 0,
    players: [],
    rules: {},
    latencyMs: null,
    fetchedAt: new Date().toISOString(),
    error: "MTA_STATUS_FAILED",
    message: error?.message || "Unable to query MTA server status."
  };
}

function isValidAddress(value) {
  return /^0x[a-fA-F0-9]{40}$/.test(String(value || "").trim());
}

function normalizeAddress(value) {
  return String(value || "").trim().toLowerCase();
}

function formatTimestamp(timestamp) {
  const numeric = Number(timestamp);
  if (!Number.isFinite(numeric) || numeric <= 0) {
    return "N/A";
  }

  return new Date(numeric * 1000).toLocaleString("en-US", {
    year: "numeric",
    month: "short",
    day: "2-digit",
    hour: "2-digit",
    minute: "2-digit",
    second: "2-digit"
  });
}

function clusterWallet(address) {
  const normalized = normalizeAddress(address);
  const lastChar = normalized.slice(-1);

  if (["a", "b", "c", "d"].includes(lastChar)) {
    return "SMART MONEY";
  }

  if (["e", "f", "0", "1"].includes(lastChar)) {
    return "RETAIL";
  }

  return "UNKNOWN";
}

function getJson(url, sourceLabel = "Upstream service") {
  return new Promise((resolve, reject) => {
    const request = https.get(
      url,
      {
        headers: {
          Accept: "application/json",
          "User-Agent": "whale-v8-blackrock/9.0"
        },
        timeout: REQUEST_TIMEOUT_MS
      },
      (response) => {
        let body = "";

        response.setEncoding("utf8");
        response.on("data", (chunk) => {
          body += chunk;
        });

        response.on("end", () => {
          if (response.statusCode && response.statusCode >= 400) {
            reject(new Error(`${sourceLabel} returned HTTP ${response.statusCode}`));
            return;
          }

          try {
            resolve(body ? JSON.parse(body) : {});
          } catch (error) {
            reject(new Error(`Invalid JSON returned by ${sourceLabel}`));
          }
        });
      }
    );

    request.on("timeout", () => {
      request.destroy(new Error(`${sourceLabel} request timed out`));
    });

    request.on("error", (error) => {
      reject(error);
    });
  });
}

function getJsonWithMeta(url, sourceLabel = "Upstream service") {
  return new Promise((resolve, reject) => {
    const request = https.get(
      url,
      {
        headers: {
          Accept: "application/json",
          "User-Agent": "whale-v8-blackrock/9.0"
        },
        timeout: REQUEST_TIMEOUT_MS
      },
      (response) => {
        let body = "";

        response.setEncoding("utf8");
        response.on("data", (chunk) => {
          body += chunk;
        });

        response.on("end", () => {
          let data = {};

          try {
            data = body ? JSON.parse(body) : {};
          } catch (_) {
            reject(new Error(`Invalid JSON returned by ${sourceLabel}`));
            return;
          }

          resolve({
            statusCode: Number(response.statusCode || 0),
            headers: response.headers || {},
            data
          });
        });
      }
    );

    request.on("timeout", () => {
      request.destroy(new Error(`${sourceLabel} request timed out`));
    });

    request.on("error", (error) => {
      reject(error);
    });
  });
}

function getBinary(url, sourceLabel = "Upstream service", timeoutMs = REQUEST_TIMEOUT_MS) {
  return new Promise((resolve, reject) => {
    const request = https.get(
      url,
      {
        headers: {
          Accept: "*/*",
          "User-Agent": "whale-v8-blackrock/9.0"
        },
        timeout: timeoutMs
      },
      (response) => {
        const chunks = [];

        response.on("data", (chunk) => {
          chunks.push(chunk);
        });

        response.on("end", () => {
          if (response.statusCode && response.statusCode >= 400) {
            reject(new Error(`${sourceLabel} returned HTTP ${response.statusCode}`));
            return;
          }

          resolve(Buffer.concat(chunks));
        });
      }
    );

    request.on("timeout", () => {
      request.destroy(new Error(`${sourceLabel} request timed out`));
    });

    request.on("error", (error) => {
      reject(error);
    });
  });
}

function sanitizeRadioStationKey(value) {
  return String(value || "")
    .trim()
    .toLowerCase()
    .replace(/[^a-z0-9-]/g, "")
    .slice(0, 80);
}

function sanitizeRadioMetadataText(value) {
  return String(value || "")
    .replace(/\u0000+/g, " ")
    .replace(/&amp;/gi, "&")
    .replace(/&quot;/gi, "\"")
    .replace(/&#39;/gi, "'")
    .replace(/&#x27;/gi, "'")
    .replace(/\s+/g, " ")
    .trim()
    .slice(0, 180);
}

function getRadioStationDefinition(rawValue) {
  const key = sanitizeRadioStationKey(rawValue);
  if (!key) {
    return null;
  }

  return RADIO_STATIONS.get(key) || null;
}

function buildRadioMetadataPayload(station, options = {}) {
  const title = sanitizeRadioMetadataText(options.title || "");
  const hasTitle = Boolean(title);
  const message = sanitizeRadioMetadataText(
    options.message || (hasTitle ? "Track title updated." : "Track title unavailable for this station.")
  );

  return {
    station: station.key,
    stationName: station.name,
    streamUrl: station.streamUrl,
    title,
    displayTitle: hasTitle ? title : "Track title unavailable",
    hasTitle,
    source: sanitizeRadioMetadataText(options.source || "icy"),
    message,
    fetchedAt: new Date().toISOString(),
    cached: Boolean(options.cached),
    stale: Boolean(options.stale)
  };
}

function parseIcyMetadataBlock(buffer) {
  const raw = Buffer.isBuffer(buffer) ? buffer.toString("utf8") : Buffer.from(buffer || []).toString("utf8");
  const titleMatch = /StreamTitle='([^']*)';?/i.exec(raw) || /StreamTitle="([^"]*)";?/i.exec(raw);

  return {
    raw: sanitizeRadioMetadataText(raw),
    title: sanitizeRadioMetadataText(titleMatch ? titleMatch[1] : ""),
    source: "icy"
  };
}

function readIcyMetadata(streamUrl, stationName, timeoutMs = RADIO_METADATA_TIMEOUT_MS, redirectCount = 0) {
  return new Promise((resolve, reject) => {
    let targetUrl;

    try {
      targetUrl = new URL(streamUrl);
    } catch (_) {
      reject(new Error(`${stationName} has an invalid stream URL.`));
      return;
    }

    const transport = targetUrl.protocol === "http:" ? http : targetUrl.protocol === "https:" ? https : null;
    if (!transport) {
      reject(new Error(`${stationName} stream URL must use http or https.`));
      return;
    }

    const request = transport.get(
      targetUrl,
      {
        headers: {
          Accept: "*/*",
          "Icy-MetaData": "1",
          "User-Agent": "whale-v8-blackrock/9.0"
        },
        timeout: timeoutMs
      },
      (response) => {
        const statusCode = Number(response.statusCode || 0);
        const redirectLocation = String(response.headers.location || "").trim();

        if (statusCode >= 300 && statusCode < 400 && redirectLocation) {
          response.resume();

          if (redirectCount >= 5) {
            reject(new Error(`${stationName} metadata redirect limit exceeded.`));
            return;
          }

          const redirectedUrl = new URL(redirectLocation, targetUrl).toString();
          readIcyMetadata(redirectedUrl, stationName, timeoutMs, redirectCount + 1).then(resolve, reject);
          return;
        }

        if (statusCode >= 400) {
          response.resume();
          reject(new Error(`${stationName} returned HTTP ${statusCode}.`));
          return;
        }

        const metaint = Number.parseInt(String(response.headers["icy-metaint"] || "").trim(), 10);
        if (!Number.isFinite(metaint) || metaint <= 0) {
          response.resume();
          reject(new Error(`${stationName} does not expose ICY metadata.`));
          return;
        }

        let settled = false;
        let bytesUntilMetadata = metaint;
        let metadataBytesRemaining = -1;
        let metadataChunks = [];
        let emptyBlocks = 0;

        const settle = (handler, value) => {
          if (settled) {
            return;
          }

          settled = true;
          response.destroy();
          handler(value);
        };

        response.on("data", (chunk) => {
          if (settled) {
            return;
          }

          let offset = 0;

          while (offset < chunk.length) {
            if (bytesUntilMetadata > 0) {
              const consumed = Math.min(bytesUntilMetadata, chunk.length - offset);
              bytesUntilMetadata -= consumed;
              offset += consumed;
              continue;
            }

            if (metadataBytesRemaining < 0) {
              metadataBytesRemaining = chunk[offset] * 16;
              metadataChunks = [];
              offset += 1;

              if (metadataBytesRemaining === 0) {
                emptyBlocks += 1;

                if (emptyBlocks >= 2) {
                  settle(resolve, {
                    raw: "",
                    title: "",
                    source: "icy"
                  });
                  return;
                }

                bytesUntilMetadata = metaint;
                metadataBytesRemaining = -1;
              }

              continue;
            }

            const consumed = Math.min(metadataBytesRemaining, chunk.length - offset);
            metadataChunks.push(chunk.subarray(offset, offset + consumed));
            metadataBytesRemaining -= consumed;
            offset += consumed;

            if (metadataBytesRemaining === 0) {
              settle(resolve, parseIcyMetadataBlock(Buffer.concat(metadataChunks)));
              return;
            }
          }
        });

        response.on("end", () => {
          if (!settled) {
            reject(new Error(`${stationName} metadata stream ended before a track title was received.`));
          }
        });

        response.on("error", (error) => {
          if (!settled) {
            reject(error);
          }
        });
      }
    );

    request.on("timeout", () => {
      request.destroy(new Error(`${stationName} metadata request timed out.`));
    });

    request.on("error", (error) => {
      reject(error);
    });
  });
}

async function getRadioNowPlayingPayload(station) {
  const cacheKey = `radio-meta:${station.key}`;
  const cachedPayload = getCachedPayload(cacheKey, RADIO_METADATA_CACHE_TTL_MS);

  if (cachedPayload) {
    return {
      ...cachedPayload,
      cached: true,
      stale: false
    };
  }

  try {
    const metadata = await readIcyMetadata(station.streamUrl, station.name, RADIO_METADATA_TIMEOUT_MS);
    const payload = buildRadioMetadataPayload(station, {
      title: metadata.title,
      source: metadata.source,
      message: metadata.title
        ? "Track title updated."
        : "This station does not provide a live track title right now."
    });

    setCachedPayload(cacheKey, payload);
    return payload;
  } catch (error) {
    const staleEntry = getRawCachedEntry(cacheKey);
    if (staleEntry?.payload) {
      return {
        ...staleEntry.payload,
        cached: true,
        stale: true,
        message: sanitizeRadioMetadataText(error?.message || staleEntry.payload.message)
      };
    }

    return buildRadioMetadataPayload(station, {
      title: "",
      source: "icy",
      message: error?.message || "Track title unavailable for this station."
    });
  }
}

function buildEtherscanUrl(address) {
  const url = new URL("https://api.etherscan.io/v2/api");
  url.searchParams.set("chainid", "1");
  url.searchParams.set("module", "account");
  url.searchParams.set("action", "txlist");
  url.searchParams.set("address", address);
  url.searchParams.set("sort", "desc");
  url.searchParams.set("apikey", ETHERSCAN_API_KEY);
  return url.toString();
}

function buildSignal(score) {
  if (score >= 70) {
    return "INSTITUTIONAL ACCUMULATION";
  }

  if (score <= 30) {
    return "DISTRIBUTION PHASE";
  }

  return "NEUTRAL FLOW";
}

function buildScore({ netFlow, whaleCount, exchangeFlow, smartMoneyCount, whaleThreshold }) {
  const normalizedNet = clampNumber(netFlow / Math.max(whaleThreshold, 1), 0, -25, 25);
  const whalePower = clampNumber(whaleCount * 6, 0, 0, 30);
  const smartMoneyBonus = clampNumber(smartMoneyCount * 4, 0, 0, 16);
  const exchangePenalty = clampNumber(exchangeFlow * 0.12, 0, 0, 24);

  const score = 50 + normalizedNet + whalePower + smartMoneyBonus - exchangePenalty;
  return Math.round(clampNumber(score, 50, 0, 100));
}

function getCachedPayload(cacheKey, ttlMs = CACHE_TTL_MS) {
  const entry = responseCache.get(cacheKey);
  if (!entry) {
    return null;
  }

  if (Date.now() - entry.savedAt > ttlMs) {
    responseCache.delete(cacheKey);
    return null;
  }

  return entry.payload;
}

function setCachedPayload(cacheKey, payload) {
  responseCache.set(cacheKey, {
    savedAt: Date.now(),
    payload
  });
}

function getRawCachedEntry(cacheKey) {
  return responseCache.get(cacheKey) || null;
}

function sanitizeVkDomain(value) {
  const normalized = String(value || "")
    .trim()
    .replace(/^https?:\/\/(www\.)?vk\.com\//i, "")
    .replace(/^https?:\/\/(www\.)?vk\.ru\//i, "")
    .replace(/^@/, "")
    .replace(/^\/+/, "")
    .split(/[/?#]/)[0];

  if (!/^[a-zA-Z0-9._-]{2,80}$/.test(normalized)) {
    return "";
  }

  return normalized;
}

function parseRetryAfterMs(value) {
  if (!value) {
    return 0;
  }

  const seconds = Number.parseInt(String(value).trim(), 10);
  if (Number.isFinite(seconds) && seconds > 0) {
    return seconds * 1000;
  }

  const parsedDate = Date.parse(String(value).trim());
  if (Number.isFinite(parsedDate)) {
    return Math.max(0, parsedDate - Date.now());
  }

  return 0;
}

function buildVkWallUrl({ domain, count, offset, filter }) {
  const url = new URL("https://api.vk.com/method/wall.get");
  url.searchParams.set("domain", domain);
  url.searchParams.set("count", String(count));
  url.searchParams.set("offset", String(offset));
  url.searchParams.set("filter", filter);
  url.searchParams.set("extended", "0");
  url.searchParams.set("access_token", VK_ACCESS_TOKEN);
  url.searchParams.set("v", VK_API_VERSION);
  return url.toString();
}

function pickVkPhotoUrl(sizes) {
  if (!Array.isArray(sizes) || sizes.length === 0) {
    return "";
  }

  const sorted = [...sizes].sort((left, right) => {
    const leftArea = Number(left.width || 0) * Number(left.height || 0);
    const rightArea = Number(right.width || 0) * Number(right.height || 0);
    return rightArea - leftArea;
  });

  const best = sorted[0] || {};
  return String(best.url || best.src || "").trim();
}

function normalizeVkAttachment(attachment) {
  if (!attachment || typeof attachment !== "object") {
    return null;
  }

  switch (attachment.type) {
    case "photo": {
      const photo = attachment.photo || {};
      return {
        type: "photo",
        imageUrl: pickVkPhotoUrl(photo.sizes),
        text: String(photo.text || "").trim(),
        width: Number(photo.width || 0) || null,
        height: Number(photo.height || 0) || null
      };
    }

    case "link": {
      const link = attachment.link || {};
      return {
        type: "link",
        url: String(link.url || "").trim(),
        title: String(link.title || "").trim(),
        caption: String(link.caption || "").trim(),
        description: String(link.description || "").trim(),
        imageUrl: pickVkPhotoUrl(link.photo?.sizes)
      };
    }

    case "video": {
      const video = attachment.video || {};
      return {
        type: "video",
        ownerId: Number(video.owner_id || 0) || null,
        id: Number(video.id || 0) || null,
        title: String(video.title || "").trim(),
        description: String(video.description || "").trim(),
        imageUrl: pickVkPhotoUrl(video.image)
      };
    }

    case "doc": {
      const doc = attachment.doc || {};
      return {
        type: "doc",
        url: String(doc.url || "").trim(),
        title: String(doc.title || "").trim(),
        ext: String(doc.ext || "").trim()
      };
    }

    case "audio": {
      const audio = attachment.audio || {};
      return {
        type: "audio",
        artist: String(audio.artist || "").trim(),
        title: String(audio.title || "").trim()
      };
    }

    default:
      return {
        type: String(attachment.type || "unknown").trim()
      };
  }
}

function extractVkPrimaryImageUrl(attachments) {
  for (const attachment of attachments) {
    if (attachment.imageUrl) {
      return attachment.imageUrl;
    }
  }

  return "";
}

function normalizeVkPost(item) {
  const attachments = Array.isArray(item.attachments)
    ? item.attachments.map(normalizeVkAttachment).filter(Boolean)
    : [];

  const ownerId = Number(item.owner_id || 0) || 0;
  const id = Number(item.id || 0) || 0;
  const unixTime = Number(item.date || 0) || 0;

  return {
    id,
    ownerId,
    fromId: Number(item.from_id || 0) || null,
    postType: String(item.post_type || "").trim(),
    text: String(item.text || "").trim(),
    dateUnix: unixTime,
    dateIso: unixTime > 0 ? new Date(unixTime * 1000).toISOString() : null,
    url: ownerId && id ? `https://vk.com/wall${ownerId}_${id}` : "",
    likes: Number(item.likes?.count || 0) || 0,
    comments: Number(item.comments?.count || 0) || 0,
    reposts: Number(item.reposts?.count || 0) || 0,
    views: Number(item.views?.count || 0) || 0,
    isPinned: Boolean(item.is_pinned),
    previewImageUrl: extractVkPrimaryImageUrl(attachments),
    attachments
  };
}

async function getVkWallPayload({ domain, count, offset, filter }) {
  const cacheKey = `vk-wall:${domain}:${count}:${offset}:${filter}`;
  const cached = getCachedPayload(cacheKey, VK_WALL_CACHE_TTL_MS);

  if (cached) {
    return {
      ...cached,
      cached: true
    };
  }

  const payload = await getJson(buildVkWallUrl({ domain, count, offset, filter }), "VK API");
  if (payload?.error) {
    throw new Error(String(payload.error.error_msg || "VK API returned an error."));
  }

  const response = payload?.response || {};
  const items = Array.isArray(response.items) ? response.items.map(normalizeVkPost) : [];
  const responsePayload = {
    domain,
    count,
    offset,
    filter,
    total: Number(response.count || items.length) || items.length,
    items,
    fetchedAt: new Date().toISOString(),
    cached: false
  };

  setCachedPayload(cacheKey, responsePayload);
  return responsePayload;
}

function readAseString(buffer, offset) {
  if (offset >= buffer.length) {
    throw new Error("ASE packet ended unexpectedly.");
  }

  const size = buffer[offset];
  if (size < 1) {
    throw new Error("ASE packet contains an invalid string length.");
  }

  const nextOffset = offset + size;
  if (nextOffset > buffer.length) {
    throw new Error("ASE packet is truncated.");
  }

  return {
    value: buffer.toString("utf8", offset + 1, nextOffset),
    nextOffset
  };
}

function parseMtaAsePacket(buffer, latencyMs, target = DEFAULT_MTA_TARGET) {
  const requestedTarget = buildMtaTarget(target);

  if (!Buffer.isBuffer(buffer) || buffer.length < 4) {
    throw new Error("MTA server returned an invalid ASE packet.");
  }

  const header = buffer.toString("ascii", 0, 4);
  if (header !== "EYE1") {
    throw new Error(`Unexpected ASE header: ${header || "EMPTY"}`);
  }

  let offset = 4;

  const gameNameInfo = readAseString(buffer, offset);
  offset = gameNameInfo.nextOffset;

  const portInfo = readAseString(buffer, offset);
  offset = portInfo.nextOffset;

  const serverNameInfo = readAseString(buffer, offset);
  offset = serverNameInfo.nextOffset;

  const gameTypeInfo = readAseString(buffer, offset);
  offset = gameTypeInfo.nextOffset;

  const mapNameInfo = readAseString(buffer, offset);
  offset = mapNameInfo.nextOffset;

  const versionInfo = readAseString(buffer, offset);
  offset = versionInfo.nextOffset;

  const passwordedInfo = readAseString(buffer, offset);
  offset = passwordedInfo.nextOffset;

  const playersInfo = readAseString(buffer, offset);
  offset = playersInfo.nextOffset;

  const maxPlayersInfo = readAseString(buffer, offset);
  offset = maxPlayersInfo.nextOffset;

  const serverPort = clampInteger(portInfo.value, requestedTarget.port, 1, 65535);

  const rules = {};
  while (offset < buffer.length) {
    const keyInfo = readAseString(buffer, offset);
    offset = keyInfo.nextOffset;

    if (!keyInfo.value) {
      break;
    }

    const valueInfo = readAseString(buffer, offset);
    offset = valueInfo.nextOffset;
    rules[keyInfo.value] = valueInfo.value;
  }

  const playersOnline = clampInteger(playersInfo.value, 0, 0, 2000);
  const maxPlayers = clampInteger(maxPlayersInfo.value, 0, 0, 2000);
  const players = [];

  for (let index = 0; index < playersOnline && offset < buffer.length; index += 1) {
    const flags = buffer[offset];
    offset += 1;

    const player = {};

    if (flags & 1) {
      const playerNameInfo = readAseString(buffer, offset);
      offset = playerNameInfo.nextOffset;
      player.name = playerNameInfo.value;
    }

    if (flags & 2) {
      const teamInfo = readAseString(buffer, offset);
      offset = teamInfo.nextOffset;
      player.team = teamInfo.value;
    }

    if (flags & 4) {
      const skinInfo = readAseString(buffer, offset);
      offset = skinInfo.nextOffset;
      player.skin = skinInfo.value;
    }

    if (flags & 8) {
      const scoreInfo = readAseString(buffer, offset);
      offset = scoreInfo.nextOffset;
      player.score = scoreInfo.value;
    }

    if (flags & 16) {
      const pingInfo = readAseString(buffer, offset);
      offset = pingInfo.nextOffset;
      player.ping = pingInfo.value;
    }

    if (flags & 32) {
      const timeInfo = readAseString(buffer, offset);
      offset = timeInfo.nextOffset;
      player.time = timeInfo.value;
    }

    players.push(player);
  }

  return {
    protocol: "ase",
    label: requestedTarget.label,
    host: requestedTarget.host,
    port: serverPort,
    queryPort: requestedTarget.queryPort,
    serverAddress: `${requestedTarget.host}:${serverPort}`,
    gameName: gameNameInfo.value,
    serverName: serverNameInfo.value,
    gameType: gameTypeInfo.value,
    mapName: mapNameInfo.value,
    version: versionInfo.value,
    passworded: passwordedInfo.value === "1",
    playersOnline,
    maxPlayers,
    players,
    rules,
    latencyMs
  };
}

function queryMtaStatus(target = DEFAULT_MTA_TARGET) {
  const requestedTarget = buildMtaTarget(target);

  return new Promise((resolve, reject) => {
    const socket = dgram.createSocket("udp4");
    const startedAt = Date.now();
    const queryPacket = Buffer.from("s", "ascii");
    let settled = false;

    const cleanup = () => {
      clearTimeout(timeoutHandle);
      socket.removeAllListeners();
      try {
        socket.close();
      } catch {
        // Ignore close errors after resolution.
      }
    };

    const settle = (callback, payload) => {
      if (settled) {
        return;
      }

      settled = true;
      cleanup();
      callback(payload);
    };

    const timeoutHandle = setTimeout(() => {
      settle(reject, new Error(`MTA query timeout after ${MTA_QUERY_TIMEOUT_MS}ms.`));
    }, MTA_QUERY_TIMEOUT_MS);

    socket.once("error", (error) => {
      settle(reject, error);
    });

    socket.once("message", (message) => {
      try {
        const status = parseMtaAsePacket(message, Date.now() - startedAt, requestedTarget);
        settle(resolve, {
          ...status,
          fetchedAt: new Date().toISOString(),
          online: true
        });
      } catch (error) {
        settle(reject, error);
      }
    });

    socket.send(queryPacket, requestedTarget.queryPort, requestedTarget.host, (error) => {
      if (error) {
        settle(reject, error);
      }
    });
  });
}

async function getMtaStatusPayload(target = DEFAULT_MTA_TARGET) {
  const requestedTarget = buildMtaTarget(target);
  const cacheKey = `mta:${requestedTarget.host}:${requestedTarget.port}:${requestedTarget.queryPort}`;
  const cached = getCachedPayload(cacheKey, MTA_STATUS_CACHE_TTL_MS);

  if (cached) {
    return {
      ...cached,
      cached: true
    };
  }

  const payload = await queryMtaStatus(requestedTarget);
  setCachedPayload(cacheKey, payload);
  return {
    ...payload,
    cached: false
  };
}

function parseMtaMasterlistServerV0(reader) {
  const declaredCount = reader.read(4);
  const items = [];

  while (reader.step(6)) {
    const octets = [];
    for (let index = 0; index < 4; index += 1) {
      octets.push(String(reader.read(1)));
    }

    items.push({
      ip: octets.reverse().join("."),
      port: reader.read(2),
      playersCount: 0,
      maxPlayersCount: 0,
      gameName: "",
      serverName: "",
      modeName: "",
      mapName: "",
      verName: "",
      passworded: 0,
      players: [],
      httpPort: 0,
      serials: 0
    });
  }

  return {
    version: 0,
    flags: 0,
    sequenceNumber: 0,
    declaredCount,
    items
  };
}

function parseMtaMasterlistServerV2(reader) {
  const flags = reader.read(4);
  const sequenceNumber = reader.read(4);
  const declaredCount = reader.read(4);
  const items = [];

  while (reader.step(6)) {
    const startPosition = reader.tell();
    const recordLength = reader.read(2);

    if (!recordLength) {
      break;
    }

    const octets = [];
    for (let index = 0; index < 4; index += 1) {
      octets.push(String(reader.read(1)));
    }

    const server = {
      ip: octets.reverse().join("."),
      port: reader.read(2),
      playersCount: 0,
      maxPlayersCount: 0,
      gameName: "",
      serverName: "",
      modeName: "",
      mapName: "",
      verName: "",
      passworded: 0,
      players: [],
      httpPort: 0,
      serials: 0
    };

    if ((flags & MTA_MASTERLIST_FLAGS.ASE_PLAYER_COUNT) !== 0) {
      server.playersCount = reader.read(2);
    }

    if ((flags & MTA_MASTERLIST_FLAGS.ASE_MAX_PLAYER_COUNT) !== 0) {
      server.maxPlayersCount = reader.read(2);
    }

    if ((flags & MTA_MASTERLIST_FLAGS.ASE_GAME_NAME) !== 0) {
      server.gameName = reader.readString();
    }

    if ((flags & MTA_MASTERLIST_FLAGS.ASE_SERVER_NAME) !== 0) {
      server.serverName = reader.readString();
    }

    if ((flags & MTA_MASTERLIST_FLAGS.ASE_GAME_MODE) !== 0) {
      server.modeName = reader.readString();
    }

    if ((flags & MTA_MASTERLIST_FLAGS.ASE_MAP_NAME) !== 0) {
      server.mapName = reader.readString();
    }

    if ((flags & MTA_MASTERLIST_FLAGS.ASE_SERVER_VER) !== 0) {
      server.verName = reader.readString();
    }

    if ((flags & MTA_MASTERLIST_FLAGS.ASE_PASSWORDED) !== 0) {
      server.passworded = reader.read(1);
    }

    if ((flags & MTA_MASTERLIST_FLAGS.ASE_SERIALS) !== 0) {
      server.serials = reader.read(1);
    }

    if ((flags & MTA_MASTERLIST_FLAGS.ASE_PLAYER_LIST) !== 0) {
      const playerListSize = reader.read(2);

      for (let index = 0; index < playerListSize; index += 1) {
        server.players.push(reader.readString());
      }
    }

    if ((flags & MTA_MASTERLIST_FLAGS.ASE_RESPONDING) !== 0) {
      reader.read(1);
    }

    if ((flags & MTA_MASTERLIST_FLAGS.ASE_RESTRICTION) !== 0) {
      reader.read(4);
    }

    if ((flags & MTA_MASTERLIST_FLAGS.ASE_SEARCH_IGNORE_SECTIONS) !== 0) {
      const ignoreSectionCount = reader.read(1);
      reader.seek(reader.tell() + (2 * ignoreSectionCount));
    }

    if ((flags & MTA_MASTERLIST_FLAGS.ASE_KEEP_FLAG) !== 0) {
      reader.read(1);
    }

    if ((flags & MTA_MASTERLIST_FLAGS.ASE_HTTP_PORT) !== 0) {
      server.httpPort = reader.read(2);
    }

    if ((flags & MTA_MASTERLIST_FLAGS.ASE_SPECIAL) !== 0) {
      reader.read(1);
    }

    reader.seek(startPosition + recordLength);
    items.push(server);
  }

  return {
    version: 2,
    flags,
    sequenceNumber,
    declaredCount,
    items
  };
}

function normalizeMtaMasterlistItem(server) {
  const ip = sanitizeMasterlistText(server.ip);
  const port = clampInteger(server.port, 0, 0, 65535);
  const httpPort = clampInteger(server.httpPort, 0, 0, 65535);
  const playersOnline = clampInteger(server.playersCount, 0, 0, 2000);
  const maxPlayers = clampInteger(server.maxPlayersCount, 0, 0, 2000);
  const serverName = sanitizeMasterlistText(server.serverName || server.gameName || "MTA Server");
  const gameName = sanitizeMasterlistText(server.gameName);
  const gameType = sanitizeMasterlistText(server.modeName);
  const mapName = sanitizeMasterlistText(server.mapName);
  const version = sanitizeMasterlistText(server.verName);

  return {
    ip,
    port,
    address: ip && port ? `${ip}:${port}` : "",
    playersOnline,
    maxPlayers,
    utilizationPercent: maxPlayers > 0
      ? Number(((playersOnline / maxPlayers) * 100).toFixed(1))
      : null,
    gameName,
    serverName,
    gameType,
    mapName,
    version,
    passworded: Boolean(server.passworded),
    serials: clampInteger(server.serials, 0, 0, 255),
    players: Array.isArray(server.players)
      ? server.players.map((value) => sanitizeMasterlistText(value)).filter(Boolean)
      : [],
    httpPort: httpPort || null,
    httpAddress: ip && httpPort ? `${ip}:${httpPort}` : ""
  };
}

function parseMtaMasterlistBuffer(rawBuffer) {
  const reader = new MtaMasterlistReader(rawBuffer);
  const headerValue = reader.read(2);
  let version = 0;

  if (headerValue === 0) {
    version = reader.read(2);
  }

  const parsed = version === 2
    ? parseMtaMasterlistServerV2(reader)
    : parseMtaMasterlistServerV0(reader);

  const items = parsed.items.map(normalizeMtaMasterlistItem);
  return {
    version: parsed.version,
    flags: parsed.flags,
    sequenceNumber: parsed.sequenceNumber,
    declaredCount: parsed.declaredCount,
    totalServers: items.length,
    onlineCount: items.filter((item) => item.playersOnline > 0).length,
    totalPlayers: items.reduce((sum, item) => sum + item.playersOnline, 0),
    items
  };
}

async function getMtaMasterlistPayload() {
  const cacheKey = "mta-masterlist:all";
  const cached = getCachedPayload(cacheKey, MTA_MASTERLIST_CACHE_TTL_MS);

  if (cached) {
    return {
      ...cached,
      cached: true
    };
  }

  const rawBuffer = await getBinary(MTA_MASTERLIST_URL, "MTA masterlist", MTA_MASTERLIST_TIMEOUT_MS);
  const parsed = parseMtaMasterlistBuffer(rawBuffer);
  const payload = {
    ...parsed,
    sourceUrl: MTA_MASTERLIST_URL,
    fetchedAt: new Date().toISOString(),
    cached: false
  };

  setCachedPayload(cacheKey, payload);
  return payload;
}

function getRequestedMtaMasterlistOptions(query = {}) {
  const includeEmpty = isTruthyQueryValue(query.includeEmpty);
  const fallbackMinPlayers = includeEmpty ? 0 : 1;

  return {
    includeEmpty,
    minPlayers: clampInteger(query.minPlayers, fallbackMinPlayers, 0, 2000),
    limit: normalizeMasterlistLimit(query.limit, DEFAULT_MTA_MASTERLIST_LIMIT),
    search: sanitizeMasterlistSearch(query.search),
    sort: sanitizeMasterlistSort(query.sort)
  };
}

function sortMtaMasterlistItems(items, sort) {
  const sorted = [...items];

  switch (sort) {
    case "players-asc":
      sorted.sort((left, right) =>
        (left.playersOnline - right.playersOnline) ||
        (left.maxPlayers - right.maxPlayers) ||
        MASTERLIST_TEXT_COLLATOR.compare(left.serverName, right.serverName)
      );
      break;

    case "name-asc":
      sorted.sort((left, right) =>
        MASTERLIST_TEXT_COLLATOR.compare(left.serverName, right.serverName) ||
        (right.playersOnline - left.playersOnline)
      );
      break;

    case "name-desc":
      sorted.sort((left, right) =>
        MASTERLIST_TEXT_COLLATOR.compare(right.serverName, left.serverName) ||
        (right.playersOnline - left.playersOnline)
      );
      break;

    case "address-asc":
      sorted.sort((left, right) =>
        MASTERLIST_TEXT_COLLATOR.compare(left.address, right.address) ||
        (right.playersOnline - left.playersOnline)
      );
      break;

    case "players-desc":
    default:
      sorted.sort((left, right) =>
        (right.playersOnline - left.playersOnline) ||
        (right.maxPlayers - left.maxPlayers) ||
        MASTERLIST_TEXT_COLLATOR.compare(left.serverName, right.serverName)
      );
      break;
  }

  return sorted;
}

function filterMtaMasterlistItems(items, options) {
  let nextItems = [...items];

  nextItems = nextItems.filter((item) => item.playersOnline >= options.minPlayers);

  if (options.search) {
    nextItems = nextItems.filter((item) => {
      const haystack = [
        item.serverName,
        item.gameName,
        item.gameType,
        item.mapName,
        item.version,
        item.address,
        item.httpAddress
      ].join(" ").toLowerCase();

      return haystack.includes(options.search);
    });
  }

  nextItems = sortMtaMasterlistItems(nextItems, options.sort);
  return options.limit > 0 ? nextItems.slice(0, options.limit) : nextItems;
}

function buildMtaMasterlistResponse(masterlistPayload, options) {
  const allItems = Array.isArray(masterlistPayload.items) ? masterlistPayload.items : [];
  const filteredItems = filterMtaMasterlistItems(allItems, {
    ...options,
    limit: 0
  });
  const returnedItems = options.limit > 0 ? filteredItems.slice(0, options.limit) : filteredItems;

  return {
    version: masterlistPayload.version,
    flags: masterlistPayload.flags,
    sequenceNumber: masterlistPayload.sequenceNumber,
    declaredCount: masterlistPayload.declaredCount,
    totalServers: masterlistPayload.totalServers,
    onlineCount: masterlistPayload.onlineCount,
    totalPlayers: masterlistPayload.totalPlayers,
    filteredCount: filteredItems.length,
    returnedCount: returnedItems.length,
    returnedPlayers: returnedItems.reduce((sum, item) => sum + item.playersOnline, 0),
    fetchedAt: masterlistPayload.fetchedAt,
    cached: Boolean(masterlistPayload.cached),
    sourceUrl: masterlistPayload.sourceUrl,
    filters: {
      includeEmpty: options.includeEmpty,
      minPlayers: options.minPlayers,
      limit: options.limit,
      search: options.search,
      sort: options.sort
    },
    items: returnedItems
  };
}

app.get("/", (req, res) => {
  res.json({
    status: "ok",
    service: "WHALE V8 BLACKROCK MODE",
    endpoints: {
      dashboard: "/dashboard",
      gemScanner: "/gem-scanner",
      gemScannerApi: "/api/gem-scanner/markets",
      health: "/health",
      wallet: "/blackrock/:address?limit=80&whaleThreshold=40",
      mtaStatus: "/api/mta-status",
      serverStatus: "/api/server-status",
      mtaStatusList: "/api/mta-status-list?server=Miami%20RP@46.174.50.52:22101",
      serverStatusList: "/api/server-status-list?server=Miami%20RP@46.174.50.52:22101",
      mtaPublicServers: "/api/mta-public-servers?includeEmpty=1&limit=5000&sort=players-desc",
      mtaMonitor: "/mta-monitor",
      mtaMonitorEmbed: "/mta-monitor-embed",
      mtaServerList: "/mta-serverlist",
      vkWall: `/api/vk-wall?domain=${DEFAULT_VK_DOMAIN || "mta.miami"}&count=${DEFAULT_VK_POST_COUNT}`,
      supportChatWidget: "/support-chat-widget.js",
      supportChatAdmin: "/support-chat-admin",
      supportChatVisitorApi: "/api/support-chat/visitor/messages",
      supportChatAdminApi: "/api/support-chat/admin/conversations"
    }
  });
});

app.get("/dashboard", (req, res) => {
  res.sendFile(path.join(__dirname, "whale-v8-blackrock.html"));
});

app.get("/gem-scanner", (req, res) => {
  res.sendFile(path.join(__dirname, "crypto-gem-scanner-seo-guide.html"));
});

app.get("/health", (req, res) => {
  res.json({
    status: "ok",
    etherscanConfigured: Boolean(ETHERSCAN_API_KEY),
    cacheTtlMs: CACHE_TTL_MS,
    requestTimeoutMs: REQUEST_TIMEOUT_MS,
    mtaMonitor: {
      host: DEFAULT_MTA_HOST,
      port: DEFAULT_MTA_PORT,
      queryPort: DEFAULT_MTA_QUERY_PORT,
      timeoutMs: MTA_QUERY_TIMEOUT_MS,
      cacheTtlMs: MTA_STATUS_CACHE_TTL_MS
    },
    mtaMasterlist: {
      url: MTA_MASTERLIST_URL,
      timeoutMs: MTA_MASTERLIST_TIMEOUT_MS,
      cacheTtlMs: MTA_MASTERLIST_CACHE_TTL_MS,
      defaultLimit: DEFAULT_MTA_MASTERLIST_LIMIT
    },
    vkWall: {
      configured: Boolean(VK_ACCESS_TOKEN),
      domain: DEFAULT_VK_DOMAIN || "mta.miami",
      apiVersion: VK_API_VERSION,
      defaultCount: DEFAULT_VK_POST_COUNT,
      cacheTtlMs: VK_WALL_CACHE_TTL_MS
    },
    gemScanner: {
      marketsUrl: COINGECKO_MARKETS_URL,
      cacheTtlMs: COINGECKO_MARKETS_CACHE_TTL_MS
    },
    supportChat: {
      adminConfigured: supportChatConfig.adminConfigured,
      operatorName: supportChatConfig.operatorName,
      maxConversations: SUPPORT_CHAT_MAX_CONVERSATIONS,
      maxMessagesPerConversation: SUPPORT_CHAT_MAX_MESSAGES
    }
  });
});

app.get("/mta-monitor", (req, res) => {
  res.sendFile(path.join(__dirname, "mta-server-monitor.html"));
});

app.get("/mta-monitor-embed", (req, res) => {
  res.sendFile(path.join(__dirname, "mta-server-monitor-embed.html"));
});

app.get("/mta-serverlist", (req, res) => {
  res.sendFile(path.join(__dirname, "mta-public-serverlist.html"));
});

app.get("/mta-public-serverlist", (req, res) => {
  res.sendFile(path.join(__dirname, "mta-public-serverlist.html"));
});

app.get("/api/gem-scanner/markets", async (req, res) => {
  const cacheKey = "gem-scanner:markets";
  const cached = getCachedPayload(cacheKey, COINGECKO_MARKETS_CACHE_TTL_MS);

  if (cached) {
    res.json({
      ...cached,
      cached: true
    });
    return;
  }

  try {
    const upstream = await getJsonWithMeta(COINGECKO_MARKETS_URL, "CoinGecko markets");
    const retryAfterMs = parseRetryAfterMs(upstream.headers?.["retry-after"]);
    const rows = Array.isArray(upstream.data) ? upstream.data : null;

    if (upstream.statusCode === 429) {
      res.status(429).json({
        error: "COINGECKO_RATE_LIMITED",
        message: "CoinGecko rate limit reached.",
        retryAfterMs
      });
      return;
    }

    if (upstream.statusCode >= 400 || !rows) {
      throw new Error(
        upstream.statusCode >= 400
          ? `CoinGecko markets returned HTTP ${upstream.statusCode}.`
          : "CoinGecko markets returned an unexpected payload."
      );
    }

    const payload = {
      source: "proxy",
      cached: false,
      fetchedAt: new Date().toISOString(),
      data: rows
    };

    setCachedPayload(cacheKey, payload);
    res.json(payload);
  } catch (error) {
    const staleEntry = getRawCachedEntry(cacheKey);

    if (staleEntry?.payload?.data && Array.isArray(staleEntry.payload.data)) {
      res.status(200).json({
        ...staleEntry.payload,
        cached: true,
        stale: true,
        message: error?.message || "CoinGecko markets request failed."
      });
      return;
    }

    res.status(502).json({
      error: "COINGECKO_MARKETS_FETCH_FAILED",
      message: error?.message || "Unable to fetch CoinGecko market data.",
      data: []
    });
  }
});

app.get("/api/mta-status", async (req, res) => {
  let target = DEFAULT_MTA_TARGET;

  try {
    target = getRequestedMtaTarget(req.query);
  } catch (error) {
    res.status(400).json({
      ...buildMtaOfflinePayload(DEFAULT_MTA_TARGET, error),
      error: "INVALID_MTA_TARGET"
    });
    return;
  }

  try {
    const payload = await getMtaStatusPayload(target);
    res.json(payload);
  } catch (error) {
    res.status(502).json(buildMtaOfflinePayload(target, error));
  }
});

app.get("/api/server-status", async (req, res) => {
  let target = DEFAULT_MTA_TARGET;

  try {
    target = getRequestedMtaTarget(req.query);
  } catch (error) {
    res.status(400).json({
      ...buildMtaOfflinePayload(DEFAULT_MTA_TARGET, error),
      error: "INVALID_MTA_TARGET"
    });
    return;
  }

  try {
    const payload = await getMtaStatusPayload(target);
    res.json(payload);
  } catch (error) {
    res.status(502).json(buildMtaOfflinePayload(target, error));
  }
});

app.get("/api/mta-status-list", async (req, res) => {
  let targets = [];

  try {
    targets = getRequestedMtaTargets(req.query.server);
  } catch (error) {
    res.status(400).json({
      error: "INVALID_MTA_SERVER_LIST",
      message: error.message || "Provide valid MTA servers in the query string.",
      items: []
    });
    return;
  }

  const requestedTargets = targets.length > 0 ? targets : [DEFAULT_MTA_TARGET];
  const items = await Promise.all(
    requestedTargets.map(async (target) => {
      try {
        return await getMtaStatusPayload(target);
      } catch (error) {
        return buildMtaOfflinePayload(target, error);
      }
    })
  );

  const onlineCount = items.filter((item) => item.online !== false).length;

  res.json({
    total: items.length,
    onlineCount,
    offlineCount: items.length - onlineCount,
    fetchedAt: new Date().toISOString(),
    items
  });
});

app.get("/api/server-status-list", async (req, res) => {
  let targets = [];

  try {
    targets = getRequestedMtaTargets(req.query.server);
  } catch (error) {
    res.status(400).json({
      error: "INVALID_MTA_SERVER_LIST",
      message: error.message || "Provide valid MTA servers in the query string.",
      items: []
    });
    return;
  }

  const requestedTargets = targets.length > 0 ? targets : [DEFAULT_MTA_TARGET];
  const items = await Promise.all(
    requestedTargets.map(async (target) => {
      try {
        return await getMtaStatusPayload(target);
      } catch (error) {
        return buildMtaOfflinePayload(target, error);
      }
    })
  );

  const onlineCount = items.filter((item) => item.online !== false).length;

  res.json({
    total: items.length,
    onlineCount,
    offlineCount: items.length - onlineCount,
    fetchedAt: new Date().toISOString(),
    items
  });
});

app.get("/api/mta-public-servers", async (req, res) => {
  const options = getRequestedMtaMasterlistOptions(req.query);

  try {
    const masterlistPayload = await getMtaMasterlistPayload();
    res.json(buildMtaMasterlistResponse(masterlistPayload, options));
  } catch (error) {
    res.status(502).json({
      error: "MTA_MASTERLIST_FETCH_FAILED",
      message: error.message || "Unable to fetch the MTA master server list.",
      fetchedAt: new Date().toISOString(),
      filters: options,
      items: []
    });
  }
});

app.get("/api/vk-wall", async (req, res) => {
  const domain = sanitizeVkDomain(req.query.domain || DEFAULT_VK_DOMAIN || "mta.miami");
  const count = clampInteger(req.query.count, DEFAULT_VK_POST_COUNT, 1, 20);
  const offset = clampInteger(req.query.offset, 0, 0, 500);
  const requestedFilter = String(req.query.filter || "owner").trim().toLowerCase();
  const filter = new Set(["owner", "all", "others"]).has(requestedFilter) ? requestedFilter : "owner";

  if (!domain) {
    res.status(400).json({
      error: "INVALID_VK_DOMAIN",
      message: "Provide a VK domain like mta.miami or a valid vk.com community URL."
    });
    return;
  }

  if (!VK_ACCESS_TOKEN) {
    res.status(503).json({
      error: "MISSING_VK_ACCESS_TOKEN",
      message: "Set VK_ACCESS_TOKEN in your environment before using this endpoint."
    });
    return;
  }

  try {
    const payload = await getVkWallPayload({ domain, count, offset, filter });
    res.json(payload);
  } catch (error) {
    res.status(502).json({
      error: "VK_WALL_FETCH_FAILED",
      message: error.message || "Unable to fetch VK wall posts.",
      domain,
      count,
      offset,
      filter
    });
  }
});

app.get("/api/radio-now-playing", async (req, res) => {
  const station = getRadioStationDefinition(req.query.station);

  if (!station) {
    res.status(400).json({
      error: "INVALID_RADIO_STATION",
      message: "Provide a supported radio station key.",
      stations: [...RADIO_STATIONS.keys()]
    });
    return;
  }

  const payload = await getRadioNowPlayingPayload(station);
  res.json(payload);
});

app.get("/blackrock/:address", async (req, res) => {
  const address = normalizeAddress(req.params.address);
  const limit = clampInteger(req.query.limit, DEFAULT_LIMIT, 10, 150);
  const whaleThreshold = clampNumber(req.query.whaleThreshold, DEFAULT_WHALE_THRESHOLD, 1, 5000);

  if (!isValidAddress(address)) {
    res.status(400).json({
      error: "INVALID_ADDRESS",
      message: "A valid Ethereum address is required."
    });
    return;
  }

  if (!ETHERSCAN_API_KEY) {
    res.status(503).json({
      error: "MISSING_ETHERSCAN_KEY",
      message: "Set ETHERSCAN_KEY in your environment before using this endpoint."
    });
    return;
  }

  const cacheKey = `${address}:${limit}:${whaleThreshold}`;
  const cached = getCachedPayload(cacheKey);
  if (cached) {
    res.json({
      ...cached,
      cached: true
    });
    return;
  }

  try {
    const payload = await getJson(buildEtherscanUrl(address), "Etherscan");
    const rawResult = Array.isArray(payload.result) ? payload.result : [];

    if (payload.status === "0" && payload.message === "NOTOK") {
      throw new Error(typeof payload.result === "string" ? payload.result : "Etherscan returned an error");
    }

    const txs = rawResult.slice(0, limit);

    let inflow = 0;
    let outflow = 0;
    let whaleCount = 0;
    let whaleVolume = 0;
    let exchangeFlow = 0;
    let smartMoneyCount = 0;

    const enriched = txs.map((tx) => {
      const from = normalizeAddress(tx.from);
      const to = normalizeAddress(tx.to);
      const valueETH = Number(tx.value) / 1e18;
      const safeValue = Number.isFinite(valueETH) ? valueETH : 0;
      const isInbound = to === address;
      const counterparty = isInbound ? from : to;
      const isExchange = EXCHANGES.has(from) || EXCHANGES.has(to);
      const cluster = clusterWallet(counterparty);
      const isWhale = safeValue >= whaleThreshold;

      if (isInbound) {
        inflow += safeValue;
      } else {
        outflow += safeValue;
      }

      if (isExchange) {
        exchangeFlow += safeValue;
      }

      if (isWhale) {
        whaleCount += 1;
        whaleVolume += safeValue;
      }

      if (cluster === "SMART MONEY") {
        smartMoneyCount += 1;
      }

      return {
        hash: tx.hash || "",
        from: tx.from || "",
        to: tx.to || "",
        valueETH: Number(safeValue.toFixed(4)),
        type: isInbound ? "IN" : "OUT",
        exchange: isExchange,
        cluster,
        whale: isWhale,
        blockNumber: tx.blockNumber || "",
        timeStamp: Number(tx.timeStamp) || 0,
        time: formatTimestamp(tx.timeStamp)
      };
    });

    const netFlow = Number((inflow - outflow).toFixed(4));
    const blackrockScore = buildScore({
      netFlow,
      whaleCount,
      exchangeFlow,
      smartMoneyCount,
      whaleThreshold
    });
    const responsePayload = {
      address,
      limit,
      whaleThreshold,
      inflow: Number(inflow.toFixed(4)),
      outflow: Number(outflow.toFixed(4)),
      netFlow,
      whaleCount,
      whaleVolume: Number(whaleVolume.toFixed(4)),
      smartMoneyCount,
      exchangeFlow: Number(exchangeFlow.toFixed(4)),
      blackrockScore,
      signal: buildSignal(blackrockScore),
      txs: enriched,
      fetchedAt: new Date().toISOString(),
      cached: false
    };

    setCachedPayload(cacheKey, responsePayload);
    res.json(responsePayload);
  } catch (error) {
    res.status(502).json({
      error: "BLACKROCK_MODE_FAILED",
      message: error.message || "Unknown upstream error"
    });
  }
});

app.listen(PORT, HOST, () => {
  console.log(`WHALE V8 BLACKROCK MODE RUNNING http://${HOST}:${PORT}`);
});
