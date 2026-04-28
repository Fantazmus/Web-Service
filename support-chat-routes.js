import fs from "node:fs/promises";
import path from "node:path";
import { randomUUID } from "node:crypto";

const STORE_SCHEMA_VERSION = 1;

export function registerSupportChatRoutes({
  app,
  baseDir,
  adminToken = "",
  operatorName = "Support",
  maxConversations = 1500,
  maxMessagesPerConversation = 200
}) {
  const safeAdminToken = String(adminToken || "").trim();
  const safeOperatorName = sanitizeSupportChatName(operatorName) || "Support";
  const safeMaxConversations = clampInteger(maxConversations, 1500, 100, 20000);
  const safeMaxMessagesPerConversation = clampInteger(maxMessagesPerConversation, 200, 20, 2000);
  const storeDir = path.join(baseDir, ".data");
  const storePath = path.join(storeDir, "support-chat-store.json");

  const storeState = {
    data: null,
    loadPromise: null
  };

  let mutationQueue = Promise.resolve();

  function clampInteger(rawValue, fallback, min, max) {
    const parsed = Number.parseInt(rawValue, 10);
    const safeValue = Number.isFinite(parsed) ? parsed : fallback;
    return Math.min(max, Math.max(min, safeValue));
  }

  function createEmptyStore() {
    return {
      schemaVersion: STORE_SCHEMA_VERSION,
      conversations: []
    };
  }

  function sanitizeSupportChatName(value) {
    return String(value || "")
      .replace(/[\u0000-\u001f]+/g, " ")
      .replace(/\s+/g, " ")
      .trim()
      .slice(0, 60);
  }

  function sanitizeSupportChatEmail(value) {
    const normalized = String(value || "").trim().toLowerCase();
    if (!normalized) {
      return "";
    }

    return /^[^\s@]+@[^\s@]+\.[^\s@]+$/.test(normalized) ? normalized.slice(0, 120) : "";
  }

  function sanitizeSupportChatSessionKey(value) {
    const normalized = String(value || "").trim().toLowerCase();
    return /^[a-z0-9][a-z0-9-]{15,120}$/.test(normalized) ? normalized : "";
  }

  function sanitizeSupportChatText(value) {
    const normalized = String(value || "")
      .replace(/\r\n/g, "\n")
      .replace(/\r/g, "\n")
      .replace(/[\u0000-\u0008\u000b\u000c\u000e-\u001f]+/g, "")
      .split("\n")
      .map((line) => line.trimEnd())
      .join("\n")
      .replace(/\n{3,}/g, "\n\n")
      .trim();

    return normalized.slice(0, 1500);
  }

  function sanitizeSupportChatTitle(value) {
    return String(value || "")
      .replace(/[\u0000-\u001f]+/g, " ")
      .replace(/\s+/g, " ")
      .trim()
      .slice(0, 120);
  }

  function sanitizeSupportChatMeta(value, maxLength = 200) {
    return String(value || "")
      .replace(/[\u0000-\u001f]+/g, " ")
      .replace(/\s+/g, " ")
      .trim()
      .slice(0, maxLength);
  }

  function sanitizeSupportChatUrl(value) {
    const normalized = String(value || "").trim();
    if (!normalized) {
      return "";
    }

    try {
      const parsed = new URL(normalized);
      if (!new Set(["http:", "https:"]).has(parsed.protocol)) {
        return "";
      }

      return parsed.toString().slice(0, 500);
    } catch (_) {
      return "";
    }
  }

  function sanitizeRecordId(value, prefix) {
    const normalized = String(value || "").trim();
    if (normalized.startsWith(prefix) && normalized.length <= 120) {
      return normalized;
    }

    return `${prefix}${randomUUID()}`;
  }

  function sanitizeSupportChatConversationId(value) {
    const normalized = String(value || "").trim().toLowerCase();
    return /^conv_[a-z0-9-]{8,120}$/.test(normalized) ? normalized : "";
  }

  function normalizeIsoTimestamp(value, fallback = "") {
    const parsed = Date.parse(String(value || "").trim());
    if (Number.isFinite(parsed)) {
      return new Date(parsed).toISOString();
    }

    return fallback;
  }

  function normalizeMessage(rawMessage) {
    const role = rawMessage?.role === "admin" ? "admin" : rawMessage?.role === "visitor" ? "visitor" : "";
    const text = sanitizeSupportChatText(rawMessage?.text);

    if (!role || !text) {
      return null;
    }

    return {
      id: sanitizeRecordId(rawMessage?.id, "msg_"),
      role,
      text,
      createdAt: normalizeIsoTimestamp(rawMessage?.createdAt, new Date(0).toISOString())
    };
  }

  function refreshConversationMeta(conversation) {
    if (!conversation || !Array.isArray(conversation.messages) || conversation.messages.length === 0) {
      return null;
    }

    const firstMessage = conversation.messages[0];
    const lastMessage = conversation.messages[conversation.messages.length - 1];

    conversation.visitorName = sanitizeSupportChatName(conversation.visitorName);
    conversation.visitorEmail = sanitizeSupportChatEmail(conversation.visitorEmail);
    conversation.sourceTitle = sanitizeSupportChatTitle(conversation.sourceTitle);
    conversation.sourceUrl = sanitizeSupportChatUrl(conversation.sourceUrl);
    conversation.sourceUserAgent = sanitizeSupportChatMeta(conversation.sourceUserAgent, 200);
    conversation.createdAt = normalizeIsoTimestamp(conversation.createdAt, firstMessage.createdAt) || firstMessage.createdAt;
    conversation.updatedAt = lastMessage.createdAt;
    conversation.lastMessageAt = lastMessage.createdAt;
    conversation.lastMessageRole = lastMessage.role;

    return conversation;
  }

  function normalizeConversation(rawConversation) {
    const sessionKey = sanitizeSupportChatSessionKey(rawConversation?.sessionKey);
    if (!sessionKey) {
      return null;
    }

    const messages = Array.isArray(rawConversation?.messages)
      ? rawConversation.messages
        .map((message) => normalizeMessage(message))
        .filter(Boolean)
        .sort((left, right) => Date.parse(left.createdAt) - Date.parse(right.createdAt))
        .slice(-safeMaxMessagesPerConversation)
      : [];

    if (messages.length === 0) {
      return null;
    }

    const conversation = {
      id: sanitizeSupportChatConversationId(rawConversation?.id) || sanitizeRecordId(rawConversation?.id, "conv_"),
      sessionKey,
      visitorName: sanitizeSupportChatName(rawConversation?.visitorName),
      visitorEmail: sanitizeSupportChatEmail(rawConversation?.visitorEmail),
      sourceTitle: sanitizeSupportChatTitle(rawConversation?.sourceTitle),
      sourceUrl: sanitizeSupportChatUrl(rawConversation?.sourceUrl),
      sourceUserAgent: sanitizeSupportChatMeta(rawConversation?.sourceUserAgent, 200),
      createdAt: normalizeIsoTimestamp(rawConversation?.createdAt, messages[0].createdAt) || messages[0].createdAt,
      updatedAt: normalizeIsoTimestamp(rawConversation?.updatedAt, messages[messages.length - 1].createdAt)
        || messages[messages.length - 1].createdAt,
      lastMessageAt: normalizeIsoTimestamp(rawConversation?.lastMessageAt, messages[messages.length - 1].createdAt)
        || messages[messages.length - 1].createdAt,
      lastMessageRole: rawConversation?.lastMessageRole === "admin" ? "admin" : "visitor",
      messages
    };

    return refreshConversationMeta(conversation);
  }

  function normalizeStore(rawStore) {
    const conversations = Array.isArray(rawStore?.conversations)
      ? rawStore.conversations
        .map((conversation) => normalizeConversation(conversation))
        .filter(Boolean)
        .sort((left, right) => Date.parse(right.updatedAt) - Date.parse(left.updatedAt))
        .slice(0, safeMaxConversations)
      : [];

    return {
      schemaVersion: STORE_SCHEMA_VERSION,
      conversations
    };
  }

  async function persistStore() {
    storeState.data = normalizeStore(storeState.data);
    await fs.mkdir(storeDir, { recursive: true });

    const tempPath = `${storePath}.tmp`;
    await fs.writeFile(tempPath, JSON.stringify(storeState.data, null, 2), "utf8");
    await fs.rename(tempPath, storePath);
  }

  async function ensureStoreLoaded() {
    if (storeState.data) {
      return storeState.data;
    }

    if (!storeState.loadPromise) {
      storeState.loadPromise = (async () => {
        await fs.mkdir(storeDir, { recursive: true });

        try {
          const raw = await fs.readFile(storePath, "utf8");
          storeState.data = normalizeStore(JSON.parse(raw));
        } catch (error) {
          if (error?.code !== "ENOENT") {
            console.error("Support chat store could not be read. Creating a new one.", error);
          }

          storeState.data = createEmptyStore();
          await persistStore();
        }

        return storeState.data;
      })().finally(() => {
        storeState.loadPromise = null;
      });
    }

    return storeState.loadPromise;
  }

  async function readStore() {
    await mutationQueue;
    return ensureStoreLoaded();
  }

  function queueMutation(mutator) {
    const operation = mutationQueue.then(async () => {
      await ensureStoreLoaded();
      const result = await mutator(storeState.data);
      storeState.data = normalizeStore(storeState.data);
      await persistStore();
      return result;
    });

    mutationQueue = operation.catch(() => {});
    return operation;
  }

  function findConversationBySessionKey(store, sessionKey) {
    return store.conversations.find((conversation) => conversation.sessionKey === sessionKey) || null;
  }

  function findConversationById(store, conversationId) {
    return store.conversations.find((conversation) => conversation.id === conversationId) || null;
  }

  function sortConversations(store) {
    store.conversations.sort((left, right) => Date.parse(right.updatedAt) - Date.parse(left.updatedAt));
  }

  function buildAuthorName(role, conversation) {
    if (role === "admin") {
      return safeOperatorName;
    }

    return conversation?.visitorName || "Visitor";
  }

  function buildMessagePayload(message, conversation) {
    return {
      id: message.id,
      role: message.role,
      text: message.text,
      createdAt: message.createdAt,
      authorName: buildAuthorName(message.role, conversation)
    };
  }

  function buildVisitorPayload(conversation, sessionKey) {
    if (!conversation) {
      return {
        exists: false,
        sessionKey,
        operatorName: safeOperatorName,
        conversation: null,
        messages: []
      };
    }

    return {
      exists: true,
      sessionKey,
      operatorName: safeOperatorName,
      conversation: {
        visitorName: conversation.visitorName || "",
        visitorEmail: conversation.visitorEmail || "",
        sourceTitle: conversation.sourceTitle || "",
        sourceUrl: conversation.sourceUrl || "",
        createdAt: conversation.createdAt,
        updatedAt: conversation.updatedAt
      },
      messages: conversation.messages.map((message) => buildMessagePayload(message, conversation))
    };
  }

  function buildPreviewText(value) {
    return String(value || "")
      .replace(/\s+/g, " ")
      .trim()
      .slice(0, 120);
  }

  function buildAdminSummary(conversation) {
    const lastMessage = conversation.messages[conversation.messages.length - 1] || null;
    const visitorName = conversation.visitorName || "Visitor";

    return {
      id: conversation.id,
      visitorName,
      visitorEmail: conversation.visitorEmail || "",
      sourceTitle: conversation.sourceTitle || "",
      sourceUrl: conversation.sourceUrl || "",
      sourceUserAgent: conversation.sourceUserAgent || "",
      createdAt: conversation.createdAt,
      updatedAt: conversation.updatedAt,
      lastMessageAt: conversation.lastMessageAt,
      lastMessageRole: conversation.lastMessageRole,
      lastMessageText: lastMessage ? buildPreviewText(lastMessage.text) : "",
      messageCount: conversation.messages.length,
      needsReply: conversation.lastMessageRole === "visitor"
    };
  }

  function buildAdminConversationPayload(conversation) {
    return {
      operatorName: safeOperatorName,
      conversation: buildAdminSummary(conversation),
      messages: conversation.messages.map((message) => buildMessagePayload(message, conversation))
    };
  }

  function getAdminTokenFromRequest(req) {
    const headerToken = String(req.headers["x-admin-token"] || "").trim();
    if (headerToken) {
      return headerToken;
    }

    const authHeader = String(req.headers.authorization || "").trim();
    if (/^bearer\s+/i.test(authHeader)) {
      return authHeader.replace(/^bearer\s+/i, "").trim();
    }

    return "";
  }

  function authorizeAdmin(req, res) {
    if (!safeAdminToken) {
      res.status(503).json({
        error: "SUPPORT_CHAT_NOT_CONFIGURED",
        message: "Set SUPPORT_CHAT_ADMIN_TOKEN in your environment before using the admin chat panel."
      });
      return false;
    }

    if (getAdminTokenFromRequest(req) !== safeAdminToken) {
      res.status(401).json({
        error: "SUPPORT_CHAT_UNAUTHORIZED",
        message: "Provide a valid support chat admin token."
      });
      return false;
    }

    return true;
  }

  app.get("/support-chat-widget.js", (req, res) => {
    res.type("application/javascript");
    res.sendFile(path.join(baseDir, "support-chat-widget.js"));
  });

  app.get("/support-chat-admin", (req, res) => {
    res.set("Cache-Control", "no-store");
    res.sendFile(path.join(baseDir, "support-chat-admin.html"));
  });

  app.get("/support-chat-admin.html", (req, res) => {
    res.set("Cache-Control", "no-store");
    res.sendFile(path.join(baseDir, "support-chat-admin.html"));
  });

  app.get("/api/support-chat/visitor/session", async (req, res) => {
    const sessionKey = sanitizeSupportChatSessionKey(req.query.sessionKey);
    if (!sessionKey) {
      res.status(400).json({
        error: "INVALID_SUPPORT_CHAT_SESSION",
        message: "Provide a valid visitor session key."
      });
      return;
    }

    const store = await readStore();
    const conversation = findConversationBySessionKey(store, sessionKey);

    res.json(buildVisitorPayload(conversation, sessionKey));
  });

  app.post("/api/support-chat/visitor/messages", async (req, res) => {
    const sessionKey = sanitizeSupportChatSessionKey(req.body?.sessionKey);
    const text = sanitizeSupportChatText(req.body?.text);
    const visitorName = sanitizeSupportChatName(req.body?.visitorName);
    const visitorEmail = sanitizeSupportChatEmail(req.body?.visitorEmail);
    const sourceTitle = sanitizeSupportChatTitle(req.body?.sourceTitle);
    const sourceUrl = sanitizeSupportChatUrl(req.body?.sourceUrl);
    const sourceUserAgent = sanitizeSupportChatMeta(req.headers["user-agent"], 200);

    if (!sessionKey) {
      res.status(400).json({
        error: "INVALID_SUPPORT_CHAT_SESSION",
        message: "Provide a valid visitor session key."
      });
      return;
    }

    if (!text) {
      res.status(400).json({
        error: "EMPTY_SUPPORT_CHAT_MESSAGE",
        message: "Type a message before sending it."
      });
      return;
    }

    const payload = await queueMutation((store) => {
      let conversation = findConversationBySessionKey(store, sessionKey);
      const createdAt = new Date().toISOString();

      if (!conversation) {
        conversation = {
          id: `conv_${randomUUID()}`,
          sessionKey,
          visitorName: "",
          visitorEmail: "",
          sourceTitle: "",
          sourceUrl: "",
          sourceUserAgent: "",
          createdAt,
          updatedAt: createdAt,
          lastMessageAt: createdAt,
          lastMessageRole: "visitor",
          messages: []
        };

        store.conversations.unshift(conversation);
      }

      if (visitorName) {
        conversation.visitorName = visitorName;
      }

      if (visitorEmail) {
        conversation.visitorEmail = visitorEmail;
      }

      if (sourceTitle) {
        conversation.sourceTitle = sourceTitle;
      }

      if (sourceUrl) {
        conversation.sourceUrl = sourceUrl;
      }

      if (sourceUserAgent) {
        conversation.sourceUserAgent = sourceUserAgent;
      }

      conversation.messages.push({
        id: `msg_${randomUUID()}`,
        role: "visitor",
        text,
        createdAt
      });

      refreshConversationMeta(conversation);
      sortConversations(store);

      return buildVisitorPayload(conversation, sessionKey);
    });

    res.status(201).json(payload);
  });

  app.get("/api/support-chat/admin/conversations", async (req, res) => {
    if (!authorizeAdmin(req, res)) {
      return;
    }

    const store = await readStore();

    res.json({
      operatorName: safeOperatorName,
      items: store.conversations.map((conversation) => buildAdminSummary(conversation))
    });
  });

  app.get("/api/support-chat/admin/conversations/:conversationId", async (req, res) => {
    if (!authorizeAdmin(req, res)) {
      return;
    }

    const conversationId = sanitizeSupportChatConversationId(req.params.conversationId);
    if (!conversationId) {
      res.status(400).json({
        error: "INVALID_SUPPORT_CHAT_ID",
        message: "Provide a valid conversation id."
      });
      return;
    }

    const store = await readStore();
    const conversation = findConversationById(store, conversationId);

    if (!conversation) {
      res.status(404).json({
        error: "SUPPORT_CHAT_NOT_FOUND",
        message: "Conversation was not found."
      });
      return;
    }

    res.json(buildAdminConversationPayload(conversation));
  });

  app.post("/api/support-chat/admin/conversations/:conversationId/messages", async (req, res) => {
    if (!authorizeAdmin(req, res)) {
      return;
    }

    const conversationId = sanitizeSupportChatConversationId(req.params.conversationId);
    const text = sanitizeSupportChatText(req.body?.text);

    if (!conversationId) {
      res.status(400).json({
        error: "INVALID_SUPPORT_CHAT_ID",
        message: "Provide a valid conversation id."
      });
      return;
    }

    if (!text) {
      res.status(400).json({
        error: "EMPTY_SUPPORT_CHAT_MESSAGE",
        message: "Type a message before sending it."
      });
      return;
    }

    const payload = await queueMutation((store) => {
      const conversation = findConversationById(store, conversationId);
      if (!conversation) {
        throw new Error("SUPPORT_CHAT_NOT_FOUND");
      }

      const createdAt = new Date().toISOString();

      conversation.messages.push({
        id: `msg_${randomUUID()}`,
        role: "admin",
        text,
        createdAt
      });

      refreshConversationMeta(conversation);
      sortConversations(store);

      return buildAdminConversationPayload(conversation);
    }).catch((error) => {
      if (error?.message === "SUPPORT_CHAT_NOT_FOUND") {
        return null;
      }

      throw error;
    });

    if (!payload) {
      res.status(404).json({
        error: "SUPPORT_CHAT_NOT_FOUND",
        message: "Conversation was not found."
      });
      return;
    }

    res.status(201).json(payload);
  });

  return {
    adminConfigured: Boolean(safeAdminToken),
    operatorName: safeOperatorName
  };
}
