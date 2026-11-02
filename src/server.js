import { createHash, randomBytes, timingSafeEqual } from "node:crypto";
import { createReadStream, createWriteStream } from "node:fs";
import { mkdir, readFile, rename, rm, stat, writeFile } from "node:fs/promises";
import { createServer } from "node:http";
import { extname, join, resolve } from "node:path";
import { pipeline } from "node:stream/promises";
import { pathToFileURL } from "node:url";

const MAX_FILE_SIZE = 100 * 1024 * 1024;
const MAX_TRACKS = 5000;
const MAX_DISCS = 500;
const MAX_SCHEDULES = 200;
const AUDIO_EXTENSIONS = new Set([".aac", ".m4a", ".mp3", ".ogg", ".opus", ".wav"]);
const MIME_TYPES = new Map([
  [".aac", "audio/aac"],
  [".m4a", "audio/mp4"],
  [".mp3", "audio/mpeg"],
  [".ogg", "audio/ogg"],
  [".opus", "audio/opus"],
  [".wav", "audio/wav"]
]);

function emptyState() {
  return {
    version: 1,
    discs: [],
    tracks: [],
    playlist: [],
    currentIndex: null,
    isPlaying: false,
    repeat: true,
    schedules: []
  };
}

function jsonResponse(res, status, value, headers = {}) {
  res.writeHead(status, { "Content-Type": "application/json; charset=utf-8", ...headers });
  res.end(JSON.stringify(value));
}

function fail(res, status, message, headers = {}) {
  jsonResponse(res, status, { ok: false, message }, headers);
}

function escapeHtml(value) {
  return String(value).replace(/[&<>"']/g, (character) => ({
    "&": "&amp;",
    "<": "&lt;",
    ">": "&gt;",
    '"': "&quot;",
    "'": "&#39;"
  })[character]);
}

function validDiscName(name) {
  return typeof name === "string" && name.trim().length > 0 && [...name].length <= 100;
}

function discIdFor(name) {
  return name.normalize("NFKD")
    .replace(/[\u0300-\u036f]/g, "")
    .toLowerCase()
    .replace(/[^a-z0-9]+/g, "-")
    .replace(/^-+|-+$/g, "");
}

function validFileName(name) {
  return typeof name === "string"
    && name.length > 0
    && name.length <= 255
    && !name.startsWith(".")
    && !name.includes("/")
    && !name.includes("\\")
    && !/[\u0000-\u001f\u007f]/.test(name)
    && name !== "."
    && name !== ".."
    && AUDIO_EXTENSIONS.has(extname(name).toLowerCase());
}

function parseTrackId(id) {
  if (typeof id !== "string") return null;
  const slash = id.indexOf("/");
  if (slash < 1 || slash !== id.lastIndexOf("/")) return null;
  const discId = id.slice(0, slash);
  const name = id.slice(slash + 1);
  if (!/^[a-z0-9]+(?:-[a-z0-9]+)*$/.test(discId) || !validFileName(name)) return null;
  return { discId, name };
}

function trackPath(dataDir, id) {
  const parsed = parseTrackId(id);
  if (!parsed) return null;
  return join(dataDir, "audio", parsed.discId, parsed.name);
}

function toTrack(state, id) {
  const track = state.tracks.find((entry) => entry.id === id);
  const disc = state.discs.find((entry) => entry.id === track?.discId);
  if (!track || !disc) return null;
  return { ...track, discName: disc.name };
}

function playerState(state) {
  const queue = state.playlist.map((id) => {
    const track = toTrack(state, id);
    return track ? { ...track, url: "/api/player/audio" } : null;
  }).filter(Boolean);
  const currentIndex = state.currentIndex !== null && state.currentIndex >= 0
    && state.currentIndex < queue.length ? state.currentIndex : null;
  return {
    status: queue.length ? "ready" : "empty",
    audioSource: "library",
    queue,
    currentIndex,
    currentTrack: currentIndex === null ? null : queue[currentIndex],
    isPlaying: currentIndex === null ? false : state.isPlaying,
    repeat: state.repeat
  };
}

function normalizeState(value) {
  if (!value || value.version !== 1 || !Array.isArray(value.discs)
    || !Array.isArray(value.tracks) || !Array.isArray(value.playlist)
    || !Array.isArray(value.schedules)) {
    throw new Error("El archivo de datos no tiene un formato válido (versión 1).");
  }
  const trackIds = new Set(value.tracks.map((track) => track.id));
  value.playlist = value.playlist.filter((id, index, list) =>
    trackIds.has(id) && list.indexOf(id) === index).slice(0, MAX_TRACKS);
  value.schedules = value.schedules.filter((schedule) => trackIds.has(schedule.trackId));
  if (!Number.isInteger(value.currentIndex) || value.currentIndex < 0
    || value.currentIndex >= value.playlist.length) value.currentIndex = null;
  value.isPlaying = Boolean(value.isPlaying && value.currentIndex !== null);
  value.repeat = value.repeat !== false;
  return value;
}

function parseRange(header, size) {
  if (!header) return null;
  const match = /^bytes=(\d*)-(\d*)$/.exec(header);
  if (!match || (!match[1] && !match[2]) || size <= 0) return false;
  let start;
  let end;
  if (!match[1]) {
    const suffix = Number(match[2]);
    if (!Number.isSafeInteger(suffix) || suffix <= 0) return false;
    start = Math.max(0, size - suffix);
    end = size - 1;
  } else {
    start = Number(match[1]);
    end = match[2] ? Number(match[2]) : size - 1;
    if (!Number.isSafeInteger(start) || !Number.isSafeInteger(end) || start >= size || start > end) {
      return false;
    }
    end = Math.min(end, size - 1);
  }
  return { start, end };
}

function cookieValue(req, name) {
  const cookies = req.headers.cookie?.split(";") ?? [];
  for (const cookie of cookies) {
    const [key, ...rest] = cookie.trim().split("=");
    if (key === name) return rest.join("=");
  }
  return "";
}

export async function createLibraryServer({
  dataDir = process.env.DATA_DIR ?? "./data",
  apiKey = process.env.API_KEY ?? "",
  host = process.env.HOST ?? "0.0.0.0",
  port = Number(process.env.PORT ?? 3001)
} = {}) {
  const root = resolve(dataDir);
  const audioRoot = join(root, "audio");
  const tempRoot = join(root, "tmp");
  const statePath = join(root, "library.json");
  const configuredKey = Buffer.byteLength(apiKey, "utf8") >= 32;
  const sessions = new Map();
  const authAttempts = new Map();
  await Promise.all([mkdir(audioRoot, { recursive: true }), mkdir(tempRoot, { recursive: true })]);

  let state;
  try {
    state = normalizeState(JSON.parse(await readFile(statePath, "utf8")));
  } catch (error) {
    if (error.code !== "ENOENT") throw error;
    state = emptyState();
  }

  let mutationQueue = Promise.resolve();
  const mutate = (operation) => {
    const pending = mutationQueue.then(async () => {
      const result = await operation();
      await saveState();
      return result;
    });
    mutationQueue = pending.catch(() => {});
    return pending;
  };

  async function saveState() {
    const tempPath = join(tempRoot, `state-${randomBytes(8).toString("hex")}.tmp`);
    await writeFile(tempPath, JSON.stringify(state), { mode: 0o600 });
    await rename(tempPath, statePath);
  }

  function authResult(req, res) {
    if (!configuredKey) {
      fail(res, 503, "La clave API no está configurada.");
      return false;
    }
    const ip = req.socket.remoteAddress ?? "unknown";
    const now = Date.now();
    const attempts = (authAttempts.get(ip) ?? []).filter((time) => now - time < 60_000);
    if (attempts.length >= 10) {
      authAttempts.set(ip, attempts);
      fail(res, 429, "Demasiados intentos de autenticación.");
      return false;
    }
    const authorization = req.headers.authorization ?? "";
    const supplied = authorization.startsWith("Bearer ") ? authorization.slice(7) : "";
    const suppliedBuffer = Buffer.from(supplied);
    const expectedBuffer = Buffer.from(apiKey);
    const matches = suppliedBuffer.length === expectedBuffer.length
      && timingSafeEqual(suppliedBuffer, expectedBuffer);
    if (!matches) {
      attempts.push(now);
      authAttempts.set(ip, attempts);
      fail(res, 401, "Se requiere una clave API válida.", { "WWW-Authenticate": "Bearer" });
      return false;
    }
    return true;
  }

  function authRateLimited(req) {
    const ip = req.socket.remoteAddress ?? "unknown";
    const attempts = (authAttempts.get(ip) ?? []).filter((time) => Date.now() - time < 60_000);
    authAttempts.set(ip, attempts);
    return attempts.length >= 10;
  }

  function recordAuthFailure(req) {
    const ip = req.socket.remoteAddress ?? "unknown";
    const attempts = authAttempts.get(ip) ?? [];
    attempts.push(Date.now());
    authAttempts.set(ip, attempts);
  }

  function adminSession(req) {
    const token = cookieValue(req, "nas_session");
    const expires = sessions.get(token);
    if (!token || !expires || expires < Date.now()) {
      sessions.delete(token);
      return false;
    }
    sessions.set(token, Date.now() + 8 * 60 * 60 * 1000);
    return true;
  }

  async function readJson(req, limit = 1024 * 1024) {
    const chunks = [];
    let length = 0;
    for await (const chunk of req) {
      length += chunk.length;
      if (length > limit) {
        const error = new Error("El cuerpo de la solicitud es demasiado grande.");
        error.status = 413;
        throw error;
      }
      chunks.push(chunk);
    }
    try {
      return JSON.parse(Buffer.concat(chunks).toString("utf8"));
    } catch {
      const error = new Error("El cuerpo debe contener JSON válido.");
      error.status = 400;
      throw error;
    }
  }

  async function readForm(req, limit = 1024 * 1024) {
    const chunks = [];
    let length = 0;
    for await (const chunk of req) {
      length += chunk.length;
      if (length > limit) {
        const error = new Error("El cuerpo de la solicitud es demasiado grande.");
        error.status = 413;
        throw error;
      }
      chunks.push(chunk);
    }
    return new URLSearchParams(Buffer.concat(chunks).toString("utf8"));
  }

  async function receiveUpload(req, tempPath) {
    const hash = createHash("sha256");
    let sizeBytes = 0;
    const output = createWriteStream(tempPath, { flags: "wx", mode: 0o600 });
    try {
      for await (const chunk of req) {
        sizeBytes += chunk.length;
        if (sizeBytes > MAX_FILE_SIZE) {
          const error = new Error("El archivo supera el límite de 100 MiB.");
          error.status = 413;
          throw error;
        }
        hash.update(chunk);
        await new Promise((resolveWrite, rejectWrite) => {
          output.write(chunk, (error) => error ? rejectWrite(error) : resolveWrite());
        });
      }
      await new Promise((resolveEnd, rejectEnd) => {
        output.once("error", rejectEnd);
        output.end(resolveEnd);
      });
    } catch (error) {
      output.destroy();
      await rm(tempPath, { force: true });
      throw error;
    }
    const sha256 = hash.digest("hex");
    const expected = req.headers["x-content-sha256"];
    if (expected !== undefined && (typeof expected !== "string"
      || !/^[a-f0-9]{64}$/i.test(expected) || expected.toLowerCase() !== sha256)) {
      await rm(tempPath, { force: true });
      const error = new Error("El checksum SHA-256 no coincide.");
      error.status = 422;
      throw error;
    }
    return { sizeBytes, sha256 };
  }

  async function sendAudio(req, res, id) {
    const path = trackPath(root, id);
    const track = path && toTrack(state, id);
    if (!track) return fail(res, 404, "El tema no existe.");
    let fileInfo;
    try {
      fileInfo = await stat(path);
    } catch (error) {
      if (error.code === "ENOENT") return fail(res, 404, "El archivo de audio no existe.");
      throw error;
    }
    const range = parseRange(req.headers.range, fileInfo.size);
    if (range === false) {
      res.writeHead(416, {
        "Content-Range": `bytes */${fileInfo.size}`,
        "Accept-Ranges": "bytes"
      });
      return res.end();
    }
    const headers = {
      "Content-Type": MIME_TYPES.get(extname(track.name).toLowerCase()) ?? "application/octet-stream",
      "Accept-Ranges": "bytes",
      "Cache-Control": "no-store"
    };
    if (range) {
      headers["Content-Range"] = `bytes ${range.start}-${range.end}/${fileInfo.size}`;
      headers["Content-Length"] = range.end - range.start + 1;
      res.writeHead(206, headers);
    } else {
      headers["Content-Length"] = fileInfo.size;
      res.writeHead(200, headers);
    }
    const stream = range
      ? createReadStream(path, { start: range.start, end: range.end })
      : createReadStream(path);
    await pipeline(stream, res);
  }

  function htmlPage(content, title = "Biblioteca musical") {
    return `<!doctype html><html lang="es"><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1"><title>${escapeHtml(title)}</title>
<style>
body{font:16px system-ui,sans-serif;max-width:1000px;margin:2rem auto;padding:0 1rem;background:#f5f6f8;color:#182230}h1,h2{color:#152d4a}section,article{background:white;border:1px solid #dce1e8;border-radius:10px;padding:1rem;margin:1rem 0}label{display:block;margin:.7rem 0}input,select,button{font:inherit;padding:.5rem}button{cursor:pointer;background:#155eef;color:white;border:0;border-radius:5px}button.danger{background:#b42318}li{margin:.65rem 0}small,.muted{color:#667085}.row{display:flex;gap:.5rem;flex-wrap:wrap;align-items:center}form.inline{display:inline}pre{white-space:pre-wrap}
.track-card{display:grid;gap:.8rem;margin:.75rem 0}.track-heading,.progress-label{display:flex;justify-content:space-between;align-items:center;gap:1rem}.track-card audio,#uploadPreview{width:100%;max-width:520px}.extension{display:inline-block;margin-left:.5rem;font-weight:700;color:#475467}.error{color:#b42318}#uploadProgress{margin-top:1rem}#uploadBar{display:block;width:100%;height:1.2rem;margin-top:.4rem}#fileOptions[hidden],#uploadProgress[hidden],[hidden]{display:none!important}input[type=search]{min-width:min(280px,80vw)}
</style><body>${content}</body></html>`;
  }

  function pageResponse(res, content, status = 200) {
    res.writeHead(status, { "Content-Type": "text/html; charset=utf-8", "Cache-Control": "no-store" });
    res.end(content);
  }

  const server = createServer(async (req, res) => {
    const url = new URL(req.url, "http://localhost");
    try {
      if (req.method === "GET" && url.pathname === "/") {
        res.writeHead(302, { Location: "/admin" });
        return res.end();
      }
      if (req.method === "GET" && url.pathname === "/api/health") {
        let freeBytes = null;
        try {
          const { statfs } = await import("node:fs/promises");
          const space = await statfs(root);
          freeBytes = Number(space.bavail) * Number(space.bsize);
        } catch { /* free space is optional in the contract */ }
        return jsonResponse(res, 200, {
          ok: true, apiVersion: 1, migrationSupport: true, maxFileSize: MAX_FILE_SIZE, freeBytes
        });
      }
      if (req.method === "GET" && url.pathname === "/api/admin/config") {
        return jsonResponse(res, 200, { apiKeyConfigured: configuredKey });
      }
      if (url.pathname === "/api/admin/session" && req.method === "GET") {
        if (!authResult(req, res)) return;
        return jsonResponse(res, 200, { authenticated: true });
      }
      if (url.pathname.startsWith("/api/") && ["POST", "PUT", "DELETE"].includes(req.method)) {
        if (!authResult(req, res)) return;
      }
      if (req.method === "GET" && url.pathname === "/api/discs") {
        if (state.discs.length > MAX_DISCS || state.tracks.length > MAX_TRACKS) {
          return fail(res, 500, "El catálogo supera los límites admitidos.");
        }
        const discs = state.discs.map((disc) => ({
          ...disc,
          tracks: state.tracks.filter((track) => track.discId === disc.id)
            .map((track) => ({ ...track, discName: disc.name }))
        }));
        return jsonResponse(res, 200, { discs });
      }
      if (url.pathname === "/api/discs" && req.method === "POST") {
        const body = await readJson(req);
        if (!body || typeof body !== "object" || Array.isArray(body) || !validDiscName(body.name)) {
          return fail(res, 400, "El nombre del disco debe tener entre 1 y 100 caracteres.");
        }
        const name = body.name.trim();
        const id = discIdFor(name);
        if (!id) return fail(res, 400, "El nombre no genera un ID de disco válido.");
        const result = await mutate(() => {
          const existing = state.discs.find((disc) => disc.id === id);
          if (existing) return { created: false, disc: existing };
          if (state.discs.length >= MAX_DISCS) {
            const error = new Error("Se alcanzó el límite de 500 discos.");
            error.status = 409;
            throw error;
          }
          const disc = { id, name };
          state.discs.push(disc);
          return { created: true, disc };
        });
        return jsonResponse(res, result.created ? 201 : 200, result);
      }
      if (url.pathname === "/api/discs" && req.method === "DELETE") {
        const id = url.searchParams.get("id");
        if (!id || !/^[a-z0-9]+(?:-[a-z0-9]+)*$/.test(id)) return fail(res, 400, "ID de disco inválido.");
        const disc = state.discs.find((entry) => entry.id === id);
        if (!disc) return fail(res, 404, "El disco no existe.");
        await rm(join(audioRoot, id), { recursive: true, force: true });
        await mutate(() => {
          state.discs = state.discs.filter((entry) => entry.id !== id);
          state.tracks = state.tracks.filter((track) => track.discId !== id);
          const removed = new Set(state.tracks.map((track) => track.id));
          state.playlist = state.playlist.filter((trackId) => removed.has(trackId));
          state.schedules = state.schedules.filter((schedule) => removed.has(schedule.trackId));
          state.currentIndex = state.playlist.length && state.currentIndex !== null
            ? Math.min(state.currentIndex, state.playlist.length - 1) : null;
          if (state.currentIndex === null) state.isPlaying = false;
        });
        return jsonResponse(res, 200, { ok: true });
      }
      if (url.pathname === "/api/tracks" && req.method === "POST") {
        if (req.headers["content-type"]?.split(";")[0] !== "application/octet-stream") {
          return fail(res, 400, "Content-Type debe ser application/octet-stream.");
        }
        let fileName;
        try { fileName = decodeURIComponent(req.headers["x-file-name"] ?? ""); }
        catch { return fail(res, 400, "X-File-Name no está URL-encoded correctamente."); }
        if (!validFileName(fileName)) return fail(res, 400, "Nombre o extensión de archivo no permitidos.");
        const discName = url.searchParams.get("disc")?.trim();
        if (!validDiscName(discName)) return fail(res, 400, "El parámetro disc debe tener entre 1 y 100 caracteres.");
        const discId = discIdFor(discName);
        if (!discId) return fail(res, 400, "El nombre no genera un ID de disco válido.");
        const trackId = `${discId}/${fileName}`;
        const tempPath = join(tempRoot, `upload-${randomBytes(12).toString("hex")}.tmp`);
        const details = await receiveUpload(req, tempPath);
        const directory = join(audioRoot, discId);
        await mkdir(directory, { recursive: true });
        const target = trackPath(root, trackId);
        try {
          const result = await mutate(async () => {
            if (state.tracks.some((track) => track.id === trackId)) {
              const error = new Error("Ya existe un tema con ese nombre.");
              error.status = 409;
              throw error;
            }
            if (state.tracks.length >= MAX_TRACKS) {
              const error = new Error("Se alcanzó el límite de 5000 temas.");
              error.status = 409;
              throw error;
            }
            if (!state.discs.some((disc) => disc.id === discId)) {
              if (state.discs.length >= MAX_DISCS) {
                const error = new Error("Se alcanzó el límite de 500 discos.");
                error.status = 409;
                throw error;
              }
              state.discs.push({ id: discId, name: discName });
            }
            await rename(tempPath, target);
            const track = {
              id: trackId,
              name: fileName,
              title: fileName.slice(0, -extname(fileName).length),
              discId,
              sizeBytes: details.sizeBytes,
              sha256: details.sha256
            };
            state.tracks.push(track);
            const queued = state.playlist.length < MAX_TRACKS;
            if (queued) {
              state.playlist.push(trackId);
              if (state.currentIndex === null) state.currentIndex = 0;
            }
            const storedDisc = state.discs.find((disc) => disc.id === discId);
            return { queued, track: { ...track, discName: storedDisc.name } };
          });
          return jsonResponse(res, 201, {
            ...result,
            ...(!result.queued ? { message: "El archivo se guardó, pero la playlist alcanzó el límite de 5000 temas." } : {})
          });
        } catch (error) {
          await rm(tempPath, { force: true });
          throw error;
        }
      }
      if (url.pathname === "/api/tracks" && req.method === "PUT") {
        const trackId = url.searchParams.get("id");
        const path = trackPath(root, trackId);
        const track = path && toTrack(state, trackId);
        if (!track) return fail(res, 404, "El tema no existe.");
        if (req.headers["content-type"]?.split(";")[0] !== "application/octet-stream") {
          return fail(res, 400, "Content-Type debe ser application/octet-stream.");
        }
        const tempPath = join(tempRoot, `replace-${randomBytes(12).toString("hex")}.tmp`);
        const details = await receiveUpload(req, tempPath);
        try {
          await mutate(async () => {
            await rename(tempPath, path);
            track.sizeBytes = details.sizeBytes;
            track.sha256 = details.sha256;
          });
        } catch (error) {
          await rm(tempPath, { force: true });
          throw error;
        }
        return jsonResponse(res, 200, { ok: true, track });
      }
      if (url.pathname === "/api/tracks" && req.method === "DELETE") {
        const id = url.searchParams.get("id");
        const path = trackPath(root, id);
        if (!path || !toTrack(state, id)) return fail(res, 404, "El tema no existe.");
        await rm(path);
        await mutate(() => {
          state.tracks = state.tracks.filter((track) => track.id !== id);
          state.playlist = state.playlist.filter((trackId) => trackId !== id);
          state.schedules = state.schedules.filter((schedule) => schedule.trackId !== id);
          state.currentIndex = state.playlist.length && state.currentIndex !== null
            ? Math.min(state.currentIndex, state.playlist.length - 1) : null;
          if (state.currentIndex === null) state.isPlaying = false;
        });
        return jsonResponse(res, 200, { ok: true });
      }
      if (req.method === "GET" && ["/api/player/state", "/api/player/playlist"].includes(url.pathname)) {
        return jsonResponse(res, 200, playerState(state));
      }
      if (url.pathname === "/api/player/audio" && req.method === "GET") {
        const current = playerState(state).currentTrack;
        if (!current) return fail(res, 404, "No hay un tema actual.");
        return await sendAudio(req, res, current.id);
      }
      if (url.pathname === "/api/audio" && req.method === "GET") {
        const id = url.searchParams.get("id");
        if (!parseTrackId(id)) return fail(res, 400, "ID de tema inválido.");
        return await sendAudio(req, res, id);
      }
      if (url.pathname === "/api/player/playlist" && req.method === "PUT") {
        const body = await readJson(req);
        if (!body || typeof body !== "object" || Array.isArray(body)
          || !Array.isArray(body.tracks) || body.tracks.length > MAX_TRACKS
          || body.tracks.some((id) => typeof id !== "string")
          || new Set(body.tracks).size !== body.tracks.length) {
          return fail(res, 400, "tracks debe ser una lista única de hasta 5000 IDs.");
        }
        if (body.repeat !== undefined && typeof body.repeat !== "boolean") {
          return fail(res, 400, "repeat debe ser booleano.");
        }
        if (body.tracks.some((id) => !toTrack(state, id))) return fail(res, 404, "Uno o más temas no existen.");
        const result = await mutate(() => {
          const currentId = state.currentIndex === null ? null : state.playlist[state.currentIndex];
          state.playlist = body.tracks;
          if (body.repeat !== undefined) state.repeat = body.repeat;
          state.currentIndex = currentId && state.playlist.includes(currentId)
            ? state.playlist.indexOf(currentId) : state.playlist.length ? 0 : null;
          if (state.currentIndex === null) state.isPlaying = false;
          return playerState(state);
        });
        return jsonResponse(res, 200, { state: result });
      }
      if (req.method === "POST" && ["/api/player/next", "/api/player/previous",
        "/api/player/current", "/api/player/playback"].includes(url.pathname)) {
        const body = await readJson(req);
        if (!body || typeof body !== "object" || Array.isArray(body)) {
          return fail(res, 400, "El cuerpo debe ser un objeto JSON.");
        }
        const result = await mutate(() => {
          if (url.pathname === "/api/player/playback") {
            if (typeof body.isPlaying !== "boolean") {
              const error = new Error("isPlaying debe ser booleano.");
              error.status = 400;
              throw error;
            }
            state.isPlaying = body.isPlaying && state.playlist.length > 0;
          } else if (url.pathname === "/api/player/current") {
            if (!Number.isInteger(body.index) || body.index < 0 || body.index >= state.playlist.length) {
              const error = new Error("index debe señalar un tema de la playlist.");
              error.status = 400;
              throw error;
            }
            state.currentIndex = body.index;
          } else if (state.playlist.length) {
            const current = state.currentIndex ?? 0;
            const direction = url.pathname.endsWith("/next") ? 1 : -1;
            const next = current + direction;
            if (next >= 0 && next < state.playlist.length) state.currentIndex = next;
            else if (state.repeat) state.currentIndex = direction > 0 ? 0 : state.playlist.length - 1;
          }
          return playerState(state);
        });
        return jsonResponse(res, 200, { state: result });
      }
      if (url.pathname === "/api/schedules" && req.method === "GET") {
        return jsonResponse(res, 200, { schedules: state.schedules });
      }
      if (url.pathname === "/api/schedules" && req.method === "PUT") {
        const body = await readJson(req);
        if (!body || typeof body !== "object" || Array.isArray(body)
          || !Array.isArray(body.schedules) || body.schedules.length > MAX_SCHEDULES) {
          return fail(res, 400, "schedules debe ser una lista de hasta 200 programaciones.");
        }
        const ids = new Set();
        for (const schedule of body.schedules) {
          if (!schedule || typeof schedule.id !== "string" || !schedule.id.trim()
            || ids.has(schedule.id) || typeof schedule.trackId !== "string"
            || !["once", "daily", "weekly"].includes(schedule.recurrence)
            || typeof schedule.scheduledAt !== "string" || !Number.isFinite(Date.parse(schedule.scheduledAt))) {
            return fail(res, 400, "Cada programación requiere id, trackId, scheduledAt válido y recurrence once/daily/weekly.");
          }
          ids.add(schedule.id);
          if (!toTrack(state, schedule.trackId)) return fail(res, 404, "Uno o más temas programados no existen.");
        }
        const saved = await mutate(() => {
          state.schedules = body.schedules.map(({ id, trackId, scheduledAt, recurrence }) =>
            ({ id, trackId, scheduledAt, recurrence }));
          return state.schedules;
        });
        return jsonResponse(res, 200, { schedules: saved });
      }
      if (url.pathname === "/admin/login" && req.method === "POST") {
        if (!configuredKey) return pageResponse(res, htmlPage("<h1>API_KEY no está configurada</h1>"), 503);
        if (authRateLimited(req)) return pageResponse(res, htmlPage("<h1>Demasiados intentos</h1>"), 429);
        const form = await readForm(req, 4096);
        const suppliedBuffer = Buffer.from(form.get("key") ?? "");
        const expectedBuffer = Buffer.from(apiKey);
        const matches = suppliedBuffer.length === expectedBuffer.length
          && timingSafeEqual(suppliedBuffer, expectedBuffer);
        if (!matches) {
          recordAuthFailure(req);
          return pageResponse(res, htmlPage("<h1>Clave incorrecta</h1><p><a href=\"/admin\">Volver</a></p>"), 401);
        }
        const token = randomBytes(32).toString("hex");
        sessions.set(token, Date.now() + 8 * 60 * 60 * 1000);
        const secure = req.socket.encrypted || req.headers["x-forwarded-proto"] === "https";
        res.writeHead(303, {
          Location: "/admin",
          "Set-Cookie": `nas_session=${token}; HttpOnly; SameSite=Strict; Path=/; Max-Age=28800${secure ? "; Secure" : ""}`
        });
        return res.end();
      }
      if (url.pathname === "/admin/logout" && req.method === "POST") {
        sessions.delete(cookieValue(req, "nas_session"));
        res.writeHead(303, { Location: "/admin", "Set-Cookie": "nas_session=; HttpOnly; SameSite=Strict; Path=/; Max-Age=0" });
        return res.end();
      }
      if (url.pathname === "/admin/upload" && req.method === "POST") {
        if (!adminSession(req)) return fail(res, 401, "Sesión expirada.");
        if (req.headers["content-type"]?.split(";")[0] !== "application/octet-stream") {
          return fail(res, 400, "Content-Type debe ser application/octet-stream.");
        }
        const fileName = decodeURIComponent(req.headers["x-file-name"] ?? "");
        const discName = url.searchParams.get("disc")?.trim();
        if (!validFileName(fileName) || !validDiscName(discName)) return fail(res, 400, "Nombre de disco o archivo inválido.");
        const discId = discIdFor(discName);
        const id = `${discId}/${fileName}`;
        const tempPath = join(tempRoot, `admin-${randomBytes(12).toString("hex")}.tmp`);
        const details = await receiveUpload(req, tempPath);
        try {
          await mutate(async () => {
            if (state.tracks.some((track) => track.id === id)) {
              const error = new Error(`Ya existe ${fileName}.`);
              error.status = 409;
              throw error;
            }
            if (state.tracks.length >= MAX_TRACKS) {
              const error = new Error("Se alcanzó el límite de 5000 temas.");
              error.status = 409;
              throw error;
            }
            if (!state.discs.some((disc) => disc.id === discId) && state.discs.length >= MAX_DISCS) {
              const error = new Error("Se alcanzó el límite de 500 discos.");
              error.status = 409;
              throw error;
            }
            if (!state.discs.some((disc) => disc.id === discId)) state.discs.push({ id: discId, name: discName });
            await mkdir(join(audioRoot, discId), { recursive: true });
            await rename(tempPath, trackPath(root, id));
            const track = {
              id, name: fileName, title: fileName.slice(0, -extname(fileName).length),
              discId, sizeBytes: details.sizeBytes, sha256: details.sha256
            };
            state.tracks.push(track);
            if (state.playlist.length < MAX_TRACKS) {
              state.playlist.push(id);
              if (state.currentIndex === null) state.currentIndex = 0;
            }
          });
        } catch (error) {
          await rm(tempPath, { force: true });
          throw error;
        }
        return jsonResponse(res, 201, { ok: true });
      }
      if (url.pathname === "/admin/replace" && req.method === "PUT") {
        if (!adminSession(req)) return fail(res, 401, "Sesión expirada.");
        if (req.headers["content-type"]?.split(";")[0] !== "application/octet-stream") {
          return fail(res, 400, "Content-Type debe ser application/octet-stream.");
        }
        const id = url.searchParams.get("id");
        const path = trackPath(root, id);
        const track = path && state.tracks.find((entry) => entry.id === id);
        if (!track) return fail(res, 404, "El tema no existe.");
        const tempPath = join(tempRoot, `replace-${randomBytes(12).toString("hex")}.tmp`);
        const details = await receiveUpload(req, tempPath);
        try {
          await mutate(async () => {
            await rename(tempPath, path);
            track.sizeBytes = details.sizeBytes;
            track.sha256 = details.sha256;
          });
        } catch (error) {
          await rm(tempPath, { force: true });
          throw error;
        }
        return jsonResponse(res, 200, { ok: true });
      }
      if (req.method === "GET" && url.pathname === "/admin") {
        if (!adminSession(req)) {
          return pageResponse(res, htmlPage(`<main><h1>Biblioteca musical</h1><section><h2>Iniciar sesión</h2>
<form method="post" action="/admin/login"><label>Clave API <input type="password" name="key" autocomplete="current-password" required></label>
<button>Entrar</button></form></section></main>`));
        }
        const tracks = state.tracks.map((track) => ({
          ...track, disc: state.discs.find((disc) => disc.id === track.discId)?.name ?? track.discId
        }));
        const discOptions = state.discs.map((disc) => `<option value="${escapeHtml(disc.id)}">${escapeHtml(disc.name)}</option>`).join("");
        const discList = state.discs.length ? `<ul>${state.discs.map((disc) => `<li>${escapeHtml(disc.name)} <form class="inline" method="post" action="/admin/discs/delete"><input type="hidden" name="data" value="${escapeHtml(JSON.stringify({ id: disc.id }))}"><button class="danger">Borrar disco y temas</button></form></li>`).join("")}</ul>` : "<p class=\"muted\">Aún no hay discos.</p>";
        const library = tracks.length ? tracks.map((track) => `<article class="track-card" data-disc="${escapeHtml(track.discId)}" data-search="${escapeHtml(`${track.disc} ${track.name}`.toLowerCase())}">
<div class="track-heading"><div><strong>${escapeHtml(track.title)}</strong><div class="muted">${escapeHtml(track.disc)} · ${escapeHtml(track.name)} · ${(track.sizeBytes / 1024 / 1024).toFixed(2)} MiB</div></div>
<form method="post" action="/admin/tracks/delete" onsubmit="return confirm('¿Borrar este tema de la biblioteca?')"><input type="hidden" name="data" value="${escapeHtml(JSON.stringify({ id: track.id }))}"><button class="danger">Borrar</button></form></div>
<audio controls preload="none" src="/api/audio?id=${encodeURIComponent(track.id)}"></audio></article>`).join("") : "<p id=\"emptyLibrary\" class=\"muted\">Todavía no hay archivos de audio. Selecciona un disco y sube el primero.</p>";
        const content = `<main><div class="row"><h1>Biblioteca musical</h1><form method="post" action="/admin/logout"><button class="danger">Cerrar sesión</button></form></div>
<p class="muted">${state.discs.length} discos · ${tracks.length} temas · ${state.schedules.length} programaciones</p>
<section><h2>Crear disco</h2><form method="post" action="/admin/discs"><label>Nombre <input name="name" maxlength="100" required></label><button>Crear</button></form></section>
<section><h2>Discos</h2>${discList}</section>
<section id="uploadSection"><h2>Subir archivo de audio</h2><p class="muted">Selecciona un disco existente. El archivo se vinculará a ese disco.</p>
<label>Disco <select id="discSelect" required><option value="">Seleccionar disco…</option>${discOptions}</select></label>
<label>Archivo de audio <input id="audioFile" type="file" accept=".aac,.m4a,.mp3,.ogg,.opus,.wav,audio/*" required></label>
<div id="fileOptions" hidden><label>Nombre del archivo <input id="fileTitle" maxlength="250" required><span id="fileExtension" class="extension"></span></label>
<p id="fileInfo" class="muted"></p><audio id="uploadPreview" controls preload="metadata"></audio></div>
<button id="uploadButton" type="button" disabled>Subir archivo</button>
<div id="uploadProgress" hidden><div class="progress-label"><span id="uploadStatus" role="status">Preparando…</span><span id="uploadPercent">0%</span></div><progress id="uploadBar" max="100" value="0">0%</progress></div>
<p id="uploadError" class="error" role="alert"></p></section>
<section id="library"><h2>Explorador de biblioteca</h2><div class="row"><label>Buscar <input id="librarySearch" type="search" placeholder="Nombre del tema o disco"></label><label>Disco <select id="libraryDisc"><option value="">Todos los discos</option>${discOptions}</select></label></div>
<p id="libraryCount" class="muted"></p><div id="libraryTracks">${library}</div><p id="noMatches" class="muted" hidden>No hay temas que coincidan con el filtro.</p></section>
<section><h2>Reemplazar contenido</h2><label>Tema <select id="replaceTrack" ${tracks.length ? "" : "disabled"}>${tracks.map((track) => `<option value="${escapeHtml(track.id)}">${escapeHtml(track.disc)} — ${escapeHtml(track.name)}</option>`).join("")}</select></label>
<label>Archivo nuevo <input id="replacementFile" type="file" accept=".aac,.m4a,.mp3,.ogg,.opus,.wav,audio/*" ${tracks.length ? "" : "disabled"}></label><button id="replaceButton" type="button" ${tracks.length ? "" : "disabled"}>Reemplazar contenido</button><p id="replaceStatus" role="status"></p></section>
<section><h2>Playlist</h2><p>Un ID por línea. Los temas nuevos se agregan automáticamente al final.</p>
<form method="post" action="/admin/playlist"><label>IDs en orden<textarea name="tracks" rows="8" style="width:100%" spellcheck="false">${escapeHtml(state.playlist.join("\n"))}</textarea></label><button>Guardar playlist</button></form>
<div class="row"><form method="post" action="/admin/playback"><input type="hidden" name="action" value="previous"><button>Anterior</button></form><form method="post" action="/admin/playback"><input type="hidden" name="action" value="${state.isPlaying ? "pause" : "play"}"><button>${state.isPlaying ? "Pausar" : "Reproducir"}</button></form><form method="post" action="/admin/playback"><input type="hidden" name="action" value="next"><button>Siguiente</button></form></div><p>Actual: ${escapeHtml(playerState(state).currentTrack?.name ?? "ninguno")}</p></section>
<section><h2>Programaciones</h2><form method="post" action="/admin/schedules"><label>Programaciones (JSON)<textarea name="schedules" rows="5" style="width:100%">${escapeHtml(JSON.stringify(state.schedules, null, 2))}</textarea></label><button>Guardar programaciones</button></form><small>Formato: [{"id":"mañana","trackId":"disco/tema.mp3","scheduledAt":"2026-11-03T08:00:00-03:00","recurrence":"daily"}]</small></section>
<script>
const audioFile=document.getElementById("audioFile"),fileTitle=document.getElementById("fileTitle"),fileExtension=document.getElementById("fileExtension"),preview=document.getElementById("uploadPreview"),fileOptions=document.getElementById("fileOptions"),uploadButton=document.getElementById("uploadButton"),discSelect=document.getElementById("discSelect");
let previewUrl="",validAudioFile=false;
audioFile.onchange=()=>{const file=audioFile.files[0],error=document.getElementById("uploadError");error.textContent="";if(previewUrl)URL.revokeObjectURL(previewUrl);previewUrl="";preview.removeAttribute("src");preview.load();if(!file){fileOptions.hidden=true;uploadButton.disabled=true;validAudioFile=false;return}const dot=file.name.lastIndexOf("."),extension=dot>0?file.name.slice(dot):"",base=dot>0?file.name.slice(0,dot):file.name;validAudioFile=[".aac",".m4a",".mp3",".ogg",".opus",".wav"].includes(extension.toLowerCase());fileTitle.value=base;fileExtension.textContent=extension;fileOptions.hidden=false;uploadButton.disabled=!validAudioFile||!discSelect.value;document.getElementById("fileInfo").textContent=(file.size/1024/1024).toFixed(2)+" MiB · "+extension.toUpperCase().replace(".","");if(!validAudioFile)error.textContent="Elige un archivo AAC, M4A, MP3, OGG, OPUS o WAV.";previewUrl=URL.createObjectURL(file);preview.src=previewUrl};
discSelect.onchange=()=>{uploadButton.disabled=!validAudioFile||!fileTitle.value.trim()||!discSelect.value};
fileTitle.oninput=()=>{uploadButton.disabled=!validAudioFile||!fileTitle.value.trim()||!discSelect.value};
function uploadWithProgress(file,disc,name){return new Promise((resolve,reject)=>{const request=new XMLHttpRequest(),progress=document.getElementById("uploadBar"),percent=document.getElementById("uploadPercent"),status=document.getElementById("uploadStatus");request.open("POST","/admin/upload?disc="+encodeURIComponent(disc));request.setRequestHeader("Content-Type","application/octet-stream");request.setRequestHeader("X-File-Name",encodeURIComponent(name));request.upload.onprogress=event=>{if(!event.lengthComputable)return;const value=Math.round(event.loaded/event.total*100);progress.value=value;percent.textContent=value+"%";status.textContent=value===100?"Procesando archivo…":"Subiendo "+file.name};request.onload=()=>{let result={};try{result=JSON.parse(request.responseText)}catch{}if(request.status>=200&&request.status<300)resolve(result);else reject(new Error(result.message||"No se pudo subir el archivo."))};request.onerror=()=>reject(new Error("Se perdió la conexión durante la subida."));request.onabort=()=>reject(new Error("Subida cancelada."));request.send(file)})}
uploadButton.onclick=async()=>{const file=audioFile.files[0],disc=discSelect.value,title=fileTitle.value.trim(),error=document.getElementById("uploadError"),box=document.getElementById("uploadProgress"),status=document.getElementById("uploadStatus");error.textContent="";if(!disc||!file||!title||!validAudioFile){error.textContent="Selecciona un disco, un archivo de audio compatible y un nombre válido.";return}const name=title+fileExtension.textContent;if(name.length>255){error.textContent="El nombre del archivo es demasiado largo.";return}uploadButton.disabled=true;box.hidden=false;document.getElementById("uploadBar").value=0;document.getElementById("uploadPercent").textContent="0%";try{await uploadWithProgress(file,disc,name);document.getElementById("uploadBar").value=100;document.getElementById("uploadPercent").textContent="100%";status.textContent="Archivo subido correctamente.";setTimeout(()=>location.reload(),800)}catch(uploadError){error.textContent=uploadError.message;status.textContent="Subida fallida.";uploadButton.disabled=false}};
document.getElementById("replaceButton").onclick=async()=>{const file=document.getElementById("replacementFile").files[0],id=document.getElementById("replaceTrack").value,status=document.getElementById("replaceStatus");if(!file)return status.textContent="Selecciona el nuevo archivo.";try{await new Promise((resolve,reject)=>{const request=new XMLHttpRequest();request.open("PUT","/admin/replace?id="+encodeURIComponent(id));request.setRequestHeader("Content-Type","application/octet-stream");request.upload.onprogress=event=>{if(event.lengthComputable)status.textContent="Subiendo "+Math.round(event.loaded/event.total*100)+"%"};request.onload=()=>request.status>=200&&request.status<300?resolve():reject(new Error("No se pudo reemplazar el archivo."));request.onerror=()=>reject(new Error("Se perdió la conexión."));request.send(file)});status.textContent="Archivo reemplazado.";setTimeout(()=>location.reload(),700)}catch(error){status.textContent=error.message}};
const libraryCards=[...document.querySelectorAll(".track-card")],librarySearch=document.getElementById("librarySearch"),libraryDisc=document.getElementById("libraryDisc");
function filterLibrary(){const query=librarySearch.value.trim().toLowerCase(),disc=libraryDisc.value;let visible=0;for(const card of libraryCards){const show=(!query||card.dataset.search.includes(query))&&(!disc||card.dataset.disc===disc);card.hidden=!show;if(show)visible++}document.getElementById("libraryCount").textContent=visible+" de "+libraryCards.length+" temas";document.getElementById("noMatches").hidden=visible!==0||libraryCards.length===0}
librarySearch.oninput=filterLibrary;libraryDisc.onchange=filterLibrary;filterLibrary();
</script></main>`;
        return pageResponse(res, htmlPage(content));
      }
      if (["/admin/discs", "/admin/discs/delete", "/admin/tracks/delete", "/admin/playlist",
        "/admin/playback", "/admin/schedules"].includes(url.pathname) && req.method === "POST") {
        if (!adminSession(req)) return pageResponse(res, htmlPage("<h1>Sesión expirada</h1>"), 401);
        const form = await readForm(req);
        const body = {};
        for (const [key, value] of form) {
          if (key === "data") Object.assign(body, JSON.parse(value));
          else if (key === "tracks") body.tracks = value.split(/\r?\n/).map((id) => id.trim()).filter(Boolean);
          else if (key === "schedules") body.schedules = JSON.parse(value);
          else body[key] = value;
        }
        const action = url.pathname;
        if (action === "/admin/discs") {
          if (!validDiscName(body.name)) return fail(res, 400, "Nombre de disco inválido.");
          const name = body.name.trim(), id = discIdFor(name);
          if (!id) return fail(res, 400, "Nombre de disco inválido.");
          if (!state.discs.some((disc) => disc.id === id) && state.discs.length >= MAX_DISCS) {
            return fail(res, 409, "Se alcanzó el límite de 500 discos.");
          }
          await mutate(() => { if (!state.discs.some((disc) => disc.id === id)) state.discs.push({ id, name }); });
        } else if (action === "/admin/discs/delete") {
          if (!/^[a-z0-9]+(?:-[a-z0-9]+)*$/.test(body.id ?? "")
            || !state.discs.some((disc) => disc.id === body.id)) return fail(res, 404, "El disco no existe.");
          await rm(join(audioRoot, body.id), { recursive: true, force: true });
          await mutate(() => {
            state.discs = state.discs.filter((disc) => disc.id !== body.id);
            state.tracks = state.tracks.filter((track) => track.discId !== body.id);
            const existing = new Set(state.tracks.map((track) => track.id));
            state.playlist = state.playlist.filter((id) => existing.has(id));
            state.schedules = state.schedules.filter((schedule) => existing.has(schedule.trackId));
            state.currentIndex = state.playlist.length && state.currentIndex !== null
              ? Math.min(state.currentIndex, state.playlist.length - 1) : null;
            if (state.currentIndex === null) state.isPlaying = false;
          });
        } else if (action === "/admin/tracks/delete") {
          const path = trackPath(root, body.id);
          if (!path || !toTrack(state, body.id)) return fail(res, 404, "El tema no existe.");
          await rm(path);
          await mutate(() => {
            state.tracks = state.tracks.filter((track) => track.id !== body.id);
            state.playlist = state.playlist.filter((id) => id !== body.id);
            state.schedules = state.schedules.filter((schedule) => schedule.trackId !== body.id);
            if (!state.playlist.length) state.currentIndex = null;
            else if (state.currentIndex !== null) state.currentIndex = Math.min(state.currentIndex, state.playlist.length - 1);
          });
        } else if (action === "/admin/playlist") {
          if (!Array.isArray(body.tracks) || body.tracks.length > MAX_TRACKS
            || new Set(body.tracks).size !== body.tracks.length
            || body.tracks.some((id) => !toTrack(state, id))) return fail(res, 400, "Playlist inválida.");
          await mutate(() => {
            state.playlist = body.tracks;
            state.currentIndex = state.playlist.length ? Math.min(state.currentIndex ?? 0, state.playlist.length - 1) : null;
            if (state.currentIndex === null) state.isPlaying = false;
          });
        } else if (action === "/admin/playback") {
          await mutate(() => {
            if (body.action === "next" || body.action === "previous") {
              if (!state.playlist.length) return;
              const step = body.action === "next" ? 1 : -1;
              const next = (state.currentIndex ?? 0) + step;
              state.currentIndex = state.repeat ? (next + state.playlist.length) % state.playlist.length
                : Math.max(0, Math.min(next, state.playlist.length - 1));
            } else if (body.action === "play" || body.action === "pause") {
              state.isPlaying = body.action === "play" && state.playlist.length > 0;
            } else throw Object.assign(new Error("Control desconocido."), { status: 400 });
          });
        } else if (action === "/admin/schedules") {
          const ids = new Set();
          if (!Array.isArray(body.schedules) || body.schedules.length > MAX_SCHEDULES
            || body.schedules.some((schedule) => {
              if (!schedule || typeof schedule.id !== "string" || !schedule.id.trim()
                || ids.has(schedule.id) || !["once", "daily", "weekly"].includes(schedule.recurrence)
                || typeof schedule.scheduledAt !== "string" || !Number.isFinite(Date.parse(schedule.scheduledAt))
                || !toTrack(state, schedule.trackId)) return true;
              ids.add(schedule.id);
              return false;
            })) return fail(res, 400, "Programaciones inválidas.");
          await mutate(() => { state.schedules = body.schedules; });
        }
        res.writeHead(303, { Location: "/admin" });
        return res.end();
      }
      return fail(res, 404, "Ruta no encontrada.");
    } catch (error) {
      if (res.headersSent) {
        res.destroy(error);
        return;
      }
      const status = Number.isInteger(error.status) ? error.status : 500;
      if (status === 500) console.error("Error interno:", error.message);
      return fail(res, status, status === 500 ? "Error interno del servicio." : error.message);
    }
  });

  let scheduleRunning = false;
  const scheduleTimer = setInterval(async () => {
    const now = Date.now();
    if (scheduleRunning || !state.schedules.some((schedule) => Date.parse(schedule.scheduledAt) <= now)) return;
    scheduleRunning = true;
    try {
      await mutate(() => {
        const remaining = [];
        for (const schedule of state.schedules) {
          const at = Date.parse(schedule.scheduledAt);
          if (!Number.isFinite(at) || at > now) {
            remaining.push(schedule);
            continue;
          }
          let index = state.playlist.indexOf(schedule.trackId);
          if (index < 0 && state.playlist.length < MAX_TRACKS) {
            state.playlist.push(schedule.trackId);
            index = state.playlist.length - 1;
          }
          if (index >= 0) state.currentIndex = index;
          if (schedule.recurrence !== "once") {
            const interval = schedule.recurrence === "daily" ? 86_400_000 : 7 * 86_400_000;
            const next = at + (Math.floor((now - at) / interval) + 1) * interval;
            remaining.push({ ...schedule, scheduledAt: new Date(next).toISOString() });
          }
        }
        state.schedules = remaining;
      });
    } catch (error) {
      console.error("No se pudieron ejecutar las programaciones:", error.message);
    } finally {
      scheduleRunning = false;
    }
  }, 1000);
  scheduleTimer.unref();

  return {
    server,
    listen: () => new Promise((resolveListen, rejectListen) => {
      server.once("error", rejectListen);
      server.listen(port, host, () => {
        server.removeListener("error", rejectListen);
        resolveListen(server.address());
      });
    }),
    close: () => new Promise((resolveClose, rejectClose) => {
      clearInterval(scheduleTimer);
      server.close((error) => error ? rejectClose(error) : resolveClose());
    })
  };
}

if (process.argv[1] && import.meta.url === pathToFileURL(resolve(process.argv[1])).href) {
  const app = await createLibraryServer();
  const address = await app.listen();
  console.log(`Biblioteca NAS escuchando en ${typeof address === "object" ? address.address : ""}:${typeof address === "object" ? address.port : ""}`);
}
