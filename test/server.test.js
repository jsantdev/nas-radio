import { afterEach, test } from "node:test";
import assert from "node:assert/strict";
import { mkdtemp, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { Script } from "node:vm";
import { createLibraryServer } from "../src/server.js";

const apiKey = "a".repeat(64);
const applications = [];
const tempDirectories = [];

async function startServer(options = {}) {
  const dataDir = await mkdtemp(join(tmpdir(), "nas-radio-test-"));
  tempDirectories.push(dataDir);
  const app = await createLibraryServer({
    dataDir,
    apiKey,
    host: "127.0.0.1",
    port: 0,
    ...options
  });
  const address = await app.listen();
  applications.push(app);
  return { app, base: `http://127.0.0.1:${address.port}`, dataDir };
}

function authHeaders(extra = {}) {
  return { Authorization: `Bearer ${apiKey}`, ...extra };
}

afterEach(async () => {
  await Promise.all(applications.splice(0).map((app) => app.close()));
  await Promise.all(tempDirectories.splice(0).map((directory) =>
    rm(directory, { recursive: true, force: true })));
});

test("health, authentication, upload, ranged audio, replace and cleanup", async () => {
  const { base } = await startServer();
  const health = await fetch(`${base}/api/health`);
  assert.equal(health.status, 200);
  assert.equal((await health.json()).apiVersion, 1);

  const denied = await fetch(`${base}/api/discs`, { method: "POST", body: "{}" });
  assert.equal(denied.status, 401);
  assert.equal(denied.headers.get("www-authenticate"), "Bearer");

  const created = await fetch(`${base}/api/discs`, {
    method: "POST",
    headers: authHeaders({ "Content-Type": "application/json" }),
    body: JSON.stringify({ name: "Clásicos Latinos" })
  });
  assert.equal(created.status, 201);

  const original = Buffer.from("0123456789");
  const uploaded = await fetch(`${base}/api/tracks?disc=Cl%C3%A1sicos%20Latinos`, {
    method: "POST",
    headers: authHeaders({
      "Content-Type": "application/octet-stream",
      "X-File-Name": encodeURIComponent("Cancion uno.mp3")
    }),
    body: original
  });
  assert.equal(uploaded.status, 201);
  const uploadJson = await uploaded.json();
  assert.equal(uploadJson.track.id, "clasicos-latinos/Cancion uno.mp3");
  assert.equal(uploadJson.queued, true);

  const partial = await fetch(`${base}/api/player/audio`, { headers: { Range: "bytes=2-5" } });
  assert.equal(partial.status, 206);
  assert.equal(partial.headers.get("content-range"), "bytes 2-5/10");
  assert.equal(Buffer.from(await partial.arrayBuffer()).toString(), "2345");

  const replacement = Buffer.from("abcdefgh");
  const replaced = await fetch(`${base}/api/tracks?id=${encodeURIComponent(uploadJson.track.id)}`, {
    method: "PUT",
    headers: authHeaders({ "Content-Type": "application/octet-stream" }),
    body: replacement
  });
  assert.equal(replaced.status, 200);
  assert.equal((await replaced.json()).track.id, uploadJson.track.id);
  const current = await fetch(`${base}/api/player/audio`);
  assert.equal(Buffer.from(await current.arrayBuffer()).toString(), "abcdefgh");

  const scheduledAt = new Date(Date.now() + 60_000).toISOString();
  const schedule = await fetch(`${base}/api/schedules`, {
    method: "PUT",
    headers: authHeaders({ "Content-Type": "application/json" }),
    body: JSON.stringify({
      schedules: [{ id: "prueba", trackId: uploadJson.track.id, scheduledAt, recurrence: "once" }]
    })
  });
  assert.equal(schedule.status, 200);

  const deleted = await fetch(`${base}/api/tracks?id=${encodeURIComponent(uploadJson.track.id)}`, {
    method: "DELETE", headers: authHeaders()
  });
  assert.equal(deleted.status, 200);
  const player = await fetch(`${base}/api/player/state`);
  assert.deepEqual((await player.json()).queue, []);
  const schedules = await fetch(`${base}/api/schedules`);
  assert.deepEqual((await schedules.json()).schedules, []);
});

test("persists catalogue and rejects a bad checksum", async () => {
  const first = await startServer();
  const content = Buffer.from("audio");
  const failedUpload = await fetch(`${first.base}/api/tracks?disc=Disco`, {
    method: "POST",
    headers: authHeaders({
      "Content-Type": "application/octet-stream",
      "X-File-Name": "tema.mp3",
      "X-Content-SHA256": "0".repeat(64)
    }),
    body: content
  });
  assert.equal(failedUpload.status, 422);

  const uploaded = await fetch(`${first.base}/api/tracks?disc=Disco`, {
    method: "POST",
    headers: authHeaders({
      "Content-Type": "application/octet-stream",
      "X-File-Name": "tema.mp3"
    }),
    body: content
  });
  assert.equal(uploaded.status, 201);
  const dataDir = first.dataDir;
  await first.app.close();
  applications.splice(applications.indexOf(first.app), 1);

  const reopened = await startServer({ dataDir });
  const catalog = await fetch(`${reopened.base}/api/discs`);
  const result = await catalog.json();
  assert.equal(result.discs[0].tracks[0].id, "disco/tema.mp3");
  assert.equal(result.discs[0].tracks[0].sizeBytes, content.length);
});

test("admin login keeps the API key out of the panel and uses an HTTP-only session", async () => {
  const { base } = await startServer();
  const page = await fetch(`${base}/admin`);
  assert.equal(page.status, 200);
  assert.equal((await page.text()).includes(apiKey), false);

  const login = await fetch(`${base}/admin/login`, {
    method: "POST",
    headers: { "Content-Type": "application/x-www-form-urlencoded" },
    body: new URLSearchParams({ key: apiKey }),
    redirect: "manual"
  });
  assert.equal(login.status, 303);
  assert.match(login.headers.get("set-cookie"), /HttpOnly/);
  assert.match(login.headers.get("set-cookie"), /SameSite=Strict/);
  assert.equal(login.headers.get("set-cookie").includes(apiKey), false);

  const cookie = login.headers.get("set-cookie").split(";")[0];
  const createdDisc = await fetch(`${base}/admin/discs`, {
    method: "POST",
    headers: { Cookie: cookie, "Content-Type": "application/x-www-form-urlencoded" },
    body: new URLSearchParams({ name: "Panel" }),
    redirect: "manual"
  });
  assert.equal(createdDisc.status, 303);

  const upload = await fetch(`${base}/admin/upload?disc=Panel`, {
    method: "POST",
    headers: {
      Cookie: cookie,
      "Content-Type": "application/octet-stream",
      "X-File-Name": "tema.mp3"
    },
    body: Buffer.from("admin-audio")
  });
  assert.equal(upload.status, 201);

  const replacement = await fetch(`${base}/admin/replace?id=panel%2Ftema.mp3`, {
    method: "PUT",
    headers: { Cookie: cookie, "Content-Type": "application/octet-stream" },
    body: Buffer.from("replaced")
  });
  assert.equal(replacement.status, 200);
  const catalog = await fetch(`${base}/api/discs`);
  assert.equal((await catalog.json()).discs[0].tracks[0].id, "panel/tema.mp3");

  const adminPage = await fetch(`${base}/admin`, { headers: { Cookie: cookie } });
  const adminHtml = await adminPage.text();
  assert.match(adminHtml, /id="discSelect" required/);
  assert.match(adminHtml, /id="uploadPreview"/);
  assert.match(adminHtml, /id="uploadBar"/);
  assert.match(adminHtml, /id="librarySearch"/);
  assert.match(adminHtml, /id="fileExtension"/);
  assert.ok(adminHtml.indexOf("id=\"uploadProgress\"") < adminHtml.indexOf("id=\"library\""));
  const inlineScript = adminHtml.match(/<script>([\s\S]*?)<\/script>/)?.[1];
  assert.ok(inlineScript);
  assert.doesNotThrow(() => new Script(inlineScript));
});

test("one-time schedules select the requested track and are removed", async () => {
  const { base } = await startServer();
  const uploaded = await fetch(`${base}/api/tracks?disc=Reloj`, {
    method: "POST",
    headers: authHeaders({
      "Content-Type": "application/octet-stream",
      "X-File-Name": "hora.mp3"
    }),
    body: Buffer.from("audio")
  });
  const { track } = await uploaded.json();
  const scheduledAt = new Date(Date.now() + 1200).toISOString();
  const response = await fetch(`${base}/api/schedules`, {
    method: "PUT",
    headers: authHeaders({ "Content-Type": "application/json" }),
    body: JSON.stringify({
      schedules: [{ id: "once", trackId: track.id, scheduledAt, recurrence: "once" }]
    })
  });
  assert.equal(response.status, 200);
  await new Promise((resolveWait) => setTimeout(resolveWait, 2300));
  const player = await fetch(`${base}/api/player/state`);
  assert.equal((await player.json()).currentTrack.id, track.id);
  const schedules = await fetch(`${base}/api/schedules`);
  assert.deepEqual((await schedules.json()).schedules, []);
});
