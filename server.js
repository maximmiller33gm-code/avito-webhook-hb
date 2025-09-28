// server.js — ESM
// package.json: { "type": "module", "scripts": { "start": "node server.js" } }

import express from 'express';
import fs from 'fs';
import { promises as fsp } from 'fs';
import path from 'path';
import crypto from 'crypto';
import process from 'process';
import { fileURLToPath } from 'url';

// __dirname for ESM
const __filename = fileURLToPath(import.meta.url);
const __dirname  = path.dirname(__filename);

// ===== ENV / CONFIG =====
const PORT               = Number(process.env.PORT || 3000);
const TASK_KEY           = process.env.TASK_KEY || 'dev-task-key';
const LOG_DIR            = process.env.LOG_DIR  || '/mnt/data/logs';
const TASK_DIR           = process.env.TASK_DIR || '/mnt/data/tasks';
const DEFAULT_REPLY      = process.env.DEFAULT_REPLY || 'Здравствуйте!';
const ONLY_FIRST_SYSTEM  = String(process.env.ONLY_FIRST_SYSTEM || 'true').toLowerCase() === 'true';
const WEBHOOK_SECRET     = process.env.WEBHOOK_SECRET || ''; // пусто = без строгой проверки секрета

// Сколько лог-файлов и сколько байт «хвоста» читать при проверке
const LOG_SCAN_FILES     = Math.max(1, Number(process.env.LOG_SCAN_FILES || 2));
const LOG_TAIL_BYTES     = Math.max(64 * 1024, Number(process.env.LOG_TAIL_BYTES || 512 * 1024));

// Живучесть локов
const VISIBILITY_TIMEOUT_MS = Number(process.env.VISIBILITY_TIMEOUT_MS || 180000); // 3 мин
const HEARTBEAT_GRACE_MS    = Number(process.env.HEARTBEAT_GRACE_MS    || 60000);  // 1 мин

// ===== helpers =====
async function ensureDir(dir) { try { await fsp.mkdir(dir, { recursive: true }); } catch {} }
function nowIso() { return new Date().toISOString(); }
function genId() { return crypto.randomBytes(16).toString('hex'); }

function todayLogName() {
  const d = new Date();
  const y = d.getUTCFullYear();
  const m = String(d.getUTCMonth() + 1).padStart(2, '0');
  const dd = String(d.getUTCDate()).padStart(2, '0');
  return `logs.${y}${m}${dd}.log`;
}

async function appendLog(text) {
  console.log(text);
  await ensureDir(LOG_DIR);
  await fsp.appendFile(path.join(LOG_DIR, todayLogName()), text + '\n', 'utf8');
}

function ok(res, extra = {}) { return res.send({ ok: true, ...extra }); }
function bad(res, code, msg) { return res.status(code).send({ ok: false, error: msg }); }

async function readLastLogTails(n = LOG_SCAN_FILES, tailBytes = LOG_TAIL_BYTES) {
  await ensureDir(LOG_DIR);
  const files = (await fsp.readdir(LOG_DIR))
    .filter(f => f.endsWith('.log'))
    .map(f => ({ f, t: fs.statSync(path.join(LOG_DIR, f)).mtimeMs }))
    .sort((a, b) => b.t - a.t)
    .slice(0, n);

  const out = [];
  for (const it of files) {
    const full = path.join(LOG_DIR, it.f);
    let buf = '';
    try { buf = await fsp.readFile(full, 'utf8'); } catch { continue; }
    if (buf.length > tailBytes) buf = buf.slice(buf.length - tailBytes);
    out.push({ name: it.f, text: buf });
  }
  return out;
}

// ===== FILE QUEUE =====
// формат .json: { id, account, chat_id, reply_text, message_id, created_at }
async function createTask({ account, chat_id, reply_text, message_id }) {
  if (!chat_id) throw new Error('chat_id required for task');
  await ensureDir(TASK_DIR);
  const id  = genId();
  const acc = (account || 'hr-main').replace(/[^a-zA-Z0-9_-]/g, '_');
  const task = {
    id,
    account: acc,
    chat_id,
    reply_text: reply_text || DEFAULT_REPLY,
    message_id: message_id || null,
    created_at: nowIso(),
  };
  const file = path.join(TASK_DIR, `${acc}__${id}.json`);
  await fsp.writeFile(file, JSON.stringify(task, null, 2), 'utf8');
  return task;
}

async function claimTask(account) {
  await ensureDir(TASK_DIR);
  let files = (await fsp.readdir(TASK_DIR)).filter(f => f.endsWith('.json'));

  // новые вперёд
  files.sort((a, b) => {
    const ta = fs.statSync(path.join(TASK_DIR, a)).mtimeMs;
    const tb = fs.statSync(path.join(TASK_DIR, b)).mtimeMs;
    return tb - ta;
  });

  // фильтр по аккаунту
  if (account) {
    const pref = `${account}__`;
    files = files.filter(f => f.startsWith(pref));
  }

  // берём только первые 7
  files = files.slice(0, 7);

  for (const f of files) {
    const full   = path.join(TASK_DIR, f);
    const taking = full.replace(/\.json$/, '.json.taking');
    try {
      await fsp.rename(full, taking); // атомарный lock
      const raw = JSON.parse(await fsp.readFile(taking, 'utf8'));
      const lockId = path.basename(taking);
      return { task: raw, lockId };
    } catch {
      // кто-то успел взять — пробуем дальше
    }
  }

  // «подметание» висяков перед тем как вернуть пусто
  const locks = (await fsp.readdir(TASK_DIR)).filter(f => f.endsWith('.json.taking'));
  const now = Date.now();
  for (const lf of locks) {
    if (account && !lf.startsWith(`${account}__`)) continue;
    const full = path.join(TASK_DIR, lf);
    let st; try { st = fs.statSync(full); } catch { continue; }
    const age = now - st.mtimeMs;
    if (age > (VISIBILITY_TIMEOUT_MS + HEARTBEAT_GRACE_MS)) {
      const back = full.replace(/\.json\.taking$/, '.json');
      try { await fsp.rename(full, back); } catch {}
      await appendLog(`[CLAIM-SWEEP] requeued stale lock ${lf}, age=${age}ms`);
    }
  }

  return null;
}

async function doneTask(lockId) {
  try { await fsp.unlink(path.join(TASK_DIR, lockId)); } catch {}
  return true;
}

async function requeueTask(lockId) {
  const from = path.join(TASK_DIR, lockId);
  const to   = from.replace(/\.json\.taking$/, '.json');
  try { await fsp.rename(from, to); } catch {}
  return true;
}

async function readTaking(lockId) {
  const full = path.join(TASK_DIR, lockId);
  return JSON.parse(await fsp.readFile(full, 'utf8'));
}

// ===== APP =====
const app = express();
app.use(express.json({ limit: '1mb' }));

// health
app.get('/', (req, res) => ok(res, { up: true }));

// debug: список файлов задач
app.get('/tasks/debug', async (req, res) => {
  try {
    await ensureDir(TASK_DIR);
    const files = (await fsp.readdir(TASK_DIR)).sort();
    res.send({ ok: true, files });
  } catch (e) { res.status(500).send({ ok: false, error: String(e) }); }
});

// прочитать .json задачи
app.get('/tasks/read', async (req, res) => {
  try {
    const file = String(req.query.file || '').trim();
    if (!file || !/^[\w.\-]+$/.test(file)) return bad(res, 400, 'bad file');
    const full = path.join(TASK_DIR, file);
    if (!fs.existsSync(full)) return bad(res, 404, 'not found');
    const content = await fsp.readFile(full, 'utf8');
    res.type('application/json').send(content);
  } catch (e) { res.status(500).send({ ok:false, error:String(e) }); }
});

// ручная постановка (для тестов)
app.post('/tasks/enqueue', async (req, res) => {
  try {
    const { account, chat_id, reply_text, message_id } = req.body || {};
    if (!chat_id) return bad(res, 400, 'chat_id required');
    const t = await createTask({ account, chat_id, reply_text, message_id });
    res.send({ ok: true, task: t });
  } catch (e) { res.status(500).send({ ok: false, error: String(e) }); }
});

// ===== WEBHOOK =====

// защита от дублей «первого системного»
const seenSystemToday = new Set(); // ключ: `${account}:${chatId}`

function looksLikeCandidateText(txt) {
  return /кандидат.*откликнулся/i.test(String(txt || ''));
}

app.post('/webhook/:account', async (req, res) => {
  const account = req.params.account || 'hr-main';

  // 1) ЛОГИРУЕМ СРАЗУ — чтобы видеть, что реально пришло
  try {
    const headersDump = JSON.stringify(req.headers || {}, null, 2);
    const bodyDump    = JSON.stringify(req.body   || {}, null, 2);
    await appendLog(
      `=== INCOMING WEBHOOK (${account}) @ ${nowIso()} ===\n` +
      `-- HEADERS --\n${headersDump}\n` +
      `-- BODY --\n${bodyDump}\n` +
      `=========================\n`
    );
  } catch {}

  // 2) МЯГКАЯ проверка секрета / подписи
  if (WEBHOOK_SECRET) {
    const hSecret    = String(req.headers['x-avito-secret'] || req.body?.secret || '');
    const hSignature = String(
      req.headers['x-avito-messenger-signature'] || // реальный заголовок Avito
      req.headers['x-avito-signature'] || ''
    );
    const pass =
      (WEBHOOK_SECRET && hSecret === WEBHOOK_SECRET) || (!!hSignature);
    if (!pass) return bad(res, 403, 'forbidden');
  }

  // 3) Постановка задач
  try {
    const payload  = req.body?.payload || {};
    const val      = payload?.value || {};
    const isSystem = val?.type === 'system';
    const txt      = String(val?.content?.text || '');
    const flowId   = String(val?.content?.flow_id || '');
    const chatId   = val?.chat_id;
    const msgId    = val?.id;

    const isJobFlow = isSystem && flowId === 'job';
    const isCandidateText = looksLikeCandidateText(txt);

    if (chatId && (isJobFlow || isCandidateText)) {
      let allowed = true;

      // На "flow_id:job" — ВСЕГДА ставим задачу (даже если это не первое system-сообщение)
      if (!isJobFlow && ONLY_FIRST_SYSTEM) {
        const key = `${account}:${chatId}`;
        if (seenSystemToday.has(key)) allowed = false;
        else seenSystemToday.add(key);
      }

      if (allowed) {
        await createTask({
          account,
          chat_id: chatId,
          reply_text: DEFAULT_REPLY,
          message_id: msgId
        });
        await appendLog(`[ENQUEUE] account=${account} chat=${chatId} reason=${isJobFlow ? 'flow:job' : 'text-match'}`);
      }
    }
  } catch { /* игнорим, вебхуку всегда отвечаем 200 */ }

  return ok(res);
});

// ===== LOGS =====
app.get('/logs', async (req, res) => {
  try {
    await ensureDir(LOG_DIR);
    const files = (await fsp.readdir(LOG_DIR))
      .filter(f => f.endsWith('.log'))
      .map(f => ({ name: f, mtime: fs.statSync(path.join(LOG_DIR, f)).mtimeMs }))
      .sort((a, b) => b.mtime - a.mtime);
    res.send({ ok: true, files });
  } catch (e) { res.status(500).send({ ok: false, error: String(e) }); }
});

app.get('/logs/read', async (req, res) => {
  try {
    const file = String(req.query.file || '').trim();
    if (!file || !/^[\w.\-]+$/.test(file)) return bad(res, 400, 'bad file');
    const full = path.join(LOG_DIR, file);
    if (!fs.existsSync(full)) return bad(res, 404, 'not found');

    const tail = Number(req.query.tail || LOG_TAIL_BYTES);
    let buf = await fsp.readFile(full, 'utf8');
    if (buf.length > tail) buf = buf.slice(buf.length - tail);
    res.type('text/plain').send(buf);
  } catch (e) { res.status(500).send({ ok: false, error: String(e) }); }
});

// /logs/has?chat=...&author=... (author опционален)
app.get('/logs/has', async (req, res) => {
  const chat   = String(req.query.chat   || '').trim();
  const author = String(req.query.author || '').trim();
  if (!chat) return bad(res, 400, 'chat required');

  const tails = await readLastLogTails(LOG_SCAN_FILES, LOG_TAIL_BYTES);
  let exists = false;
  for (const t of tails) {
    if (!t.text.includes(`"chat_id": "${chat}"`)) continue;
    if (author) {
      if (t.text.includes(`"author_id": ${author}`)) { exists = true; break; }
    } else {
      const re = new RegExp(`"chat_id"\\s*:\\s*"${chat}"[\\s\\S]{0,800}?"type"\\s*:\\s*"text"[\\s\\S]{0,400}?"author_id"\\s*:\\s*(\\d+)`, 'i');
      const m = re.exec(t.text);
      if (m && Number(m[1] || 0) > 0) { exists = true; break; }
    }
  }
  return ok(res, { exists, files: tails.map(x => x.name) });
});

// ===== TASKS API =====
function checkKey(req, res) {
  const key = String(req.query.key || req.body?.key || '').trim();
  if (!TASK_KEY || key !== TASK_KEY) { bad(res, 403, 'bad key'); return false; }
  return true;
}

app.all('/tasks/claim', async (req, res) => {
  if (!checkKey(req, res)) return;
  const account = String(req.query.account || req.body?.account || '').trim();
  const got = await claimTask(account);
  if (!got) return ok(res, { has: false });
  const { task, lockId } = got;
  return ok(res, {
    has: true,
    lockId,
    ChatId: task.chat_id,
    ReplyText: task.reply_text,
    MessageId: task.message_id || '',
    Account: task.account || ''
  });
});

app.post('/tasks/done', async (req, res) => {
  if (!checkKey(req, res)) return;
  const lock = String(req.query.lock || req.body?.lock || '').trim();
  if (!lock || !lock.endsWith('.json.taking')) return bad(res, 400, 'lock invalid');
  await doneTask(lock);
  return ok(res);
});

app.post('/tasks/requeue', async (req, res) => {
  if (!checkKey(req, res)) return;
  const lock = String(req.query.lock || req.body?.lock || '').trim();
  if (!lock || !lock.endsWith('.json.taking')) return bad(res, 400, 'lock invalid');
  await requeueTask(lock);
  return ok(res);
});

// Продлеваем «жизнь» лок-файла (для длинных операций)
app.post('/tasks/heartbeat', async (req, res) => {
  if (!checkKey(req, res)) return;
  const lock = String(req.query.lock || req.body?.lock || '').trim();
  if (!lock || !lock.endsWith('.json.taking')) return bad(res, 400, 'lock invalid');
  const full = path.join(TASK_DIR, lock);
  try {
    const now = new Date();
    await ensureDir(TASK_DIR);
    await fsp.utimes(full, now, now);
    return ok(res, { touched: true });
  } catch {
    return bad(res, 404, 'not found');
  }
});

// Закрыть только если в логах видно исходящее по этому chat_id
app.post('/tasks/doneSafe', async (req, res) => {
  if (!checkKey(req, res)) return;
  const lock = String(req.query.lock || req.body?.lock || '').trim();
  if (!lock || !lock.endsWith('.json.taking')) return bad(res, 400, 'lock invalid');

  let chat = '';
  try {
    const t = await readTaking(lock);
    chat = String(t.chat_id || '').trim();
  } catch {}

  if (!chat) return res.status(428).send({ ok: false, error: 'no chat_id in lock' });

  const tails = await readLastLogTails(LOG_SCAN_FILES, LOG_TAIL_BYTES);
  let confirmed = false;
  for (const t of tails) {
    if (!t.text.includes(`"chat_id": "${chat}"`)) continue;
    const re = new RegExp(`"chat_id"\\s*:\\s*"${chat}"[\\s\\S]{0,800}?"type"\\s*:\\s*"text"[\\s\\S]{0,400}?"author_id"\\s*:\\s*(\\d+)`, 'i');
    const m = re.exec(t.text);
    if (m && Number(m[1] || 0) > 0) { confirmed = true; break; }
  }

  if (!confirmed) {
    return res.status(428).send({ ok: false, error: 'not confirmed in logs', files: tails.map(x => x.name) });
  }

  await doneTask(lock);
  return res.status(204).send(); // No Content
});

// ===== REAPER: возвращаем протухшие локи =====
setInterval(async () => {
  try {
    await ensureDir(TASK_DIR);
    const locks = (await fsp.readdir(TASK_DIR)).filter(f => f.endsWith('.json.taking'));
    const now = Date.now();
    for (const f of locks) {
      const full = path.join(TASK_DIR, f);
      let st;
      try { st = fs.statSync(full); } catch { continue; }
      const age = now - st.mtimeMs;
      if (age > (VISIBILITY_TIMEOUT_MS + HEARTBEAT_GRACE_MS)) {
        const back = full.replace(/\.json\.taking$/, '.json');
        try { await fsp.rename(full, back); } catch {}
        await appendLog(`[REAPER] requeued stale lock ${f}, age=${age}ms`);
      }
    }
  } catch {}
}, 30000);

// ===== START =====
(async () => {
  await ensureDir(LOG_DIR);
  await ensureDir(TASK_DIR);
  console.log(`App root: ${path.resolve(__dirname)}`);
  console.log(`LOG_DIR=${path.resolve(LOG_DIR)}`);
  console.log(`TASK_DIR=${path.resolve(TASK_DIR)}`);
  console.log(`ONLY_FIRST_SYSTEM=${ONLY_FIRST_SYSTEM}`);
  console.log(`WEBHOOK_SECRET=${WEBHOOK_SECRET ? '(set)' : '(empty, disabled)'}`);
  console.log(`LOG_SCAN_FILES=${LOG_SCAN_FILES}, LOG_TAIL_BYTES=${LOG_TAIL_BYTES}`);
  console.log(`VISIBILITY_TIMEOUT_MS=${VISIBILITY_TIMEOUT_MS}, HEARTBEAT_GRACE_MS=${HEARTBEAT_GRACE_MS}`);
  app.listen(PORT, () => console.log(`Server on :${PORT}`));
})();
