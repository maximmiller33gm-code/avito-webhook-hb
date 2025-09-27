// server.js — ESM (в package.json должно быть: { "type": "module" })
import express from 'express';
import fs from 'fs';
import { promises as fsp } from 'fs';
import path from 'path';
import crypto from 'crypto';
import process from 'process';
import { fileURLToPath } from 'url';

// __dirname в ESM
const __filename = fileURLToPath(import.meta.url);
const __dirname  = path.dirname(__filename);

// ===== ENV / CONFIG =====
const PORT              = Number(process.env.PORT || 3000);
const TASK_KEY          = process.env.TASK_KEY || 'dev-task-key';
const LOG_DIR           = process.env.LOG_DIR  || '/mnt/data/logs';
const TASK_DIR          = process.env.TASK_DIR || '/mnt/data/tasks';
const DEFAULT_REPLY     = process.env.DEFAULT_REPLY || 'Здравствуйте!';
const ONLY_FIRST_SYSTEM = String(process.env.ONLY_FIRST_SYSTEM || 'true').toLowerCase() === 'true';
const WEBHOOK_SECRET    = process.env.WEBHOOK_SECRET || '';
const LOG_TAIL_BYTES    = Number(process.env.LOG_TAIL_BYTES || 512 * 1024); // 512 КБ

// Надёжность обработки
const VISIBILITY_TIMEOUT_MS = Number(process.env.VISIBILITY_TIMEOUT_MS || 180000); // 3 мин
const HEARTBEAT_GRACE_MS    = Number(process.env.HEARTBEAT_GRACE_MS    || 60000);  // +1 мин

// ===== helpers =====
async function ensureDir(dir) { try { await fsp.mkdir(dir, { recursive: true }); } catch {} }
function nowIso() { return new Date().toISOString(); }
function genId() { return crypto.randomBytes(16).toString('hex'); }

function todayLogName() {
  const d = new Date();
  const y = d.getUTCFullYear();
  const m = String(d.getUTCMonth()+1).padStart(2, '0');
  const dd = String(d.getUTCDate()).padStart(2, '0');
  return `logs.${y}${m}${dd}.log`;
}

// лог и в файл, и в консоль
async function appendLog(text) {
  console.log(text);
  await ensureDir(LOG_DIR);
  const file = path.join(LOG_DIR, todayLogName());
  await fsp.appendFile(file, text + '\n', 'utf8');
  return file;
}

function ok(res, extra = {}) { return res.send({ ok: true, ...extra }); }
function bad(res, code, msg) { return res.status(code).send({ ok: false, error: msg }); }

// утилиты чтения последних N логов (по mtime, хвост файла)
async function readLastLogTails(n = 2, tailBytes = LOG_TAIL_BYTES) {
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
// формат задачи (json): { id, account, chat_id, reply_text, message_id, created_at }

async function createTask({ account, chat_id, reply_text, message_id }) {
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

// Claim: берём из последних по mtime (первые 3), опционально фильтруем по account
async function claimTask(account) {
  await ensureDir(TASK_DIR);
  let files = (await fsp.readdir(TASK_DIR)).filter(f => f.endsWith('.json'));

  // сортировка по дате изменения: новые вперёд
  files.sort((a, b) => {
    const ta = fs.statSync(path.join(TASK_DIR, a)).mtimeMs;
    const tb = fs.statSync(path.join(TASK_DIR, b)).mtimeMs;
    return tb - ta;
  });

  if (account) {
    const pref = `${account}__`;
    files = files.filter(f => f.startsWith(pref));
  }

  files = files.slice(0, 3);

  for (const f of files) {
    const full   = path.join(TASK_DIR, f);
    const taking = full.replace(/\.json$/, '.json.taking');
    try {
      await fsp.rename(full, taking); // атомарный lock
      const raw = JSON.parse(await fsp.readFile(taking, 'utf8'));
      const lockId = path.basename(taking);
      return { task: raw, lockId };
    } catch {
      // файл мог забрать другой процесс — продолжаем
    }
  }
  return null;
}

async function doneTask(lockId) {
  const file = path.join(TASK_DIR, lockId);
  try { await fsp.unlink(file); } catch {}
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
  const raw  = JSON.parse(await fsp.readFile(full, 'utf8'));
  return raw || {};
}

// ===== APP =====
const app = express();
app.use(express.json({ limit: '1mb' }));

// health
app.get('/', (req, res) => ok(res, { up: true }));

// debug: список task-файлов
app.get('/tasks/debug', async (req, res) => {
  try {
    await ensureDir(TASK_DIR);
    const files = (await fsp.readdir(TASK_DIR)).sort();
    res.send({ ok: true, files });
  } catch (e) {
    res.status(500).send({ ok: false, error: String(e) });
  }
});

// ручная постановка (для тестов)
app.post('/tasks/enqueue', async (req, res) => {
  try {
    const { account, chat_id, reply_text, message_id } = req.body || {};
    if (!chat_id) return bad(res, 400, 'chat_id required');
    const t = await createTask({ account, chat_id, reply_text, message_id });
    res.send({ ok: true, task: t });
  } catch (e) {
    res.status(500).send({ ok: false, error: String(e) });
  }
});

// ===== ВЕБХУК АВИТО =====

// простая in-memory защита "только первое системное по чату"
const seenSystemToday = new Set(); // ключ: `${account}:${chatId}`

// Примитивный детектор «системного отклика»
function looksLikeCandidateText(txt) {
  return /кандидат|отклик|откликнулся/i.test(String(txt || ''));
}

app.post('/webhook/:account', async (req, res) => {
  const account = req.params.account || 'hr-main';

  // (опц.) проверка секрета
  if (WEBHOOK_SECRET) {
    const headerSecret = req.headers['x-avito-secret'];
    const bodySecret   = req.body && req.body.secret;
    if (String(headerSecret || bodySecret || '') !== String(WEBHOOK_SECRET)) {
      return bad(res, 403, 'forbidden');
    }
  }

  // лог RAW
  const pretty = JSON.stringify(req.body || {}, null, 2);
  const header = `=== RAW AVITO WEBHOOK (${account}) @ ${nowIso()} ===\n`;
  const footer = `\n=========================\n`;
  await appendLog(header + pretty + footer);

  // простая логика создания задачи на отклик
  try {
    const payload = req.body?.payload || {};
    const val     = payload?.value || {};
    const isSystem = val?.type === 'system';
    const txt     = String(val?.content?.text || '');
    const chatId  = val?.chat_id;
    const msgId   = val?.id;

    if (isSystem && looksLikeCandidateText(txt) && chatId) {
      let allowed = true;

      if (ONLY_FIRST_SYSTEM) {
        const key = `${account}:${chatId}`;
        if (seenSystemToday.has(key)) {
          allowed = false;
        } else {
          seenSystemToday.add(key);
        }
      }

      if (allowed) {
        await createTask({
          account,
          chat_id: chatId,
          reply_text: DEFAULT_REPLY,
          message_id: msgId
        });
      }
    }
  } catch {/* игнорим, вебхуку всегда отвечаем 200 */}

  return ok(res);
});

// ===== Проверка логов =====

// /logs — список файлов
app.get('/logs', async (req, res) => {
  try {
    await ensureDir(LOG_DIR);
    const files = (await fsp.readdir(LOG_DIR))
      .filter(f => f.endsWith('.log'))
      .map(f => ({ name: f, mtime: fs.statSync(path.join(LOG_DIR, f)).mtimeMs }))
      .sort((a, b) => b.mtime - a.mtime);

    res.send({ ok: true, files });
  } catch (e) {
    res.status(500).send({ ok: false, error: String(e) });
  }
});

// /logs/read — прочитать лог (полностью или хвост)
app.get('/logs/read', async (req, res) => {
  try {
    const file = String(req.query.file || '').trim();
    if (!file || !/^[\w.\-]+$/.test(file)) return bad(res, 400, 'bad file');
    const full = path.join(LOG_DIR, file);
    const exists = fs.existsSync(full);
    if (!exists) return bad(res, 404, 'not found');

    const tail = Number(req.query.tail || 300000); // 300 КБ по умолчанию
    let buf = await fsp.readFile(full, 'utf8');
    if (buf.length > tail) buf = buf.slice(buf.length - tail);
    res.type('text/plain').send(buf);
  } catch (e) {
    res.status(500).send({ ok: false, error: String(e) });
  }
});

// /logs/has — есть ли в последних логах chat_id + author_id (author опционален)
app.get('/logs/has', async (req, res) => {
  const chat   = String(req.query.chat   || '').trim();
  const author = String(req.query.author || '').trim(); // можно не передавать
  if (!chat) return bad(res, 400, 'chat required');

  const tails = await readLastLogTails(2, LOG_TAIL_BYTES);
  let exists = false;
  for (const t of tails) {
    if (!t.text.includes(`"chat_id": "${chat}"`)) continue;

    if (author) {
      if (t.text.includes(`"author_id": ${author}`)) { exists = true; break; }
    } else {
      // без author: считаем подтверждением любое текстовое сообщение c author_id != 0
      const re = new RegExp(`"chat_id"\\s*:\\s*"${chat}"[\\s\\S]{0,800}?"type"\\s*:\\s*"text"[\\s\\S]{0,400}?"author_id"\\s*:\\s*(\\d+)`, 'i');
      const m = re.exec(t.text);
      if (m && Number(m[1] || 0) > 0) { exists = true; break; }
    }
  }
  return ok(res, { exists, files: tails.map(x => x.name) });
});

// ===== задачи: claim / done / requeue / heartbeat / doneSafe =====
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

// поддерживающий удар (обновить mtime лок-файла)
app.post('/tasks/heartbeat', async (req, res) => {
  if (!checkKey(req, res)) return;
  const lock = String(req.query.lock || req.body?.lock || '').trim();
  if (!lock || !lock.endsWith('.json.taking')) return bad(res, 400, 'lock invalid');

  const full = path.join(TASK_DIR, lock);
  try {
    const now = Date.now();
    await ensureDir(TASK_DIR);
    await fsp.utimes(full, now/1000, now/1000); // touch mtime
    return ok(res, { touched: true });
  } catch {
    return bad(res, 404, 'not found');
  }
});

// doneSafe: сервер сам проверяет логи (2 последних) и закрывает только при наличии подтверждения
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

  const tails = await readLastLogTails(2, LOG_TAIL_BYTES);
  let confirmed = false;
  for (const t of tails) {
    if (!t.text.includes(`"chat_id": "${chat}"`)) continue;
    // подтверждаем любое текстовое исходящее (author_id != 0)
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

// ===== Фоновый «реапер» старых локов =====
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

// ===== start =====
(async () => {
  await ensureDir(LOG_DIR);
  await ensureDir(TASK_DIR);
  console.log(`App root: ${path.resolve(__dirname)}`);
  console.log(`LOG_DIR=${path.resolve(LOG_DIR)}`);
  console.log(`TASK_DIR=${path.resolve(TASK_DIR)}`);
  console.log(`ONLY_FIRST_SYSTEM=${ONLY_FIRST_SYSTEM}`);
  console.log(`LOG_TAIL_BYTES=${LOG_TAIL_BYTES}`);
  console.log(`VISIBILITY_TIMEOUT_MS=${VISIBILITY_TIMEOUT_MS}`);
  console.log(`HEARTBEAT_GRACE_MS=${HEARTBEAT_GRACE_MS}`);
  app.listen(PORT, () => console.log(`Server on :${PORT}`));
})();
