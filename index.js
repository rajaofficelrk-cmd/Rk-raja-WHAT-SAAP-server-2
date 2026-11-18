const makeWASocket = require('@whiskeysockets/baileys').default;
const {
  useMultiFileAuthState,
  DisconnectReason,
  fetchLatestWaWebVersion,
  Browsers,
  delay,
} = require('@whiskeysockets/baileys');
const { Boom } = require('@hapi/boom');
const express = require('express');
const pino = require('pino');
const fs = require('fs');
const multer = require('multer');

const app = express();
app.use(express.json());
app.use(express.urlencoded({ extended: true }));

const PORT = process.env.PORT || 25029;
const HOST = '0.0.0.0';

const MIN_DELAY_SECONDS = 3;
const DEFAULT_DELAY_SECONDS = 10;
const SEND_RETRY = 2; // Retry badha di hai taaki network drop hone par message fail na ho
const RETRY_WAIT_MS = 3000;

let bulkState = {
  running: false,
  stopFlag: false,
  sent: 0,
  failed: 0,
  total: 0,
  remaining: 0,
  cycle: 0,
  targets: [],
  messages: [],
  delayMs: DEFAULT_DELAY_SECONDS * 1000,
  logs: [],
  logId: 0,
  workerAlive: false,
  sessionSock: null
};

function pushLog(type, msg) {
  bulkState.logs.push({ id: ++bulkState.logId, ts: Date.now(), type, msg });
  if (bulkState.logs.length > 1000) bulkState.logs.splice(0, bulkState.logs.length - 1000);
  const icons = { ok: '✅', err: '❌', warn: '⚠️', info: 'ℹ️' };
  console.log(`${icons[type] || '•'} ${msg}`);
}

// 🛡️ CRITICAL FIX: Server ko auto-crash hone se bachane ke liye
process.on('uncaughtException', (err) => {
  pushLog('err', 'Uncaught Exception (Handled): ' + err.message);
  console.error(err);
});

process.on('unhandledRejection', (reason, promise) => {
  pushLog('err', 'Unhandled Rejection (Handled): ' + (reason ? reason.message : reason));
});

// Helper: Parse directly pasted raw numbers
function parseRawNumbers(rawString) {
  if (!rawString || typeof rawString !== 'string') return [];
  const rawList = rawString.split(/[\r\n,\s]+/);
  const seen = new Set();
  const targets = [];

  for (let num of rawList) {
    const trimmed = num.trim();
    if (!trimmed) continue;

    let jid;
    if (trimmed.includes('@g.us') || trimmed.includes('@s.whatsapp.net')) {
      jid = trimmed;
    } else {
      let cleaned = trimmed.replace(/\D/g, '');
      if (cleaned.length < 5) continue;
      jid = `${cleaned}@s.whatsapp.net`;
    }

    if (!seen.has(jid)) {
      seen.add(jid);
      targets.push({
        jid: jid,
        label: trimmed
      });
    }
  }
  return targets;
}

async function safeSend(sockInstance, jid, text) {
  for (let attempt = 0; attempt <= SEND_RETRY; attempt++) {
    try {
      if (!sockInstance) throw new Error('socket not ready');
      await sockInstance.sendMessage(jid, { text });
      return { ok: true };
    } catch (e) {
      if (attempt < SEND_RETRY) {
        pushLog('warn', `Retry ${attempt + 1}/${SEND_RETRY} → ${jid}: ${e.message}`);
        await delay(RETRY_WAIT_MS);
      } else {
        return { ok: false, error: e.message };
      }
    }
  }
  return { ok: false, error: 'unknown error' };
}

async function runWorker() {
  if (bulkState.workerAlive) return;
  bulkState.workerAlive = true;
  pushLog('info', 'Worker started — Sending messages continuously...');

  try {
    while (!bulkState.stopFlag) {
      // Agar stop flag true ho gaya toh turant loop tod do
      if (bulkState.stopFlag) break;

      if (!bulkState.sessionSock) {
        pushLog('warn', 'Session socket disconnected, waiting for connection...');
        await delay(5000);
        continue;
      }

      for (let mi = 0; mi < bulkState.messages.length && !bulkState.stopFlag; mi++) {
        const msg = bulkState.messages[mi];

        for (let ti = 0; ti < bulkState.targets.length && !bulkState.stopFlag; ti++) {
          const t = bulkState.targets[ti];

          try {
            const res = await safeSend(bulkState.sessionSock, t.jid, msg);
            if (res.ok) {
              bulkState.sent++;
              pushLog('ok', `Cycle ${bulkState.cycle + 1} | Msg ${mi + 1}/${bulkState.messages.length} → ${t.label}`);
            } else {
              bulkState.failed++;
              pushLog('err', `Msg ${mi + 1} → ${t.label}: ${res.error}`);
            }
          } catch (e) {
            bulkState.failed++;
            pushLog('err', `Send exception → ${t.label}: ${e.message}`);
          }

          bulkState.remaining = bulkState.messages.length - (mi + 1);
          
          // Delay between messages (User jab tak stop na kare tab tak delay maintain karega)
          if (!bulkState.stopFlag) {
            await delay(bulkState.delayMs);
          }
        }
      }

      // Agar stop flag nahi laga hai, toh cycle complete hone par automatically next cycle shuru karega
      if (!bulkState.stopFlag) {
        bulkState.cycle++;
        bulkState.remaining = bulkState.messages.length;
        pushLog('info', `Cycle ${bulkState.cycle} completed successfully — restarting next cycle...`);
      }
    }
  } catch (loopErr) {
    pushLog('err', 'Worker loop crashed: ' + loopErr.message);
  } finally {
    // Yeh block tabhi chalega jab user khud /api/stop-bulk call karega
    bulkState.running = false;
    bulkState.stopFlag = false;
    bulkState.workerAlive = false;
    pushLog('info', `Task stopped safely by user — Sent: ${bulkState.sent}, Failed: ${bulkState.failed}`);
  }
}

// API Endpoint to start bulk messaging
app.post('/api/start-bulk', async (req, res) => {
  try {
    const { rawNumbers, messagesList, delaySeconds } = req.body;

    if (bulkState.running) {
      return res.status(400).json({ success: false, error: 'A bulk task is already running.' });
    }

    let targets = [];
    if (rawNumbers && rawNumbers.trim().length > 0) {
      targets = parseRawNumbers(rawNumbers);
      pushLog('info', `Parsed ${targets.length} numbers from direct paste input.`);
    }

    if (targets.length === 0) {
      return res.status(400).json({ success: false, error: 'No valid targets provided.' });
    }

    if (!messagesList || !Array.isArray(messagesList) || messagesList.length === 0) {
      return res.status(400).json({ success: false, error: 'No messages provided.' });
    }

    // Reset and configure state
    bulkState.targets = targets;
    bulkState.messages = messagesList;
    bulkState.delayMs = Math.max(MIN_DELAY_SECONDS, Number(delaySeconds) || DEFAULT_DELAY_SECONDS) * 1000;
    bulkState.sent = 0;
    bulkState.failed = 0;
    bulkState.cycle = 0;
    bulkState.total = targets.length * messagesList.length;
    bulkState.remaining = messagesList.length;
    bulkState.running = true;
    bulkState.stopFlag = false;
    bulkState.workerAlive = false; // Reset worker flag

    // Run background worker
    runWorker();

    return res.json({ success: true, totalTargets: targets.length });
  } catch (e) {
    return res.status(500).json({ success: false, error: e.message });
  }
});

// Endpoint to stop bulk task manually
app.post('/api/stop-bulk', (req, res) => {
  if (!bulkState.running) {
    return res.json({ success: false, message: 'No active task running.' });
  }
  bulkState.stopFlag = true;
  pushLog('warn', 'Stop signal received. Task will stop shortly.');
  return res.json({ success: true, message: 'Stopping task...' });
});

// Endpoint to check status / logs
app.get('/api/status', (req, res) => {
  res.json({
    running: bulkState.running,
    sent: bulkState.sent,
    failed: bulkState.failed,
    cycle: bulkState.cycle,
    remaining: bulkState.remaining,
    logs: bulkState.logs.slice(-50)
  });
});

app.listen(PORT, HOST, () => {
  console.log(`Server is running on http://${HOST}:${PORT}`);
});
