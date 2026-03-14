const path = require('path');
const Database = require('better-sqlite3');

const C = {
    reset: '\x1b[0m',
    green: '\x1b[32m',
    yellow: '\x1b[33m',
    red: '\x1b[31m',
    cyan: '\x1b[36m',
    white: '\x1b[37m',
    dim: '\x1b[2m',
    bold: '\x1b[1m',
};

// مسار قاعدة السجلات — نفس قاعدة النظام البايثوني
const LOGS_DB_PATH = path.join(__dirname, '..', '..', 'logs', 'ht_sc_logs.db');

// اتصال كسول بقاعدة السجلات — يُفتح عند أول استخدام فقط
let _logsDb = null;
function getLogsDb() {
    if (!_logsDb) {
        try {
            _logsDb = new Database(LOGS_DB_PATH);
            _logsDb.pragma('journal_mode = WAL');
            _logsDb.pragma('synchronous = NORMAL');
        } catch (_) {
            // إذا فشل فتح القاعدة نستمر بدون تسجيل — لا نوقف المراقب
            _logsDb = null;
        }
    }
    return _logsDb;
}

// إغلاق قاعدة السجلات عند إيقاف المراقب
function closeLogsDb() {
    if (_logsDb) {
        try { _logsDb.close(); } catch (_) {}
        _logsDb = null;
    }
}

// طابع زمني بصيغة النظام البايثوني
function isoTimestamp() {
    const d = new Date();
    const pad = (n, l = 2) => String(n).padStart(l, '0');
    return d.getFullYear() + '-' + pad(d.getMonth() + 1) + '-' + pad(d.getDate()) +
        ' ' + pad(d.getHours()) + ':' + pad(d.getMinutes()) + ':' +
        pad(d.getSeconds()) + '.' + pad(d.getMilliseconds(), 3);
}

// تسجيل حدث نظام في القاعدة
function logSystemEvent(message, eventType = '') {
    const db = getLogsDb();
    if (!db) return;
    try {
        db.prepare(
            'INSERT INTO system_events (timestamp, level, script_name, function_name, message, event_type) VALUES (?, ?, ?, ?, ?, ?)'
        ).run(isoTimestamp(), 'INFO', 'monitor', '', message, eventType);
    } catch (_) {}
}

// تسجيل خطأ في جدول errors
function logErrorToDb(message, errorType = '', context = '') {
    const db = getLogsDb();
    if (!db) return;
    try {
        db.prepare(
            'INSERT INTO errors (timestamp, level, script_name, function_name, message, error_type, context) VALUES (?, ?, ?, ?, ?, ?, ?)'
        ).run(isoTimestamp(), 'ERROR', 'monitor', '', message, errorType, context);
    } catch (_) {}
}

// تسجيل تحذير في system_events
function logWarningToDb(message, context = '') {
    const db = getLogsDb();
    if (!db) return;
    try {
        db.prepare(
            'INSERT INTO system_events (timestamp, level, script_name, function_name, message, event_type) VALUES (?, ?, ?, ?, ?, ?)'
        ).run(isoTimestamp(), 'WARNING', 'monitor', '', message, context);
    } catch (_) {}
}

function timestamp() {
    return new Date().toLocaleTimeString('en-GB', { hour12: false });
}

function rtl(text) {
    return text;
}

// طباعة سطر ملون مع محاذاة يمين
function printRTL(plainLen, colored) {
    const cols = process.stdout.columns || 80;
    const pad = Math.max(0, cols - plainLen);
    console.log(' '.repeat(pad) + colored);
}

module.exports = {
    info: (msg) => {
        const ts = '[' + timestamp() + ']';
        const m = rtl(msg);
        printRTL(ts.length + 1 + [...m].length, C.cyan + ts + C.green + ' ' + m + C.reset);
        // تسجيل أحداث التشغيل والاتصال في القاعدة
        if (msg.includes('متصل') || msg.includes('تشغيل') || msg.includes('إغلاق') || msg.includes('تهيئة')) {
            logSystemEvent(msg, 'lifecycle');
        }
    },
    warn: (msg) => {
        const ts = '[' + timestamp() + ']';
        const m = rtl(msg);
        printRTL(ts.length + 1 + [...m].length, C.cyan + ts + C.yellow + ' ' + m + C.reset);
        logWarningToDb(msg, 'warning');
    },
    error: (msg) => {
        const ts = '[' + timestamp() + ']';
        const m = rtl(msg);
        printRTL(ts.length + 1 + [...m].length, C.cyan + ts + C.red + ' ' + m + C.reset);
        logErrorToDb(msg, 'runtime_error', '');
    },
    // تسجيل خطأ صامت — لا يُطبع على الشاشة لكن يُسجّل في القاعدة
    silent: (msg, errorType = 'silent_error', context = '') => {
        logErrorToDb(msg, errorType, context);
    },
    // تسجيل حدث نظام مباشرة
    system: (msg, eventType = '') => {
        logSystemEvent(msg, eventType);
    },
    msg: (sender, text, msgTime, msgDate, messageId, excelFile) => {
        const cols = process.stdout.columns || 80;
        const sep = '\u2500'.repeat(Math.min(24, cols));
        const padSep = Math.max(0, cols - 24);
        console.log(' '.repeat(padSep) + C.dim + sep + C.reset);

        const idPart = messageId ? ' #' + messageId : '';
        const datePart = msgDate ? ' [' + msgDate + ']' : '';
        const ts = '[' + (msgTime || timestamp()) + ']' + datePart + idPart;
        const s = rtl(sender);
        const plainLen = ts.length + 1 + [...s].length;
        printRTL(plainLen, C.cyan + ts + C.bold + ' ' + s + C.reset);

        const lines = text.split('\n');
        for (const line of lines) {
            if (!line.trim()) continue;
            const l = rtl(line);
            printRTL([...l].length, C.white + l + C.reset);
        }

        // عرض اسم ملف الإكسل
        if (excelFile) {
            const fileLine = '\u2192 ' + excelFile;
            printRTL([...fileLine].length, C.dim + fileLine + C.reset);
        }
    },
    closeLogsDb,
};
