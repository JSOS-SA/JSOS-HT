/**
 * الملف الوسيط — اتصال القاعدة المشترك.
 * يستورده كل من يحتاج القاعدة — يكسر التبعية الدائرية.
 */

const Database = require('better-sqlite3');
const config = require('../config');

let db = null;
let newDb = null;

// فتح اتصال مع القاعدة المشتركة — WAL للوصول المتزامن
function openDb() {
    if (!db) {
        db = new Database(config.dbPath);
        db.pragma('journal_mode = WAL');
        db.pragma('busy_timeout = 5000');
        db.pragma('foreign_keys = ON');
    }
    return db;
}

// ترحيل عمود filename ليقبل NULL — مطلوب لإدراج المراقب مباشرة
function _migrateFilenameNullable(d) {
    const info = d.prepare("PRAGMA table_info(record_2026)").all();
    const col = info.find((c) => c.name === 'filename');
    if (!col || col.notnull === 0) return; // يقبل NULL بالفعل

    const row = d.prepare(
        "SELECT sql FROM sqlite_master WHERE type='table' AND name='record_2026'"
    ).get();
    if (!row) return;

    // استبدال NOT NULL بقابل NULL وتسمية الجدول المؤقت
    const newSql = row.sql
        .replace('CREATE TABLE record_2026', 'CREATE TABLE record_2026_tmp')
        .replace(/filename\s+TEXT\s+NOT\s+NULL/i, 'filename        TEXT');

    const migrate = d.transaction(() => {
        d.exec(newSql);
        d.exec('INSERT INTO record_2026_tmp SELECT * FROM record_2026');
        d.exec('DROP TABLE record_2026');
        d.exec('ALTER TABLE record_2026_tmp RENAME TO record_2026');
        // إعادة إنشاء الفهارس
        d.exec('CREATE INDEX IF NOT EXISTS idx_record2026_date ON record_2026(folder_date)');
        d.exec('CREATE INDEX IF NOT EXISTS idx_record2026_shift ON record_2026(shift_code)');
        d.exec('CREATE INDEX IF NOT EXISTS idx_record2026_flight ON record_2026(flight_number_1)');
        d.exec('CREATE INDEX IF NOT EXISTS idx_record2026_sender ON record_2026(sender_name)');
    });
    migrate();
}

// فتح اتصال مع القاعدة الجديدة — قراءة وكتابة من المراقب
function openNewDb() {
    if (!newDb) {
        newDb = new Database(config.newDbPath);
        newDb.pragma('journal_mode = WAL');
        newDb.pragma('busy_timeout = 5000');
        newDb.pragma('foreign_keys = ON');
        // ترحيل المخطط عند أول اتصال
        _migrateFilenameNullable(newDb);
    }
    return newDb;
}

// إغلاق الاتصال عند الإنهاء الآمن
function closeDb() {
    if (db) {
        try { db.close(); } catch (_) {}
        db = null;
    }
}

function closeNewDb() {
    if (newDb) {
        try { newDb.close(); } catch (_) {}
        newDb = null;
    }
}

module.exports = { openDb, closeDb, openNewDb, closeNewDb };
