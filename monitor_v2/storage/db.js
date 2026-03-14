/**
 * طبقة القاعدة — إدراج وقراءة سجلات record_2026 في ht_sc_new.db.
 * المراقب يكتب مباشرة في القاعدة بدون المرور بالإكسل.
 * filename و row_num يبقيان NULL حتى يملأهما write_excel_from_db.py.
 */

const log = require('../utils/logger');
const { openNewDb } = require('./shared-db');

// الحد الأقصى للرحلات في صف واحد (مطابق لهيكل الجدول)
const MAX_TRIPS = 10;

// ربط أسماء الحقول العربية (من المحلل) بأسماء الأعمدة في القاعدة
const TRIP_FIELD_MAP = [
    ['رقم الرحلة', 'flight_number'],
    ['وقت الاقلاع', 'departure_time'],
    ['عدد الركاب', 'passenger_count'],
    ['الوجهة', 'destination'],
    ['الفيزا', 'visa_type'],
    ['الحملة', 'campaign_name'],
    ['الحالة', 'status'],
    ['التفويج', 'dispatch'],
    ['الكشف', 'inspection'],
];

// بناء جملة الإدراج مرة واحدة عند تحميل الوحدة
const _FIXED_COLS = ['folder_date', 'shift_code', 'sender_name', 'msg_time', 'trip_count'];
const _TRIP_COLS = [];
for (let n = 1; n <= MAX_TRIPS; n++) {
    for (const [, enName] of TRIP_FIELD_MAP) {
        _TRIP_COLS.push(enName + '_' + n);
    }
}
const _ALL_COLS = _FIXED_COLS.concat(_TRIP_COLS);
const _INSERT_SQL = 'INSERT INTO record_2026 (' + _ALL_COLS.join(', ') +
    ') VALUES (' + _ALL_COLS.map(() => '?').join(', ') + ')';

/**
 * إدراج سجل في record_2026 — يُرجع المعرّف الفريد.
 * filename و row_num يبقيان NULL (يُملآن من write_excel_from_db.py).
 */
function insertRecord(folderDate, shiftCode, senderName, msgTime, trips) {
    const d = openNewDb();
    const tripCount = Math.min(trips.length, MAX_TRIPS);

    // القيم الثابتة
    const vals = [folderDate, shiftCode, senderName || '', msgTime || '', tripCount];

    // قيم الرحلات — 9 حقول × 10 رحلات
    for (let t = 0; t < MAX_TRIPS; t++) {
        for (const [arName, enName] of TRIP_FIELD_MAP) {
            if (t < trips.length) {
                const raw = trips[t][arName] || '';
                if (enName === 'passenger_count') {
                    vals.push(parseInt(raw, 10) || 0);
                } else {
                    vals.push(raw);
                }
            } else {
                // رحلة غير موجودة — قيمة فارغة
                vals.push(enName === 'passenger_count' ? 0 : null);
            }
        }
    }

    const result = d.prepare(_INSERT_SQL).run(...vals);
    return Number(result.lastInsertRowid);
}

/**
 * تحميل مفاتيح المحتوى لسجلات تاريخ معيّن — Set.
 * المفتاح = بصمة كاملة لكل بيانات الصف وكل الرحلات (حتى 10).
 */
function loadExistingRecordKeys(folderDate) {
    const keys = new Set();
    if (!folderDate) return keys;

    try {
        const d = openNewDb();
        const rows = d.prepare(
            'SELECT * FROM record_2026 WHERE folder_date = ?'
        ).all(folderDate);

        for (const r of rows) {
            // البيانات الثابتة
            const parts = [
                r.folder_date || '',
                r.shift_code || '',
                r.sender_name || '',
                r.msg_time || '',
                String(r.trip_count || 1),
            ];

            // بيانات كل الرحلات — 9 حقول × 10 رحلات
            for (let t = 1; t <= MAX_TRIPS; t++) {
                for (const [, enName] of TRIP_FIELD_MAP) {
                    const val = r[enName + '_' + t];
                    if (enName === 'passenger_count') {
                        parts.push(String(val || 0));
                    } else {
                        parts.push(val != null ? String(val) : '');
                    }
                }
            }

            keys.add(parts.join('|'));
        }
    } catch (err) {
        log.error('خطأ تحميل مفاتيح السجلات: ' + err.message);
        log.silent('خطأ تحميل مفاتيح السجلات: ' + err.message, 'db_read_error', 'loadExistingRecordKeys');
    }
    return keys;
}

/**
 * معلومات آخر سجل مسجّل للعرض.
 * يُرجع كائن بالبيانات أو null.
 */
function getLastRecord(folderDate, shiftCode) {
    if (!folderDate || !shiftCode) return null;

    try {
        const d = openNewDb();
        const row = d.prepare(
            'SELECT * FROM record_2026 WHERE folder_date = ? AND shift_code = ? ' +
            'ORDER BY id DESC LIMIT 1'
        ).get(folderDate, shiftCode);

        if (!row) return null;

        const countRow = d.prepare(
            'SELECT COUNT(*) as cnt FROM record_2026 WHERE folder_date = ? AND shift_code = ?'
        ).get(folderDate, shiftCode);

        return {
            totalRows: countRow ? countRow.cnt : 0,
            id: row.id,
            shift: row.shift_code || '',
            flight: row.flight_number_1 || '',
            depTime: row.departure_time_1 || '',
            passengers: String(row.passenger_count_1 || ''),
            destination: row.destination_1 || '',
            sender: row.sender_name || '',
            msgTime: row.msg_time || '',
        };
    } catch (err) {
        log.error('خطأ قراءة آخر سجل: ' + err.message);
        log.silent('خطأ قراءة آخر سجل: ' + err.message, 'db_read_error', 'getLastRecord');
        return null;
    }
}

/**
 * بناء مفتاح المحتوى من البيانات المحللة — بصمة كاملة.
 * يشمل كل البيانات الثابتة + كل الرحلات (حتى 10) مع كل حقولها.
 */
function buildContentKey(folderDate, shiftCode, senderName, msgTime, trips) {
    const tripCount = Math.min(trips.length, MAX_TRIPS);
    const parts = [
        folderDate || '',
        shiftCode || '',
        senderName || '',
        msgTime || '',
        String(tripCount),
    ];

    // بيانات كل الرحلات — 9 حقول × 10 رحلات
    for (let t = 0; t < MAX_TRIPS; t++) {
        for (const [arName, enName] of TRIP_FIELD_MAP) {
            if (t < trips.length) {
                const raw = trips[t][arName] || '';
                if (enName === 'passenger_count') {
                    parts.push(String(parseInt(raw, 10) || 0));
                } else {
                    parts.push(raw);
                }
            } else {
                // رحلة غير موجودة — قيمة فارغة
                parts.push(enName === 'passenger_count' ? '0' : '');
            }
        }
    }

    return parts.join('|');
}

module.exports = {
    insertRecord,
    loadExistingRecordKeys,
    getLastRecord,
    buildContentKey,
};
