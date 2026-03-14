/**
 * كتابة الإكسل باستخدام exceljs بدل xlsx.
 * يدعم التدفق للملفات الكبيرة.
 */

const fs = require('fs');
const path = require('path');
const ExcelJS = require('exceljs');
const log = require('../utils/logger');
const { getShiftLetter, getShiftCode, getShiftDate } = require('../core/shift');
const { parseMultiTrips, isStuckMessage } = require('../core/parser');
const { loadFingerprintsFromExcelRows } = require('../core/fingerprint');

const RECORD_DIR = path.join(__dirname, '..', '..', 'Record_2026');

// أسماء حقول الرحلة التسعة
const TRIP_FIELD_NAMES = [
    'رقم الرحلة', 'وقت الاقلاع', 'عدد الركاب', 'الوجهة',
    'الفيزا', 'الحملة', 'الحالة', 'التفويج', 'الكشف',
];

// بناء مسار المجلد اليومي
function getDayDir(date) {
    const parts = date.split('-');
    const month = parts[1] + '-' + parts[2];
    const dayPadded = parts[0].padStart(2, '0') + '-' + parts[1] + '-' + parts[2];
    return path.join(RECORD_DIR, month, dayPadded);
}

// مسار ملف الإكسل حسب الوقت والتاريخ
function getExcelPath(timeStr, msgDate) {
    const letter = getShiftLetter(timeStr);
    const date = getShiftDate(timeStr, msgDate);
    const dayDir = getDayDir(date);
    return path.join(dayDir, date + '-Record-' + letter + '.xlsx');
}

// مسار ملف المتعثرة
function getStuckExcelPath(timeStr, msgDate) {
    const letter = getShiftLetter(timeStr);
    const date = getShiftDate(timeStr, msgDate);
    const dayDir = getDayDir(date);
    return path.join(dayDir, date + '-Record-' + letter + '-Stuck.xlsx');
}

// عدد الأعمدة الثابتة (م + رمز النوبة + اسم المرسل + وقت وصول الرسالة)
const FIXED_COLS = 4;

// بناء صف العناوين حسب عدد الرحلات
function buildHeaderRow(maxTrips) {
    const header = ['م', 'رمز النوبة', 'اسم المرسل', 'وقت وصول الرسالة'];
    for (let t = 0; t < maxTrips; t++) {
        const suffix = maxTrips > 1 ? ' ' + (t + 1) : '';
        for (const name of TRIP_FIELD_NAMES) {
            header.push(name + suffix);
        }
    }
    return header;
}

// قراءة ملف إكسل موجود — يُرجع مصفوفة صفوف
async function readExcelFile(filePath) {
    if (!fs.existsSync(filePath)) return null;
    try {
        const wb = new ExcelJS.Workbook();
        await wb.xlsx.readFile(filePath);
        const ws = wb.worksheets[0];
        if (!ws) return null;
        const rows = [];
        ws.eachRow((row) => {
            rows.push(row.values.slice(1)); // exceljs يبدأ من 1 — نحذف الفراغ الأول
        });
        return rows;
    } catch (_) {
        return null;
    }
}

// كتابة صفوف إلى ملف إكسل (إنشاء أو إضافة)
async function writeToExcel(filePath, trips, shiftCode, sender, msgTime, messageId) {
    const fileDir = path.dirname(filePath);
    if (!fs.existsSync(fileDir)) {
        fs.mkdirSync(fileDir, { recursive: true });
    }

    let wb = new ExcelJS.Workbook();
    let ws;
    let currentMaxTrips = 1;
    let hasIdColumn = false;

    // قراءة الملف الموجود
    if (fs.existsSync(filePath)) {
        try {
            await wb.xlsx.readFile(filePath);
            ws = wb.worksheets[0];
            if (ws && ws.rowCount > 0) {
                const headerRow = ws.getRow(1);
                const firstHeader = String(headerRow.getCell(1).value || '');
                hasIdColumn = (firstHeader === 'م');
                const headerLen = headerRow.cellCount;
                const fixedCols = hasIdColumn ? FIXED_COLS : 3;
                currentMaxTrips = Math.max(1, Math.floor((headerLen - fixedCols) / 9));
            }
        } catch (_) {
            // ملف تالف — إنشاء جديد
            wb = new ExcelJS.Workbook();
            ws = null;
        }
    }

    // ترحيل ملف قديم بدون عمود المعرّف — إضافة العمود
    if (ws && !hasIdColumn && ws.rowCount > 0) {
        for (let r = ws.rowCount; r >= 1; r--) {
            const row = ws.getRow(r);
            const cellCount = row.cellCount;
            for (let c = cellCount; c >= 1; c--) {
                row.getCell(c + 1).value = row.getCell(c).value;
            }
            row.getCell(1).value = r === 1 ? 'م' : '';
            row.commit();
        }
        hasIdColumn = true;
    }

    const newMaxTrips = Math.max(currentMaxTrips, trips.length);

    if (!ws) {
        ws = wb.addWorksheet('رسائل');
        const header = buildHeaderRow(newMaxTrips);
        ws.addRow(header);
        // عرض الأعمدة
        ws.columns = header.map(() => ({ width: 18 }));
    } else if (newMaxTrips > currentMaxTrips) {
        // تحديث العناوين إذا زاد عدد الرحلات
        const newHeader = buildHeaderRow(newMaxTrips);
        const headerRow = ws.getRow(1);
        newHeader.forEach((val, i) => { headerRow.getCell(i + 1).value = val; });
        headerRow.commit();
        ws.columns = newHeader.map(() => ({ width: 18 }));
    }

    // بناء صف البيانات — المعرّف أول عمود
    const row = [messageId || '', shiftCode, sender || '', msgTime || ''];
    for (const fields of trips) {
        for (const name of TRIP_FIELD_NAMES) {
            row.push(fields[name] || '');
        }
    }
    // ملء الفراغات للرحلات الناقصة
    const totalTripCols = newMaxTrips * TRIP_FIELD_NAMES.length;
    const actualTripCols = trips.length * TRIP_FIELD_NAMES.length;
    for (let i = actualTripCols; i < totalTripCols; i++) {
        row.push('');
    }

    ws.addRow(row);

    // كتابة آمنة — ملف مؤقت ثم استبدال
    const tmpFile = filePath + '.tmp';
    await wb.xlsx.writeFile(tmpFile);
    fs.renameSync(tmpFile, filePath);
}

// تسجيل رسالة في الإكسل — يُرجع اسم الملف الفعلي أو null
async function logToExcel(sender, text, msgTime, msgDate, messageId) {
    try {
        const trips = parseMultiTrips(text);
        if (trips.length === 0) return null;

        const letter = getShiftLetter(msgTime);
        if (!letter) return null;

        const shiftCode = getShiftCode(msgTime);
        const filePath = getExcelPath(msgTime, msgDate);

        await writeToExcel(filePath, trips, shiftCode, sender, msgTime, messageId);

        // تسجيل إضافي في ملف المتعثرة
        const hasStuck = trips.some((f) => isStuckMessage(f));
        if (hasStuck) {
            const stuckPath = getStuckExcelPath(msgTime, msgDate);
            await writeToExcel(stuckPath, trips, shiftCode, sender, msgTime, messageId);
        }

        return path.basename(filePath);
    } catch (err) {
        log.error('خطأ تسجيل Excel: ' + err.message);
        return null;
    }
}

// قراءة بصمات من ملف الإكسل الحالي
async function loadExistingFingerprints(timeStr, msgDate) {
    const fingerprints = new Set();
    if (!timeStr) return fingerprints;
    const letter = getShiftLetter(timeStr);
    if (!letter) return fingerprints;

    const shiftDate = getShiftDate(timeStr, msgDate);
    const filePath = getExcelPath(timeStr, msgDate);
    const rows = await readExcelFile(filePath);
    if (!rows) return fingerprints;

    return loadFingerprintsFromExcelRows(rows, shiftDate, letter);
}

// معلومات آخر صف مسجل
async function getLastRecordInfo(timeStr, msgDate) {
    if (!timeStr) return null;
    const letter = getShiftLetter(timeStr);
    if (!letter) return null;

    const filePath = getExcelPath(timeStr, msgDate);
    const rows = await readExcelFile(filePath);
    if (!rows || rows.length < 2) return null;

    const last = rows[rows.length - 1];
    // كشف وجود عمود المعرّف — إذا أول عنوان 'م' الأعمدة تبدأ من 1
    const firstHeader = String(rows[0][0] || '');
    const off = (firstHeader === 'م') ? 1 : 0;
    return {
        totalRows: rows.length - 1,
        messageId: off ? String(last[0] || '') : '',
        shift: String(last[off] || ''),
        sender: String(last[off + 1] || ''),
        msgTime: String(last[off + 2] || ''),
        flight: String(last[off + 3] || ''),
        depTime: String(last[off + 4] || ''),
        passengers: String(last[off + 5] || ''),
        destination: String(last[off + 6] || ''),
        filePath: filePath,
    };
}

// قراءة معرّفات الرسائل من عمود المعرّف في الإكسل — يُرجع Set
async function loadExistingIds(timeStr, msgDate) {
    const ids = new Set();
    if (!timeStr) return ids;
    const letter = getShiftLetter(timeStr);
    if (!letter) return ids;
    const filePath = getExcelPath(timeStr, msgDate);
    const rows = await readExcelFile(filePath);
    if (!rows || rows.length < 2) return ids;
    // التحقق من وجود عمود المعرّف
    const firstHeader = String(rows[0][0] || '');
    if (firstHeader !== 'م') return ids;
    for (let i = 1; i < rows.length; i++) {
        const val = rows[i] && rows[i][0];
        if (val) ids.add(Number(val));
    }
    return ids;
}

module.exports = {
    logToExcel,
    loadExistingFingerprints,
    loadExistingIds,
    getLastRecordInfo,
};
