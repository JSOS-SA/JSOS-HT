/**
 * تسجيل المخالفات في القاعدة والإكسل.
 * يستورد الاتصال من shared-db.js — لا تبعية دائرية.
 */

const fs = require('fs');
const path = require('path');
const ExcelJS = require('exceljs');
const log = require('../utils/logger');
const { openDb, openNewDb } = require('./shared-db');
const { getShiftLetter, getShiftDate } = require('../core/shift');
const { checkViolations } = require('../core/validator');

// مجلد تسجيل المخالفات في إكسل
const ERROR_RECORD_DIR = path.join(__dirname, '..', '..', 'Error_Record');

// مجلد الحالة
const STATE_DIR = path.join(__dirname, '..', 'state');

// أعمدة ملف المخالفات
const VIOLATION_COLUMNS = ['التاريخ', 'النوبة', 'اسم الموظف', 'وقت المخالفة', 'تفاصيل المخالفة', 'العداد'];

// عدد مخالفات موظف من القاعدة
function getEmployeeCount(sender) {
    try {
        const d = openDb();
        const row = d.prepare(
            'SELECT COUNT(*) as cnt FROM violations WHERE sender_name = ?'
        ).get(sender);
        return row ? row.cnt : 0;
    } catch (err) {
        log.silent('خطأ عدّ مخالفات الموظف: ' + (err.message || ''), 'db_read_error', 'getEmployeeCount');
        return 0;
    }
}

// تسجيل مخالفة في إكسل — ملف violations.xlsx
async function logViolationToExcel(dateStr, shiftLetter, sender, msgTime, details, count) {
    try {
        if (!fs.existsSync(ERROR_RECORD_DIR)) {
            fs.mkdirSync(ERROR_RECORD_DIR, { recursive: true });
        }
        const filePath = path.join(ERROR_RECORD_DIR, 'violations.xlsx');
        let wb = new ExcelJS.Workbook();
        let ws;

        if (fs.existsSync(filePath)) {
            try {
                await wb.xlsx.readFile(filePath);
                ws = wb.worksheets[0];
            } catch (_) {
                // ملف تالف — إنشاء جديد
                wb = new ExcelJS.Workbook();
                ws = null;
            }
        }

        if (!ws) {
            ws = wb.addWorksheet('مخالفات');
            ws.addRow(VIOLATION_COLUMNS);
            ws.columns = VIOLATION_COLUMNS.map(() => ({ width: 22 }));
        }

        ws.addRow([dateStr, shiftLetter, sender, msgTime || '', details, count]);

        // كتابة آمنة
        const tmpFile = filePath + '.tmp';
        await wb.xlsx.writeFile(tmpFile);
        fs.renameSync(tmpFile, filePath);
    } catch (err) {
        log.error('خطأ تسجيل مخالفة إكسل: ' + err.message);
    }
}

// تحديث إحصائيات السجلات لكل نوبة — يُقرأ من record_2026 في ht_sc_new.db
// يوم العمل يبدأ من نوبة A (05:45) وينتهي بنهاية نوبة C (05:44 اليوم التالي)
// folder_date في القاعدة يعكس يوم العمل الصحيح (نوبة C بعد منتصف الليل = يوم أمس)
function updateRecordStats() {
    try {
        if (!fs.existsSync(STATE_DIR)) fs.mkdirSync(STATE_DIR, { recursive: true });
        // حساب تاريخ يوم العمل الحالي من الوقت الفعلي
        const now = new Date();
        const timeStr = now.toLocaleTimeString('en-GB', { hour12: false }).substring(0, 5);
        const todayDate = getShiftDate(timeStr);
        const d = openNewDb();
        const counts = { A: 0, B: 0, C: 0 };
        const rows = d.prepare(
            'SELECT shift_code, COUNT(*) as cnt FROM record_2026 WHERE folder_date = ? GROUP BY shift_code'
        ).all(todayDate);
        let total = 0;
        for (const r of rows) {
            if (counts.hasOwnProperty(r.shift_code)) {
                counts[r.shift_code] = r.cnt;
                total += r.cnt;
            }
        }
        const content = 'A:' + counts.A + ' B:' + counts.B + ' C:' + counts.C + ' اليوم:' + total;
        fs.writeFileSync(path.join(STATE_DIR, 'violation_stats.txt'), content);
    } catch (_) {}
}

// كتابة إشعار المخالفة في violations.log
function writeViolationLog(num, msgTime, sender, details) {
    try {
        if (!fs.existsSync(STATE_DIR)) fs.mkdirSync(STATE_DIR, { recursive: true });
        const line = num + '. [' + (msgTime || '') + '] ' + sender + ' — ' + details + '\n';
        fs.appendFileSync(path.join(STATE_DIR, 'violations.log'), line);
    } catch (_) {}
}

// تسجيل مخالفة في القاعدة + الإكسل
async function logViolation(sender, msgTime, violations) {
    try {
        const d = openDb();
        const shiftLetter = getShiftLetter(msgTime);
        const now = new Date();
        const dateStr = now.getDate() + '-' +
            String(now.getMonth() + 1).padStart(2, '0') + '-' + now.getFullYear();
        const details = violations.join(' | ');

        const prevCount = getEmployeeCount(sender);
        const newCount = prevCount + 1;

        d.prepare(
            'INSERT INTO violations (date, shift_code, sender_name, msg_time, details, employee_count) VALUES (?, ?, ?, ?, ?, ?)'
        ).run(dateStr, shiftLetter, sender, msgTime || '', details, newCount);

        // تسجيل مزدوج في إكسل
        await logViolationToExcel(dateStr, shiftLetter, sender, msgTime, details, newCount);

        // تحديث إحصائيات السجلات
        updateRecordStats();

        // كتابة إشعار في violations.log
        writeViolationLog(newCount, msgTime, sender, details);

        log.error('مخالفة: ' + sender + ' — ' + details);
    } catch (err) {
        log.error('خطأ تسجيل مخالفة: ' + err.message);
        log.silent('خطأ تسجيل مخالفة: ' + err.message, 'violation_log_error', 'logViolation');
    }
}

// مسح سجلات اليوم لرصد المخالفات السابقة
async function scanTodayRecords() {
    try {
        const d = openDb();
        const now = new Date();
        const today = now.getDate() + '-' +
            String(now.getMonth() + 1).padStart(2, '0') + '-' + now.getFullYear();

        const rows = d.prepare(
            "SELECT t.*, s.name as sender_name, m.time as msg_time FROM trips t " +
            "LEFT JOIN messages m ON t.message_id = m.id " +
            "LEFT JOIN senders s ON m.sender_id = s.id " +
            "WHERE t.date = ? AND t.source = 'monitor'"
        ).all(today);

        if (rows.length === 0) return;

        // بصمات المخالفات المسجلة — لمنع التكرار
        const existingVFPs = new Set();
        const vRows = d.prepare(
            'SELECT sender_name, msg_time FROM violations WHERE date = ?'
        ).all(today);
        for (const v of vRows) {
            existingVFPs.add((v.sender_name || '') + '|' + (v.msg_time || ''));
        }

        let newCount = 0;

        for (const row of rows) {
            const sender = row.sender_name || '';
            const msgTime = row.msg_time || '';
            if (!sender || !msgTime) continue;

            const vfp = sender + '|' + msgTime;
            if (existingVFPs.has(vfp)) continue;

            // بناء حقول الرحلة للفحص
            const fields = {};
            fields['رقم الرحلة'] = row.flight_number || '';
            fields['وقت الاقلاع'] = row.departure_time || '';
            fields['عدد الركاب'] = String(row.passenger_count || '');
            fields['الوجهة'] = row.destination || '';
            fields['الفيزا'] = row.visa_type || '';
            fields['الحملة'] = row.campaign_name || '';
            fields['الحالة'] = row.status || '';
            fields['التفويج'] = row.dispatch || '';
            fields['الكشف'] = row.inspection || '';

            const fakeText = (row.shift_code ? '00' + row.shift_code + '#' : '') + '\n';
            // تمرير getShiftLetter كمعامل — منطق بحت
            const violations = checkViolations(fakeText, [fields], msgTime, getShiftLetter);
            if (violations.length === 0) continue;

            await logViolation(sender, msgTime, violations);
            existingVFPs.add(vfp);
            newCount++;
        }

        if (newCount > 0) {
            log.info('رصد ' + newCount + ' مخالفة من سجلات اليوم');
        }
    } catch (err) {
        log.error('خطأ مسح السجلات: ' + err.message);
        log.silent('خطأ مسح السجلات: ' + err.message, 'scan_error', 'scanTodayRecords');
    }
}

module.exports = { logViolation, scanTodayRecords, getEmployeeCount, updateRecordStats };
