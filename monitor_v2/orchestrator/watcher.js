/**
 * المراقب الرئيسي — ينسق بين القراءة والتحليل والتخزين.
 * يربط كل الطبقات: screen-reader + parser + validator + db + violations.
 * منع التكرار: مفاتيح محتوى من القاعدة + ذاكرة الجلسة.
 * المراقب يكتب في القاعدة فقط — الإكسل يُكتب من write_excel_from_db.py.
 */

const fs = require('fs');
const path = require('path');
const log = require('../utils/logger');
const { writeStats, writeStartTime, removeStartTime, ensureStateDir } = require('../utils/state');
const { parseMultiTrips } = require('../core/parser');
const { getShiftLetter, getShiftCode, getShiftDate } = require('../core/shift');
const { checkViolations } = require('../core/validator');
const { insertRecord, loadExistingRecordKeys, getLastRecord, buildContentKey } = require('../storage/db');
const { logViolation, scanTodayRecords, updateRecordStats } = require('../storage/violation-store');
const ScreenReader = require('../transport/screen-reader');

const LOG_FILE = path.join(__dirname, '..', 'messages.log');

class Watcher {
    constructor(config, browser, client) {
        this.displayConfig = config.display;
        this.stateDir = config.stateDir || path.join(__dirname, '..', 'state');
        this.browser = browser;
        this.client = client;
        this.screenReader = new ScreenReader(browser);
        this.messageCount = 0;
        this.seenMessages = new Set();
        this.existingKeysByDate = new Map();
        this.watchInterval = null;

        ensureStateDir(this.stateDir);
    }

    // قراءة المحادثة المعروضة — أول تشغيل
    async readOpenChat() {
        log.info('جاري قراءة المحادثة المعروضة...');
        try {
            const now = new Date().toLocaleTimeString('en-GB', { hour12: false }).substring(0, 5);
            const shiftLetter = getShiftLetter(now);
            const shiftDate = getShiftDate(now);
            const lastInfo = getLastRecord(shiftDate, shiftLetter);
            if (lastInfo) {
                log.info('────────────────────────');
                log.info('آخر صف مسجل في القاعدة:');
                log.info('عدد الصفوف: ' + lastInfo.totalRows + ' | المعرّف: #' + lastInfo.id);
                log.info('رحلة: ' + lastInfo.flight + ' | وقت: ' + lastInfo.depTime + ' | ركاب: ' + lastInfo.passengers);
                log.info('الوجهة: ' + lastInfo.destination + ' | المرسل: ' + lastInfo.sender);
                log.info('────────────────────────');
            } else {
                log.info('لا توجد سجلات سابقة - تسجيل كل الرسائل');
            }
            // تحميل مفاتيح المحتوى من القاعدة لمنع التكرار
            this._loadKeysForDate(shiftDate);
            log.info('مفاتيح مسجلة: ' + this.existingKeysByDate.get(shiftDate).size);

            // مسح سجلات اليوم لرصد المخالفات السابقة
            await scanTodayRecords();

            // قراءة رسائل الشاشة
            const messages = await this.screenReader.readScreen();

            if (messages.length === 0) {
                log.warn('لا توجد رسائل معروضة');
                return;
            }

            let newCount = 0;
            let skippedCount = 0;
            for (const msg of messages) {
                const result = await this._processMessage(msg);
                if (result === 'new') newCount++;
                else if (result === 'skipped') skippedCount++;
            }
            writeStats(this.stateDir, this.messageCount, newCount);
            if (skippedCount > 0) {
                log.info('تم تخطي ' + skippedCount + ' رسالة مسجلة مسبقاً');
            }
            log.info('تم تسجيل ' + newCount + ' رسالة جديدة من ' + messages.length + ' معروضة');
        } catch (err) {
            log.error('فشل قراءة المحادثة: ' + err.message);
            log.silent('فشل قراءة المحادثة: ' + err.message, 'read_chat_error', 'readOpenChat');
        }
    }

    // تحديث بيانات النوبة
    async syncShiftData() {
        try {
            log.info('────────────────────────');
            log.info('تحديث بيانات النوبة...');
            const now = new Date().toLocaleTimeString('en-GB', { hour12: false }).substring(0, 5);
            const shiftLetter = getShiftLetter(now);
            const shiftDate = getShiftDate(now);

            // إعادة تحميل مفاتيح المحتوى من القاعدة
            this._loadKeysForDate(shiftDate, true);
            log.info('مفاتيح مسجلة: ' + this.existingKeysByDate.get(shiftDate).size);

            // مسح المخالفات
            await scanTodayRecords();

            const messages = await this.screenReader.readScreen();
            if (messages.length === 0) {
                log.info('لا توجد رسائل معروضة');
                log.info('────────────────────────');
                return;
            }

            let newCount = 0;
            let skippedCount = 0;
            for (const msg of messages) {
                const result = await this._processMessage(msg);
                if (result === 'new') newCount++;
                else if (result === 'skipped') skippedCount++;
            }
            writeStats(this.stateDir, this.messageCount, newCount);
            if (skippedCount > 0) log.info('تم تخطي ' + skippedCount + ' رسالة مسجلة مسبقاً');
            log.info('تم تسجيل ' + newCount + ' رسالة جديدة من ' + messages.length + ' معروضة');
            log.info('────────────────────────');
        } catch (err) {
            log.error('خطأ تحديث النوبة: ' + err.message);
            log.silent('خطأ تحديث النوبة: ' + err.message, 'sync_error', 'syncShiftData');
        }
    }

    // مراقبة الشاشة دورياً
    async watchScreen(intervalSeconds = 3) {
        log.info('مراقبة الشاشة كل ' + intervalSeconds + ' ثواني...');
        const syncTrigger = path.join(this.stateDir, 'sync_trigger');
        this.watchInterval = setInterval(async () => {
            try {
                // فحص إشارة التحديث
                if (fs.existsSync(syncTrigger)) {
                    try { fs.unlinkSync(syncTrigger); } catch (_) {}
                    await this.syncShiftData();
                    return;
                }

                const messages = await this.screenReader.readScreen();
                let newInThisCycle = 0;
                for (const msg of messages) {
                    if (!this.seenMessages.has(msg.id)) {
                        const result = await this._processMessage(msg);
                        if (result === 'new') newInThisCycle++;
                    }
                }
                writeStats(this.stateDir, this.messageCount, newInThisCycle);
                // تنظيف seenMessages عند تجاوز الحد
                if (this.seenMessages.size > 10000) {
                    const arr = [...this.seenMessages];
                    this.seenMessages = new Set(arr.slice(-5000));
                }
            } catch (err) {
                if (err.message && !err.message.includes('Session closed')) {
                    log.error('خطأ مراقبة: ' + err.message);
                    log.silent('خطأ مراقبة الشاشة: ' + err.message, 'watch_error', 'watchScreen');
                }
            }
        }, intervalSeconds * 1000);
    }

    // إيقاف المراقبة
    stop() {
        if (this.watchInterval) {
            clearInterval(this.watchInterval);
            this.watchInterval = null;
        }
    }

    // تحميل مفاتيح تاريخ معيّن — تحميل كسول مع إمكانية التحديث
    _loadKeysForDate(folderDate, forceReload = false) {
        if (forceReload || !this.existingKeysByDate.has(folderDate)) {
            try {
                this.existingKeysByDate.set(folderDate, loadExistingRecordKeys(folderDate));
            } catch (_) {
                this.existingKeysByDate.set(folderDate, new Set());
            }
        }
        return this.existingKeysByDate.get(folderDate);
    }

    // معالجة رسالة واحدة — يُرجع 'new' أو 'skipped' أو null
    async _processMessage(msg) {
        this.seenMessages.add(msg.id);
        let sender = msg.info.replace(/[\[\]]/g, '').replace(/^[^\p{L}]+|[^\p{L}]+$/gu, '').trim() || '';
        const body = msg.text.substring(0, this.displayConfig.maxMessageLength);
        const msgTime = msg.time || '';

        // تقسيم الرسالة لرحلات
        const trips = parseMultiTrips(body);
        if (trips.length === 0) return null;

        // استخراج تاريخ الرسالة الفعلي واسم المرسل من واتساب
        let msgDate = '';
        if (this.client && msg.id) {
            try {
                const waMsg = await this.client.getMessageById(msg.id);
                if (waMsg) {
                    // التاريخ من طابع يونكس — حشو الصفر لليوم
                    if (waMsg.timestamp) {
                        const dt = new Date(waMsg.timestamp * 1000);
                        const d = String(dt.getDate()).padStart(2, '0');
                        const mo = String(dt.getMonth() + 1).padStart(2, '0');
                        const y = dt.getFullYear();
                        msgDate = d + '-' + mo + '-' + y;
                    }
                    // اسم المرسل — بديل عند غياب الاسم من الشاشة
                    if (!sender) {
                        try {
                            const contact = await waMsg.getContact();
                            if (contact) {
                                sender = contact.name || contact.pushname || contact.shortName || '';
                            }
                        } catch (_) {}
                    }
                }
            } catch (_) {}
        }

        // حساب تاريخ النوبة وحرف النوبة ورمز النوبة
        const shiftLetter = getShiftLetter(msgTime);
        const shiftDate = getShiftDate(msgTime, msgDate);
        const shiftCode = getShiftCode(msgTime);

        // بصمة المحتوى الكاملة — كل البيانات وكل الرحلات
        const contentKey = buildContentKey(shiftDate, shiftLetter, sender, msgTime, trips);
        const dateKeys = this._loadKeysForDate(shiftDate);
        if (dateKeys.has(contentKey)) return 'skipped';

        this.messageCount++;

        // فحص المخالفات
        try {
            const violations = checkViolations(body, trips, msgTime, getShiftLetter);
            if (violations.length > 0) {
                await logViolation(sender, msgTime, violations);
            }
        } catch (vErr) {
            log.error('خطأ فحص المخالفات: ' + vErr.message);
        }

        // الإدراج في القاعدة — المعرّف الفريد فوري
        let recordId = null;
        try {
            recordId = insertRecord(shiftDate, shiftLetter, sender, msgTime, trips);
        } catch (err) {
            log.error('خطأ إدراج القاعدة: ' + err.message);
            log.silent('خطأ إدراج القاعدة: ' + err.message, 'db_insert_error', '_processMessage');
        }

        // إضافة المفتاح للذاكرة وتحديث الإحصائيات بعد الإدراج الناجح
        if (recordId) {
            dateKeys.add(contentKey);
            updateRecordStats();
        }

        log.msg(sender, body, msgTime, msgDate, recordId, null);
        this._logToFile(sender, body, msgTime, msgDate, recordId, null);
        return 'new';
    }

    // تسجيل في ملف نصي — يشمل المعرّف والتاريخ
    _logToFile(sender, body, msgTime, msgDate, messageId, excelFile) {
        const timestamp = new Date().toLocaleString('en-GB', { hour12: false });
        const timeStr = msgTime ? ' (' + msgTime + ')' : '';
        const dateStr = msgDate ? ' [' + msgDate + ']' : '';
        const idStr = messageId ? ' #' + messageId : '';
        const filePart = excelFile ? ' -> ' + excelFile : '';
        const line = '[' + timestamp + '] ' + sender + timeStr + dateStr + idStr + filePart + ': ' + body + '\n';
        try { fs.appendFileSync(LOG_FILE, line); } catch (_) {}
    }

    // تسجيل وقت البدء
    markStart() {
        writeStartTime(this.stateDir);
    }

    // إزالة وقت البدء
    markStop() {
        removeStartTime(this.stateDir);
    }

    getCount() {
        return this.messageCount;
    }
}

module.exports = Watcher;
