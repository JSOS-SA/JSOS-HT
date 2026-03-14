/**
 * قراءة/كتابة ملفات الحالة — الإحصائيات ووقت البدء.
 */

const fs = require('fs');
const path = require('path');

// كتابة إحصائيات المراقب
function writeStats(stateDir, messageCount, newCount) {
    try {
        const now = new Date().toLocaleTimeString('en-GB', { hour12: false });
        const lines = [
            'الرسائل: ' + messageCount,
            'جديدة: ' + newCount,
            'اخر: ' + now,
        ];
        fs.writeFileSync(path.join(stateDir, 'stats.txt'), lines.join('\n'));
    } catch (_) {}
}

// تسجيل وقت بدء المراقب
function writeStartTime(stateDir) {
    try {
        fs.writeFileSync(
            path.join(stateDir, 'start_time.txt'),
            String(Math.floor(Date.now() / 1000))
        );
        // تسجيل حالة المراقب كنشط
        fs.writeFileSync(path.join(stateDir, 'monitor_active'), '1');
    } catch (_) {}
}

// إزالة وقت البدء عند الإيقاف
function removeStartTime(stateDir) {
    try { fs.unlinkSync(path.join(stateDir, 'start_time.txt')); } catch (_) {}
    // تسجيل حالة المراقب كمتوقف
    try { fs.writeFileSync(path.join(stateDir, 'monitor_active'), '2'); } catch (_) {}
}

// التأكد من وجود مجلد الحالة
function ensureStateDir(stateDir) {
    if (!fs.existsSync(stateDir)) {
        fs.mkdirSync(stateDir, { recursive: true });
    }
}

module.exports = {
    writeStats,
    writeStartTime,
    removeStartTime,
    ensureStateDir,
};
