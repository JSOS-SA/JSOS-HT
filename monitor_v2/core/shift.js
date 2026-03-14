/**
 * حساب النوبة والتاريخ من الوقت.
 * A=05:45-13:44, B=13:45-21:44, C=21:45-05:44
 */

// حساب حرف النوبة من الوقت
function getShiftLetter(timeStr) {
    if (!timeStr) return '';
    const match = timeStr.match(/(\d{1,2}):(\d{2})/);
    if (!match) return '';
    const h = parseInt(match[1], 10);
    const m = parseInt(match[2], 10);
    const mins = h * 60 + m;
    if (mins >= 345 && mins <= 824) return 'A';
    if (mins >= 825 && mins <= 1304) return 'B';
    return 'C';
}

// رمز النوبة الكامل — 00A#
function getShiftCode(timeStr) {
    const letter = getShiftLetter(timeStr);
    if (!letter) return '';
    return '00' + letter + '#';
}

// تحديد تاريخ النوبة — نوبة C بعد منتصف الليل تتبع يوم أمس
// msgDate: تاريخ الرسالة بصيغة D-MM-YYYY — إذا فارغ يستخدم تاريخ النظام
function getShiftDate(timeStr, msgDate) {
    let now;
    if (msgDate) {
        const parts = msgDate.match(/(\d{1,2})-(\d{2})-(\d{4})/);
        now = parts
            ? new Date(parseInt(parts[3], 10), parseInt(parts[2], 10) - 1, parseInt(parts[1], 10))
            : new Date();
    } else {
        now = new Date();
    }
    const letter = getShiftLetter(timeStr);
    if (letter === 'C' && timeStr) {
        const match = timeStr.match(/(\d{1,2}):(\d{2})/);
        if (match) {
            const h = parseInt(match[1], 10);
            const m = parseInt(match[2], 10);
            const mins = h * 60 + m;
            // قبل 05:45 = اليوم السابق (بداية يوم العمل 5:45)
            if (mins < 345) {
                now.setDate(now.getDate() - 1);
            }
        }
    }
    const d = now.getDate();
    const mo = String(now.getMonth() + 1).padStart(2, '0');
    const y = now.getFullYear();
    return String(d).padStart(2, '0') + '-' + mo + '-' + y;
}

module.exports = {
    getShiftLetter,
    getShiftCode,
    getShiftDate,
};
