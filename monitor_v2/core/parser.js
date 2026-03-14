/**
 * استخراج الحقول من نص الرسائل — منطق تحليل بحت.
 * مصدر واحد لأنماط الحقول وتنظيف القيم.
 */

// أنماط استخراج الحقول من نص الرسالة
// [ \t]* بدل \s* بعد النقطتين — لمنع القفز لسطر جديد عند القيمة الفارغة
const FIELD_PATTERNS = {
    'رقم الرحلة': /رقم\s*الرحل[ةه][ \t]*[: ][ \t]*(.+)/,
    'وقت الاقلاع': /وقت[ \t]*ال[اإ]قلاع[ \t]*[: ][ \t]*(.+)/,
    'عدد الركاب': /عدد\s*الركاب[ \t]*[: ][ \t]*(.+)/,
    'الوجهة': /الوجه[ةه][ \t]*[: ][ \t]*(.+)/,
    'الفيزا': /الفيزا[ \t]*[: ][ \t]*(.+)/,
    'الحملة': /الحمل[ةه][ \t]*[: ][ \t]*(.+)/,
    'الحالة': /الحال[ةه][ \t]*[: ][ \t]*(.+)/,
    'التفويج': /التفويج[ \t]*[: ][ \t]*(.+)/,
    'الكشف': /الكشف[ \t]*[: ][ \t]*(.+)/,
};

// تنظيف القيمة المستخرجة من علامات ونقاط ومسافات دخيلة
function cleanValue(val) {
    if (!val) return '';
    let v = val.trim();
    // إزالة أحرف التنسيق غير المرئية من كل النص (Unicode Cf)
    v = v.replace(/[\u200B-\u200F\u061C\u2066-\u2069\u202A-\u202E\uFEFF]/g, '');
    // إزالة الرموز والعلامات من البداية والنهاية — إبقاء الأحرف والأرقام فقط
    v = v.replace(/^[^\p{L}\p{N}]+/gu, '');
    v = v.replace(/[^\p{L}\p{N}]+$/gu, '');
    // إذا بقيت رموز فقط بدون أحرف أو أرقام — تجاهل
    if (!/[\p{L}\p{N}]/u.test(v)) return '';
    return v.trim();
}

// استخراج الحقول من نص رحلة واحدة
function parseMessageFields(text) {
    const fields = {};
    for (const [name, pattern] of Object.entries(FIELD_PATTERNS)) {
        const match = text.match(pattern);
        fields[name] = match ? cleanValue(match[1]) : '';
    }
    return fields;
}

// تقسيم رسالة تحتوي عدة رحلات (حافلة مشتركة)
function parseMultiTrips(text) {
    const parts = text.split(/(?=رقم\s*الرحل[ةه]\s*[:\s])/);
    const trips = [];
    for (const part of parts) {
        const fields = parseMessageFields(part);
        const hasFields = Object.values(fields).some((v) => v !== '');
        if (hasFields) trips.push(fields);
    }
    return trips;
}

// فحص هل الرسالة متعثرة
function isStuckMessage(fields) {
    const status = (fields['الحالة'] || '').trim();
    return /متعثر[ةه]?/.test(status);
}

module.exports = {
    FIELD_PATTERNS,
    cleanValue,
    parseMessageFields,
    parseMultiTrips,
    isStuckMessage,
};
