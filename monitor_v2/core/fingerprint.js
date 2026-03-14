/**
 * بناء ومقارنة بصمات الرسائل — لمنع التكرار.
 * البصمة = تاريخ النوبة + المرسل + حرف النوبة + رقم الرحلة + عدد الركاب + التفويج.
 */

// حقول الرحلة المستخدمة في البصمة
const FP_FIELDS = [
    'رقم الرحلة',
    'عدد الركاب',
    'التفويج',
];

// بناء بصمة من تاريخ النوبة + المرسل + حرف النوبة + حقول الرحلات
function buildFingerprint(trips, sender, shiftDate, shiftLetter) {
    const parts = [shiftDate || '', sender || '', shiftLetter || ''];
    for (const fields of trips) {
        for (const name of FP_FIELDS) {
            parts.push(fields[name] || '');
        }
    }
    return parts.join('|');
}

// تحميل بصمات من مصفوفة صفوف قاعدة البيانات (مجمّعة حسب الرسالة)
// rows: نتائج استعلام تحتوي msg_id + حقول الرحلة + sender_name
// shiftDate, shiftLetter: تاريخ النوبة وحرفها — يُمرران من الاستعلام
function loadFingerprintsFromDbRows(rows, shiftDate, shiftLetter) {
    const fingerprints = new Set();

    // تجميع الرحلات حسب الرسالة
    const msgTrips = {};
    for (const r of rows) {
        const key = r.msg_id;
        if (!msgTrips[key]) {
            msgTrips[key] = { sender: r.sender_name || '', trips: [] };
        }
        msgTrips[key].trips.push(r);
    }

    // بناء البصمة لكل رسالة بالصيغة الجديدة
    for (const msg of Object.values(msgTrips)) {
        const tripFields = msg.trips.map((t) => ({
            'رقم الرحلة': t.flight_number || '',
            'عدد الركاب': String(t.passenger_count || ''),
            'التفويج': t.dispatch || '',
        }));
        const fp = buildFingerprint(tripFields, msg.sender, shiftDate, shiftLetter);
        if (fp.replace(/\|/g, '') !== '') {
            fingerprints.add(fp);
        }
    }

    return fingerprints;
}

// تحميل بصمات من صفوف إكسل (مصفوفة ثنائية الأبعاد)
// rows[0] = العناوين، rows[1..n] = البيانات
// الترتيب الجديد: [0]=م [1]=رمز النوبة [2]=المرسل [3]=الوقت [4..]=حقول الرحلات
// الترتيب القديم: [0]=رمز النوبة [1]=المرسل [2]=الوقت [3..]=حقول الرحلات
// shiftDate, shiftLetter: تاريخ النوبة وحرفها — يُمرران من اسم الملف
function loadFingerprintsFromExcelRows(rows, shiftDate, shiftLetter) {
    const fingerprints = new Set();
    if (!rows || rows.length < 2) return fingerprints;

    // كشف وجود عمود المعرّف — إزاحة الأعمدة
    const firstHeader = String(rows[0][0] || '');
    const off = (firstHeader === 'م') ? 1 : 0;
    const headerLen = rows[0].length;
    const tripColsCount = headerLen - (3 + off);
    const tripsPerRow = Math.max(1, Math.floor(tripColsCount / 9));

    for (let i = 1; i < rows.length; i++) {
        const r = rows[i];
        if (!r || r.length === 0) continue;
        // المرسل
        const sender = String(r[off + 1] || '').trim();
        // بناء رحلات بالحقول الثلاثة فقط: رقم الرحلة + عدد الركاب + التفويج
        const tripFields = [];
        for (let t = 0; t < tripsPerRow; t++) {
            const base = (3 + off) + t * 9;
            tripFields.push({
                'رقم الرحلة': String(r[base] || '').trim(),
                'عدد الركاب': String(r[base + 2] || '').trim(),
                'التفويج': String(r[base + 7] || '').trim(),
            });
        }
        const fp = buildFingerprint(tripFields, sender, shiftDate, shiftLetter);
        if (fp.replace(/\|/g, '') !== '') {
            fingerprints.add(fp);
        }
    }

    return fingerprints;
}

module.exports = {
    FP_FIELDS,
    buildFingerprint,
    loadFingerprintsFromDbRows,
    loadFingerprintsFromExcelRows,
};
