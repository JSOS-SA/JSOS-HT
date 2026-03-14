/**
 * فحص المخالفات — منطق بحت بدون تبعيات خارجية.
 * يستقبل getShiftLetter كمعامل بدل الاستيراد المباشر.
 */

// القيم المسموحة لكل حقل — تشمل متغيرات الهاء والتاء المربوطة
const ALLOWED_VALUES = {
    'الفيزا': ['عمرة', 'عمره', 'سياحة', 'سياحه', 'عمل', 'مقيم', 'خليج', 'زيارة', 'زياره', 'اخرى'],
    'الحالة': ['مجدولة', 'مجدوله', 'عدم وجود رحلة', 'عدم وجود رحله', 'ملغية', 'ملغيه', 'غادرت', 'معاد جدولتها', 'متأخرة', 'متأخره', 'متأخر', 'متعثرة', 'متعثره', 'متعثر'],
    'التفويج': ['مبكر', 'متأخر', 'في الموعد', 'فالموعد', 'مشترك', 'خاطئ'],
    'الكشف': ['مطابق', 'غير مطابق', 'لا يوجد'],
};

// نمط وقت الاقلاع HH:MM
const TIME_RE = /^\d{1,2}:\d{2}$/;

// نمط عدد الركاب — رقم فقط
const NUMBER_RE = /^\d+$/;

// رموز النوبات
const SHIFT_CODES = ['00A#', '00B#', '00C#'];

// فحص رمز النوبة في رأس الرسالة
// يستقبل shiftLetterFn كمعامل لكسر التبعية الدائرية
function checkShiftCode(text, msgTime, shiftLetterFn) {
    const violations = [];
    const hasAnyCode = SHIFT_CODES.some(code => text.includes(code));

    if (!hasAnyCode) {
        violations.push('النوبة: ناقص');
        return violations;
    }

    // فحص تطابق الرمز مع النوبة الحالية
    const currentLetter = shiftLetterFn(msgTime);
    if (currentLetter) {
        const expectedCode = '00' + currentLetter + '#';
        if (!text.includes(expectedCode)) {
            violations.push('النوبة: خطأ');
        }
    }

    return violations;
}

// فحص مخالفات حقول رحلة واحدة
function checkTripFields(fields) {
    const violations = [];

    // رقم الرحلة: وجود + غير فارغ
    if (!fields['رقم الرحلة'] || !fields['رقم الرحلة'].trim()) {
        violations.push('رقم الرحلة: ناقص');
    }

    // وقت الاقلاع: وجود + صيغة HH:MM
    if (!fields['وقت الاقلاع'] || !fields['وقت الاقلاع'].trim()) {
        violations.push('وقت الاقلاع: ناقص');
    } else if (!TIME_RE.test(fields['وقت الاقلاع'].trim())) {
        violations.push('وقت الاقلاع: خطأ');
    }

    // عدد الركاب: وجود + رقم
    if (!fields['عدد الركاب'] || !fields['عدد الركاب'].trim()) {
        violations.push('عدد الركاب: ناقص');
    } else if (!NUMBER_RE.test(fields['عدد الركاب'].trim())) {
        violations.push('عدد الركاب: خطأ');
    }

    // الحقول ذات القيم المحددة
    for (const [fieldName, allowedList] of Object.entries(ALLOWED_VALUES)) {
        const val = (fields[fieldName] || '').trim();
        if (!val) {
            violations.push(fieldName + ': ناقص');
        } else if (!allowedList.includes(val)) {
            violations.push(fieldName + ': خطأ');
        }
    }

    return violations;
}

// فحص كامل: رمز النوبة + حقول كل الرحلات
function checkViolations(text, trips, msgTime, shiftLetterFn) {
    const allViolations = [];

    // فحص رمز النوبة
    const shiftViolations = checkShiftCode(text, msgTime, shiftLetterFn);
    allViolations.push(...shiftViolations);

    // فحص حقول كل رحلة
    for (let i = 0; i < trips.length; i++) {
        const tripViolations = checkTripFields(trips[i]);
        if (trips.length > 1) {
            // ترقيم الرحلات عند وجود أكثر من واحدة
            for (const v of tripViolations) {
                allViolations.push('رحلة ' + (i + 1) + ' - ' + v);
            }
        } else {
            allViolations.push(...tripViolations);
        }
    }

    return allViolations;
}

module.exports = {
    ALLOWED_VALUES,
    checkShiftCode,
    checkTripFields,
    checkViolations,
};
