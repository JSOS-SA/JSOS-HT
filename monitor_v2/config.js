const path = require('path');

module.exports = {
    // مسار Chrome الموجود فعلياً — يتجاوز مشكلة عدم تطابق الإصدار
    chromiumPath: path.join(
        process.env.USERPROFILE || '', '.cache', 'puppeteer',
        'chrome', 'win64-146.0.7680.31', 'chrome-win64', 'chrome.exe'
    ),

    sessionDir: path.join(__dirname, 'sessions'),

    // مجلد الحالة — بديل /tmp/screen_monitor
    stateDir: path.join(__dirname, 'state'),

    // مسار قاعدة البيانات المشتركة مع النظام البايثوني
    dbPath: path.join(__dirname, '..', 'db', 'ht_sc.db'),

    // مسار قاعدة البيانات الجديدة — record_2026 (قراءة وكتابة من المراقب)
    newDbPath: path.join(__dirname, '..', 'db', 'ht_sc_new.db'),

    browserArgs: [
        '--no-sandbox',
        '--disable-setuid-sandbox',
        '--disable-dev-shm-usage',
        '--disable-gpu',
        '--disable-extensions',
        '--disable-background-networking',
        '--disable-default-apps',
        '--disable-sync',
        '--disable-translate',
        '--no-first-run',
        '--disable-features=AudioServiceOutOfProcess,TranslateUI',
        '--window-size=1366,768',
    ],

    humanBehavior: {
        minActionDelay: 800,
        maxActionDelay: 3000,
        initialLoadWait: 5000,
        scrollPauseMin: 1000,
        scrollPauseMax: 4000,
    },

    reconnection: {
        maxRetries: 10,
        baseDelay: 30000,
        maxDelay: 600000,
        backoffMultiplier: 1.5,
        jitterRange: 5000,
    },

    display: {
        maxMessageLength: 2000,
        showGroupMessages: true,
        showPrivateMessages: true,
    },
};
