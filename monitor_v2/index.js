/**
 * نقطة الدخول — تجميع الطبقات وتشغيل المراقب.
 */

const config = require('./config');
const { createStealthBrowser } = require('./transport/browser');
const { randomDelay, startIdleBehavior } = require('./utils/human-behavior');
const SessionManager = require('./transport/session');
const Watcher = require('./orchestrator/watcher');
const { closeDb, closeNewDb } = require('./storage/shared-db');
const log = require('./utils/logger');
const qrcode = require('qrcode-terminal');
const { Client, LocalAuth } = require('whatsapp-web.js');

let stopIdleBehavior = null;

async function main() {
    console.clear();
    log.info('=== مراقب واتساب v2 ===');
    log.system('تشغيل المراقب', 'startup');
    log.info('تهيئة الطبقات الثلاث...');

    // طبقة 1: المتصفح الخفي
    log.info('[طبقة 1] تشغيل المتصفح الخفي...');
    const browser = await createStealthBrowser(config);
    const wsEndpoint = browser.wsEndpoint();

    // طبقة 2: تأخير بشري قبل الاتصال
    log.info('[طبقة 2] تأخير بشري...');
    await randomDelay(2000, 5000);

    // طبقة 3: إعداد الجلسة
    log.info('[طبقة 3] إعداد الجلسة...');
    const client = new Client({
        authStrategy: new LocalAuth({ dataPath: config.sessionDir }),
        puppeteer: {
            browserWSEndpoint: wsEndpoint,
        },
    });

    const sessionManager = new SessionManager(client, config);
    const watcher = new Watcher(config, browser, client);

    // عرض رمز QR
    client.on('qr', (qr) => {
        log.info('امسح رمز QR من هاتفك:');
        qrcode.generate(qr, { small: true });
    });

    // تم الاتصال
    client.on('ready', async () => {
        log.info('متصل بنجاح!');
        log.info('────────────────────────');
        watcher.markStart();

        // إيقاف المراقب القديم (يمنع تراكم intervals عند إعادة الاتصال)
        watcher.stop();
        if (stopIdleBehavior) { stopIdleBehavior(); stopIdleBehavior = null; }

        // قراءة الرسائل المعروضة
        await randomDelay(2000, 4000);
        await watcher.readOpenChat();

        log.info('────────────────────────');

        // تشغيل السلوك البشري
        const pages = await browser.pages();
        const waPage = pages.find(p => p.url().includes('web.whatsapp.com')) || pages[0];
        if (waPage) {
            stopIdleBehavior = startIdleBehavior(waPage, config);
        }

        // مراقبة الشاشة للرسائل الجديدة
        await watcher.watchScreen(1);
    });

    // تسجيل الدخول
    client.on('authenticated', () => {
        log.info('تم التحقق بنجاح');
    });

    // جاري التحميل
    client.on('loading_screen', (percent) => {
        log.info('تحميل: ' + percent + '%');
    });

    // إغلاق آمن
    const shutdown = async () => {
        log.warn('جاري الإغلاق...');
        log.system('إيقاف المراقب', 'shutdown');
        if (stopIdleBehavior) stopIdleBehavior();
        watcher.stop();
        watcher.markStop();
        closeDb();
        closeNewDb();
        log.closeLogsDb();
        try { await client.destroy(); } catch (_) {}
        try { await browser.close(); } catch (_) {}
        process.exit(0);
    };

    process.on('SIGINT', shutdown);
    process.on('SIGTERM', shutdown);
    process.on('SIGHUP', shutdown);

    // بدء الاتصال
    await sessionManager.connect();
}

main().catch(err => {
    log.error('خطأ: ' + err.message);
    log.silent('خطأ قاتل في المراقب: ' + err.message + '\n' + (err.stack || ''), 'fatal_error', 'main');
    log.closeLogsDb();
    process.exit(1);
});
