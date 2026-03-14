/**
 * المتصفح الخفي — تشغيل Chromium مع إضافة التخفي.
 */

const puppeteerExtra = require('puppeteer-extra');
const StealthPlugin = require('puppeteer-extra-plugin-stealth');
const log = require('../utils/logger');

puppeteerExtra.use(StealthPlugin());

async function createStealthBrowser(config) {
    log.info('تشغيل المتصفح الخفي...');

    const launchOptions = {
        headless: false,
        args: config.browserArgs,
        userDataDir: config.sessionDir + '/browser_profile',
        defaultViewport: null,
        ignoreDefaultArgs: ['--enable-automation'],
    };

    // إذا chromiumPath محدد يستخدمه — وإلا puppeteer يحمّل Chromium تلقائياً
    if (config.chromiumPath) {
        launchOptions.executablePath = config.chromiumPath;
    }

    const browser = await puppeteerExtra.launch(launchOptions);

    log.info('المتصفح جاهز');
    return browser;
}

module.exports = { createStealthBrowser };
