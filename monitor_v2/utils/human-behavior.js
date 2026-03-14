/**
 * محاكاة السلوك البشري — تأخيرات عشوائية وحركات الفأرة.
 */

const crypto = require('crypto');

function randomInt(min, max) {
    return crypto.randomInt(min, max + 1);
}

function randomDelay(min, max) {
    const ms = randomInt(min, max);
    return new Promise(resolve => setTimeout(resolve, ms));
}

function startIdleBehavior(page, config) {
    let running = true;

    async function doAction() {
        if (!running) return;
        try {
            const x = randomInt(200, 800);
            const y = randomInt(200, 500);
            await page.mouse.move(x, y, { steps: randomInt(3, 8) });

            if (randomInt(1, 5) === 1) {
                await page.evaluate(() => {
                    const panel = document.querySelector('#pane-side');
                    if (panel) panel.scrollTop += (Math.random() - 0.5) * 100;
                });
            }
        } catch (_) {}
        if (running) setTimeout(doAction, randomInt(30000, 120000));
    }

    setTimeout(doAction, randomInt(30000, 120000));
    return () => { running = false; };
}

module.exports = { randomDelay, randomInt, startIdleBehavior };
