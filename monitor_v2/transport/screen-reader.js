/**
 * قراءة الشاشة فقط — يقرأ عناصر صفحة واتساب ويُرجع رسائل خام.
 */

class ScreenReader {
    constructor(browser) {
        this.browser = browser;
        this.page = null;
    }

    async getPage() {
        if (!this.page || this.page.isClosed()) {
            const pages = await this.browser.pages();
            this.page = pages.find(p => p.url().includes('web.whatsapp.com')) || pages[0] || null;
        }
        return this.page;
    }

    // قراءة رسائل الشاشة المعروضة — يُرجع مصفوفة { id, info, text, time }
    async readScreen() {
        const page = await this.getPage();
        if (!page) return [];
        return await page.evaluate(() => {
            const results = [];
            const msgElements = document.querySelectorAll('.message-in, .message-out');
            msgElements.forEach(el => {
                const textEl = el.querySelector('[data-testid="selectable-text"]');
                const text = textEl ? textEl.innerText : '';
                if (text) {
                    // المرسل: أول span قبل نص الرسالة
                    const allSpans = el.querySelectorAll('span[dir="auto"]');
                    let sender = '';
                    for (const s of allSpans) {
                        const txt = s.innerText || '';
                        // تجاهل: فارغ، داخل نص الرسالة، صيغة وقت، أو رموز بدون أحرف حقيقية
                        if (!textEl.contains(s) && txt && !/^\d{1,2}:\d{2}/.test(txt) && /[\p{L}]{2,}/u.test(txt)) {
                            sender = txt.replace(/^[^\p{L}]+|[^\p{L}]+$/gu, '').trim();
                            if (sender) break;
                        }
                    }
                    // وقت الرسالة
                    let msgTime = '';
                    const timeEl = el.querySelector('[data-testid="msg-time"]');
                    if (timeEl) {
                        msgTime = timeEl.innerText.trim();
                    } else {
                        for (const s of allSpans) {
                            if (!textEl.contains(s) && /^\d{1,2}:\d{2}/.test(s.innerText)) {
                                msgTime = s.innerText.trim();
                                break;
                            }
                        }
                    }
                    // معرف فريد
                    const dataIdEl = el.closest('[data-id]');
                    const id = dataIdEl ? dataIdEl.getAttribute('data-id') : (sender + text);
                    results.push({ id: id, info: sender, text: text, time: msgTime });
                }
            });
            return results;
        });
    }
}

module.exports = ScreenReader;
