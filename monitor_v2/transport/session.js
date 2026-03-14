/**
 * إدارة الجلسة وإعادة الاتصال — تأخير تصاعدي مع ضوضاء عشوائية.
 */

const EventEmitter = require('events');
const { randomDelay, randomInt } = require('../utils/human-behavior');
const log = require('../utils/logger');

class SessionManager extends EventEmitter {
    constructor(client, config) {
        super();
        this.client = client;
        this.config = config.reconnection;
        this.retryCount = 0;
        this.state = 'متوقف';

        client.on('disconnected', (reason) => this.handleDisconnect(reason));
        client.on('auth_failure', () => this.handleAuthFailure());
    }

    async connectWithTimeout(timeoutMs = 60000) {
        return Promise.race([
            this.client.initialize(),
            new Promise((_, reject) =>
                setTimeout(() => reject(new Error('انتهت مهلة الاتصال')), timeoutMs)
            )
        ]);
    }

    async connect() {
        this.state = 'جاري الاتصال...';
        log.info(this.state);

        await randomDelay(2000, 5000);

        try {
            await this.connectWithTimeout();
        } catch (err) {
            log.error('فشل الاتصال: ' + err.message);
            this.scheduleReconnect();
        }
    }

    handleDisconnect(reason) {
        this.state = 'منقطع';
        log.warn('انقطع الاتصال: ' + reason);

        if (reason === 'BANNED') {
            log.error('تم حظر الرقم! لا يمكن إعادة الاتصال.');
            this.state = 'محظور';
            return;
        }

        this.scheduleReconnect();
    }

    handleAuthFailure() {
        log.error('فشل التحقق! احذف مجلد sessions وأعد المسح.');
        this.state = 'فشل التحقق';
    }

    scheduleReconnect() {
        if (this.retryCount >= this.config.maxRetries) {
            log.error('تجاوز الحد الأقصى للمحاولات. أعد التشغيل يدوياً.');
            this.state = 'متوقف';
            return;
        }

        this.retryCount++;
        const delay = Math.min(
            this.config.baseDelay * Math.pow(this.config.backoffMultiplier, this.retryCount - 1),
            this.config.maxDelay
        );
        const jitter = randomInt(0, this.config.jitterRange);
        const totalDelay = delay + jitter;

        this.state = 'إعادة اتصال بعد ' + Math.round(totalDelay / 1000) + ' ثانية';
        log.warn(this.state + ' (محاولة ' + this.retryCount + ')');

        setTimeout(async () => {
            try {
                await this.connectWithTimeout();
                this.retryCount = 0;
                this.state = 'متصل';
            } catch (err) {
                log.error('فشل إعادة الاتصال: ' + err.message);
                this.scheduleReconnect();
            }
        }, totalDelay);
    }

    getState() {
        return this.state;
    }
}

module.exports = SessionManager;
