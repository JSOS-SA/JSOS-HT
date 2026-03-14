"""إعداد نظام التسجيل — يكتب في قاعدة بيانات SQLite + يعرض على الشاشة بالألوان.

الملف: scripts/logger_config.py
"""

import contextlib
import logging
import sqlite3
import sys
import threading
import traceback
from pathlib import Path

# مسار قاعدة السجلات — منفصلة عن قاعدة البيانات الرئيسية
_BASE_DIR = Path(__file__).resolve().parent.parent
LOGS_DIR = _BASE_DIR / "logs"
LOGS_DB_PATH = LOGS_DIR / "ht_sc_logs.db"

# === ألوان الطرفية (نسخة من common.py لتجنب الاستيراد الدائري) ===
_GREEN = "\033[92m"
_RED = "\033[91m"
_ORANGE = "\033[93m"
_CYAN = "\033[96m"
_DIM = "\033[2m"
_RESET = "\033[0m"

# تعيين لون لكل مستوى تسجيل
_LEVEL_COLORS = {
    logging.DEBUG: _DIM,
    logging.INFO: _GREEN,
    logging.WARNING: _ORANGE,
    logging.ERROR: _RED,
    logging.CRITICAL: _RED,
}

# === أسماء الجداول السبعة ===
_TABLES_SQL = """
-- أحداث النظام: تشغيل، إغلاق، معلومات البيئة
CREATE TABLE IF NOT EXISTS system_events (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    timestamp TEXT NOT NULL DEFAULT (strftime('%Y-%m-%d %H:%M:%f', 'now', 'localtime')),
    level TEXT NOT NULL,
    script_name TEXT,
    function_name TEXT,
    message TEXT,
    event_type TEXT,
    python_version TEXT,
    os_info TEXT,
    is_wt INTEGER,
    available_memory_mb REAL,
    working_directory TEXT,
    libraries_status TEXT
);

-- تفاعلات المستخدم: إدخالات، اختيارات، تنقل
CREATE TABLE IF NOT EXISTS user_actions (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    timestamp TEXT NOT NULL DEFAULT (strftime('%Y-%m-%d %H:%M:%f', 'now', 'localtime')),
    level TEXT NOT NULL,
    script_name TEXT,
    function_name TEXT,
    message TEXT,
    action_type TEXT,
    prompt_text TEXT,
    user_response TEXT,
    is_valid INTEGER
);

-- عمليات الملفات: فتح، قراءة، كتابة، نسخ، حذف
CREATE TABLE IF NOT EXISTS file_operations (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    timestamp TEXT NOT NULL DEFAULT (strftime('%Y-%m-%d %H:%M:%f', 'now', 'localtime')),
    level TEXT NOT NULL,
    script_name TEXT,
    function_name TEXT,
    message TEXT,
    operation TEXT,
    file_path TEXT,
    file_size_bytes INTEGER,
    result TEXT,
    error_reason TEXT,
    duration_seconds REAL,
    command_output TEXT
);

-- جودة البيانات: قيم مشبوهة أو ناقصة
CREATE TABLE IF NOT EXISTS data_quality (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    timestamp TEXT NOT NULL DEFAULT (strftime('%Y-%m-%d %H:%M:%f', 'now', 'localtime')),
    level TEXT NOT NULL,
    script_name TEXT,
    function_name TEXT,
    message TEXT,
    source_file TEXT,
    row_number INTEGER,
    column_name TEXT,
    actual_value TEXT,
    expected_type TEXT,
    issue_type TEXT
);

-- سجل المعالجة: كل عملية معالجة بيانات
CREATE TABLE IF NOT EXISTS processing_log (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    timestamp TEXT NOT NULL DEFAULT (strftime('%Y-%m-%d %H:%M:%f', 'now', 'localtime')),
    level TEXT NOT NULL,
    script_name TEXT,
    function_name TEXT,
    message TEXT,
    operation_type TEXT,
    input_file TEXT,
    output_file TEXT,
    rows_read INTEGER,
    rows_written INTEGER,
    trips_extracted INTEGER,
    duration_seconds REAL,
    result TEXT
);

-- عمليات قاعدة البيانات الرئيسية ht_sc.db
CREATE TABLE IF NOT EXISTS db_operations (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    timestamp TEXT NOT NULL DEFAULT (strftime('%Y-%m-%d %H:%M:%f', 'now', 'localtime')),
    level TEXT NOT NULL,
    script_name TEXT,
    function_name TEXT,
    message TEXT,
    operation TEXT,
    target_db TEXT,
    query_summary TEXT,
    rows_affected INTEGER,
    error_sql TEXT
);

-- الأخطاء: كل استثناء مع التفاصيل
CREATE TABLE IF NOT EXISTS errors (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    timestamp TEXT NOT NULL DEFAULT (strftime('%Y-%m-%d %H:%M:%f', 'now', 'localtime')),
    level TEXT NOT NULL,
    script_name TEXT,
    function_name TEXT,
    message TEXT,
    error_type TEXT,
    traceback_full TEXT,
    line_number INTEGER,
    context TEXT
);

-- فهارس لتسريع البحث
CREATE INDEX IF NOT EXISTS idx_system_events_ts ON system_events(timestamp);
CREATE INDEX IF NOT EXISTS idx_system_events_level ON system_events(level);
CREATE INDEX IF NOT EXISTS idx_system_events_script ON system_events(script_name);

CREATE INDEX IF NOT EXISTS idx_user_actions_ts ON user_actions(timestamp);
CREATE INDEX IF NOT EXISTS idx_user_actions_level ON user_actions(level);
CREATE INDEX IF NOT EXISTS idx_user_actions_script ON user_actions(script_name);

CREATE INDEX IF NOT EXISTS idx_file_operations_ts ON file_operations(timestamp);
CREATE INDEX IF NOT EXISTS idx_file_operations_level ON file_operations(level);
CREATE INDEX IF NOT EXISTS idx_file_operations_script ON file_operations(script_name);

CREATE INDEX IF NOT EXISTS idx_data_quality_ts ON data_quality(timestamp);
CREATE INDEX IF NOT EXISTS idx_data_quality_level ON data_quality(level);
CREATE INDEX IF NOT EXISTS idx_data_quality_script ON data_quality(script_name);

CREATE INDEX IF NOT EXISTS idx_processing_log_ts ON processing_log(timestamp);
CREATE INDEX IF NOT EXISTS idx_processing_log_level ON processing_log(level);
CREATE INDEX IF NOT EXISTS idx_processing_log_script ON processing_log(script_name);

CREATE INDEX IF NOT EXISTS idx_db_operations_ts ON db_operations(timestamp);
CREATE INDEX IF NOT EXISTS idx_db_operations_level ON db_operations(level);
CREATE INDEX IF NOT EXISTS idx_db_operations_script ON db_operations(script_name);

CREATE INDEX IF NOT EXISTS idx_errors_ts ON errors(timestamp);
CREATE INDEX IF NOT EXISTS idx_errors_level ON errors(level);
CREATE INDEX IF NOT EXISTS idx_errors_script ON errors(script_name);
"""


class _SQLiteHandler(logging.Handler):
    """معالج تسجيل يكتب في قاعدة بيانات SQLite.

    يستخدم خيطاً مستقلاً لعدم تعطيل البرنامج الرئيسي
    """

    def __init__(self) -> None:
        super().__init__()
        # إنشاء مجلد السجلات إذا لم يكن موجوداً
        LOGS_DIR.mkdir(parents=True, exist_ok=True)
        self._local = threading.local()
        # إنشاء الجداول عند أول تشغيل
        self._ensure_tables()

    def _get_conn(self) -> sqlite3.Connection:
        """اتصال واحد لكل خيط — يتجنب مشاكل التزامن"""
        if not hasattr(self._local, "conn") or self._local.conn is None:
            conn = sqlite3.connect(str(LOGS_DB_PATH), timeout=5)
            conn.execute("PRAGMA journal_mode=WAL")
            conn.execute("PRAGMA synchronous=NORMAL")
            self._local.conn = conn
        return self._local.conn

    def _ensure_tables(self) -> None:
        """إنشاء الجداول والفهارس إذا لم تكن موجودة"""
        conn = self._get_conn()
        conn.executescript(_TABLES_SQL)
        conn.commit()

    def emit(self, record: logging.LogRecord) -> None:
        """كتابة سجل واحد في الجدول المناسب"""
        try:
            # استخراج الجدول المستهدف من extra — الافتراضي system_events
            table = getattr(record, "log_table", None)
            if not table:
                return  # تجاهل السجلات بدون جدول محدد

            # الأعمدة المشتركة
            base = {
                "level": record.levelname,
                "script_name": getattr(record, "script_name", record.module),
                "function_name": getattr(record, "func_name", record.funcName),
                "message": record.getMessage(),
            }

            # دمج الأعمدة الإضافية من extra
            extra = getattr(record, "extra_cols", {})
            cols = {**base, **extra}

            # بناء استعلام الإدخال ديناميكياً
            col_names = ", ".join(cols.keys())
            placeholders = ", ".join(["?"] * len(cols))
            sql = f"INSERT INTO {table} ({col_names}) VALUES ({placeholders})"

            conn = self._get_conn()
            conn.execute(sql, list(cols.values()))
            conn.commit()
        except Exception:
            # منع الحلقة اللانهائية — لا نسجّل أخطاء التسجيل نفسها
            pass

    def close(self) -> None:
        """إغلاق الاتصال عند إيقاف المعالج"""
        if hasattr(self._local, "conn") and self._local.conn:
            with contextlib.suppress(Exception):
                self._local.conn.close()
            self._local.conn = None
        super().close()


class _ColorStreamHandler(logging.StreamHandler):
    """معالج يعرض على الشاشة بألوان — لا يغيّر الألوان الحالية في common.py.

    يعرض فقط سجلات WARNING وما فوقها حتى لا يزحم الشاشة
    """

    def __init__(self) -> None:
        super().__init__(sys.stderr)
        self.setLevel(logging.WARNING)

    def emit(self, record: logging.LogRecord) -> None:
        """طباعة ملوّنة على الشاشة"""
        try:
            color = _LEVEL_COLORS.get(record.levelno, _RESET)
            msg = self.format(record)
            # الطباعة المباشرة تتجنب التداخل مع print المُعدّل في common.py
            sys.stderr.write(f"{color}{msg}{_RESET}\n")
            sys.stderr.flush()
        except Exception:
            pass


# === المسجّل الرئيسي ===
_logger = None
_initialized = False


def setup_logging() -> logging.Logger:
    """تفعيل نظام التسجيل — تُستدعى مرة واحدة من main.py.

    تُعيد المسجّل الرئيسي
    """
    global _logger, _initialized
    if _initialized:
        return _logger

    _logger = logging.getLogger("ht_sc")
    _logger.setLevel(logging.DEBUG)
    # منع تسرّب السجلات للمسجّل الجذري
    _logger.propagate = False

    # معالج القاعدة — يستقبل كل المستويات
    db_handler = _SQLiteHandler()
    db_handler.setLevel(logging.DEBUG)
    _logger.addHandler(db_handler)

    # معالج الشاشة — WARNING وما فوق فقط
    stream_handler = _ColorStreamHandler()
    stream_handler.setFormatter(logging.Formatter("%(message)s"))
    _logger.addHandler(stream_handler)

    _initialized = True
    return _logger


def get_logger() -> logging.Logger:
    """جلب المسجّل — يُنشئه إذا لم يكن مُفعّلاً"""
    global _logger
    if _logger is None:
        setup_logging()
    return _logger


# === دوال تسجيل مساعدة — تبسّط الاستخدام في السكربتات ===


def log_to(
    table: str,
    level: int = logging.INFO,
    message: str = "",
    script_name: str = "",
    func_name: str = "",
    **extra_cols: object,
) -> None:
    """تسجيل حدث في جدول محدد.

    table: اسم الجدول (system_events, user_actions, file_operations, ...)
    extra_cols: أعمدة إضافية خاصة بالجدول
    """
    logger = get_logger()
    record = logger.makeRecord(
        name="ht_sc",
        level=level,
        fn="",
        lno=0,
        msg=message,
        args=(),
        exc_info=None,
    )
    record.log_table = table
    record.script_name = script_name
    record.func_name = func_name
    record.extra_cols = extra_cols
    logger.handle(record)


def log_system(message: str, event_type: str = "", script_name: str = "main", **kw: object) -> None:
    """تسجيل حدث نظام"""
    log_to(
        "system_events",
        logging.INFO,
        message,
        script_name=script_name,
        event_type=event_type,
        **kw,
    )


def log_action(
    message: str,
    action_type: str = "input",
    prompt_text: str = "",
    user_response: str = "",
    is_valid: int = 1,
    script_name: str = "",
    func_name: str = "",
) -> None:
    """تسجيل تفاعل مستخدم"""
    log_to(
        "user_actions",
        logging.INFO,
        message,
        script_name=script_name,
        func_name=func_name,
        action_type=action_type,
        prompt_text=prompt_text,
        user_response=user_response,
        is_valid=is_valid,
    )


def log_file_op(
    message: str,
    operation: str = "",
    file_path: str = "",
    file_size_bytes: int | None = None,
    result: str = "success",
    error_reason: str = "",
    duration_seconds: float | None = None,
    command_output: str = "",
    script_name: str = "",
    func_name: str = "",
) -> None:
    """تسجيل عملية ملف"""
    log_to(
        "file_operations",
        logging.INFO,
        message,
        script_name=script_name,
        func_name=func_name,
        operation=operation,
        file_path=file_path,
        file_size_bytes=file_size_bytes,
        result=result,
        error_reason=error_reason,
        duration_seconds=duration_seconds,
        command_output=command_output,
    )


def log_quality(
    message: str,
    source_file: str = "",
    row_number: int | None = None,
    column_name: str = "",
    actual_value: str = "",
    expected_type: str = "",
    issue_type: str = "",
    script_name: str = "",
    func_name: str = "",
) -> None:
    """تسجيل مشكلة جودة بيانات"""
    log_to(
        "data_quality",
        logging.WARNING,
        message,
        script_name=script_name,
        func_name=func_name,
        source_file=source_file,
        row_number=row_number,
        column_name=column_name,
        actual_value=actual_value,
        expected_type=expected_type,
        issue_type=issue_type,
    )


def log_processing(
    message: str,
    operation_type: str = "",
    input_file: str = "",
    output_file: str = "",
    rows_read: int | None = None,
    rows_written: int | None = None,
    trips_extracted: int | None = None,
    duration_seconds: float | None = None,
    result: str = "success",
    script_name: str = "",
    func_name: str = "",
) -> None:
    """تسجيل عملية معالجة"""
    log_to(
        "processing_log",
        logging.INFO,
        message,
        script_name=script_name,
        func_name=func_name,
        operation_type=operation_type,
        input_file=input_file,
        output_file=output_file,
        rows_read=rows_read,
        rows_written=rows_written,
        trips_extracted=trips_extracted,
        duration_seconds=duration_seconds,
        result=result,
    )


def log_db_op(
    message: str,
    operation: str = "",
    target_db: str = "ht_sc.db",
    query_summary: str = "",
    rows_affected: int | None = None,
    error_sql: str = "",
    script_name: str = "",
    func_name: str = "",
) -> None:
    """تسجيل عملية على قاعدة البيانات الرئيسية"""
    log_to(
        "db_operations",
        logging.INFO,
        message,
        script_name=script_name,
        func_name=func_name,
        operation=operation,
        target_db=target_db,
        query_summary=query_summary,
        rows_affected=rows_affected,
        error_sql=error_sql,
    )


def log_error(
    message: str,
    error_type: str = "",
    traceback_full: str = "",
    line_number: int | None = None,
    context: str = "",
    script_name: str = "",
    func_name: str = "",
) -> None:
    """تسجيل خطأ"""
    log_to(
        "errors",
        logging.ERROR,
        message,
        script_name=script_name,
        func_name=func_name,
        error_type=error_type,
        traceback_full=traceback_full,
        line_number=line_number,
        context=context,
    )


def log_exception(
    message: str,
    exc: Exception | None = None,
    script_name: str = "",
    func_name: str = "",
    context: str = "",
) -> None:
    """تسجيل استثناء مع traceback تلقائي"""
    tb = traceback.format_exc()
    etype = type(exc).__name__ if exc else "Unknown"
    log_error(
        message=message,
        error_type=etype,
        traceback_full=tb,
        context=context,
        script_name=script_name,
        func_name=func_name,
    )
