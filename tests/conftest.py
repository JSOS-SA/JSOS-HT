"""
إعداد مسار الاستيراد للاختبارات
يُضاف مجلد scripts تلقائياً حتى تعمل الاستيرادات
"""
import sys
import os

# مسار مجلد السكربتات بالنسبة لمجلد الاختبارات
_SCRIPTS = os.path.join(os.path.dirname(__file__), '..', 'scripts')
_SCRIPTS = os.path.abspath(_SCRIPTS)

if _SCRIPTS not in sys.path:
    sys.path.insert(0, _SCRIPTS)
