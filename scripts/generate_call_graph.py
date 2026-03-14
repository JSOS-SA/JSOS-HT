"""أداة توليد خريطة الاستدعاءات.

تحليل الكود لاستخراج الدوال ومعاملاتها والعلاقات بينها (من يستدعي من)
"""

import ast
import os
import sys

# ضمان إمكانية الاستيراد
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
SCRIPTS_DIR = os.path.join(BASE_DIR, "scripts")
if SCRIPTS_DIR not in sys.path:
    sys.path.insert(0, SCRIPTS_DIR)

from common import print_err, print_header, print_ok, print_warn


class CallGraphVisitor(ast.NodeVisitor):
    """زائر لشجرة الكود (AST) يقوم بجمع تعريفات الدوال واستدعاءاتها"""

    def __init__(self) -> None:
        """تهيئة الزائر."""
        self.functions = {}
        self.current_func = None
        self.current_file = ""

    def visit_FunctionDef(self, node) -> None:
        """معالجة تعريف دالة متزامنة."""
        self._process_function(node, "Sync")

    def visit_AsyncFunctionDef(self, node) -> None:
        """معالجة تعريف دالة غير متزامنة."""
        self._process_function(node, "Async")

    def _process_function(self, node, func_type) -> None:
        func_name = node.name

        # 1. استخراج المعاملات (Arguments)
        # ast.unparse تعيد الكود المصدري من العقدة (متاح في بايثون 3.9+)
        try:
            args_str = ast.unparse(node.args)
        except AttributeError:
            args_str = "(غير مدعوم في هذا الإصدار)"
        except Exception:
            args_str = "..."

        # مفتاح فريد للدالة (اسم الملف + اسم الدالة) لتجنب تشابه الأسماء
        key = f"{func_name} ({self.current_file})"

        self.functions[key] = {
            "name": func_name,
            "args": args_str,
            "type": func_type,
            "file": self.current_file,
            "line": node.lineno,
            "calls": set(),  # هنا سنخزن الدوال التي تستدعيها هذه الدالة
        }

        # تعيين الدالة الحالية للدخول في تفاصيلها
        prev_func = self.current_func
        self.current_func = key

        # زيارة محتوى الدالة للبحث عن Call nodes
        self.generic_visit(node)

        # استعادة الدالة السابقة (للتعشيش)
        self.current_func = prev_func

    def visit_Call(self, node) -> None:
        """يتم استدعاؤها عند العثور على استدعاء دالة داخل الكود"""
        if self.current_func:
            # محاولة معرفة اسم الدالة المستدعاة
            called_name = self._get_func_name(node.func)
            if called_name:
                self.functions[self.current_func]["calls"].add(called_name)

        # الاستمرار في الزيارة (قد يكون هناك استدعاء داخل استدعاء)
        self.generic_visit(node)

    def _get_func_name(self, node):
        """مساعد لاستخراج اسم الدالة من عقدة الاستدعاء"""
        try:
            # إذا كان اسماً مباشراً: print()
            if isinstance(node, ast.Name):
                return node.id
            # إذا كان خاصية: os.path.join()
            if isinstance(node, ast.Attribute):
                return ast.unparse(node)
        except (AttributeError, ValueError):
            pass
        return None


def generate_report(root_dir):
    """توليد تقرير خريطة الاستدعاءات."""
    visitor = CallGraphVisitor()

    print_warn("جارٍ مسح الملفات وتحليل الكود...")

    for root, dirs, files in os.walk(root_dir):
        # تجاهل المجلدات غير المهمة
        dirs[:] = [d for d in dirs if d not in [".git", "__pycache__", "venv", "node_modules", ".claude"]]

        for file in files:
            if file.endswith(".py"):
                full_path = os.path.join(root, file)
                rel_path = os.path.relpath(full_path, root_dir)

                visitor.current_file = rel_path

                try:
                    with open(full_path, encoding="utf-8") as f:
                        tree = ast.parse(f.read(), filename=full_path)
                    visitor.visit(tree)
                except Exception as e:
                    print_err(f"خطأ في تحليل {rel_path}: {e}")

    # كتابة التقرير
    output_file = os.path.join(root_dir, "call_graph_report.txt")
    with open(output_file, "w", encoding="utf-8") as f:
        f.write("=== تقرير خريطة الاستدعاءات (Call Graph) ===\n")
        f.write(f"المسار: {root_dir}\n\n")

        # ترتيب الدوال حسب الملف
        sorted_funcs = sorted(visitor.functions.values(), key=lambda x: (x["file"], x["line"]))

        for data in sorted_funcs:
            # العنوان: اسم الدالة (المعاملات)
            f.write(f"📍 الدالة: {data['name']}({data['args']})\n")
            f.write(f"   الموقع: {data['file']}:{data['line']} [{data['type']}]\n")

            if data["calls"]:
                f.write("   📞 تستدعي:\n")
                f.writelines(f"      -> {call}\n" for call in sorted(data["calls"]))
            else:
                f.write("   ⏹️  (لا تستدعي دوال خارجية)\n")

            f.write("-" * 60 + "\n")

    return output_file, len(visitor.functions)


def run() -> str:
    """تشغيل تحليل خريطة الاستدعاءات."""
    print_header("تحليل خريطة الاستدعاءات")

    report_path, count = generate_report(BASE_DIR)

    print_ok(f"\nتم تحليل {count} دالة بنجاح.")
    print_ok(f"تم حفظ التقرير التفصيلي في:\n{report_path}")

    # عرض مقتطف سريع
    print_header("مقتطف من التقرير")
    if os.path.exists(report_path):
        with open(report_path, encoding="utf-8") as f:
            # عرض أول 20 سطر
            for _ in range(20):
                line = f.readline()
                if not line:
                    break
                print(line.strip())

    return "back"


if __name__ == "__main__":
    run()
