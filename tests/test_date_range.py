import ast
import unittest
from datetime import datetime, timedelta
from pathlib import Path
from unittest.mock import Mock


class DateRangeTests(unittest.TestCase):
    def test_ten_day_range_includes_last_second_across_year_boundary(self):
        # run.py configures stdout and logging on import; isolate its pure function.
        tree = ast.parse(Path(__file__).resolve().parents[1].joinpath('run.py').read_text(encoding='utf-8'))
        function = next(node for node in tree.body if isinstance(node, ast.FunctionDef) and node.name == 'calc_date_range')
        function.returns = None
        clock = Mock()
        clock.now.return_value = datetime(2026, 12, 25, 12, 30)
        scope = {'datetime': clock, 'timedelta': timedelta}
        exec(compile(ast.Module(body=[function], type_ignores=[]), 'run.py', 'exec'), scope)
        start, end = scope['calc_date_range']()
        self.assertEqual(start, datetime(2026, 12, 25))
        self.assertEqual(end, datetime(2027, 1, 4, 23, 59, 59, 999999))
