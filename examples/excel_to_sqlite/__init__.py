"""
Excel to SQLite ETL Module exports for testing and usage.
"""

import importlib.util
from pathlib import Path

_CURRENT_DIR = Path(__file__).resolve().parent

# Load 02_excel_to_sqlite_pipeline.py dynamically
_spec_std = importlib.util.spec_from_file_location(
    "excel_to_sqlite_std", _CURRENT_DIR / "02_excel_to_sqlite_pipeline.py"
)
_mod_std = importlib.util.module_from_spec(_spec_std)
_spec_std.loader.exec_module(_mod_std)

# Load 02_parallel_excel_to_sqlite.py dynamically
_spec_par = importlib.util.spec_from_file_location(
    "excel_to_sqlite_par", _CURRENT_DIR / "02_parallel_excel_to_sqlite.py"
)
_mod_par = importlib.util.module_from_spec(_spec_par)
_spec_par.loader.exec_module(_mod_par)

# Export functions
clean_currency = _mod_std.clean_currency
clean_discount = _mod_std.clean_discount
normalize_country = _mod_std.normalize_country
parse_date_to_iso = _mod_std.parse_date_to_iso
transform_data = _mod_std.transform_data
load_into_sqlite = _mod_std.load_into_sqlite
verify_pipeline = _mod_std.verify_pipeline

transform_chunk = _mod_par.transform_chunk
initialize_high_performance_db = _mod_par.initialize_high_performance_db
