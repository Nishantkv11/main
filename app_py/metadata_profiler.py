import csv
from collections import defaultdict
from datetime import datetime


class MetadataProfiler:
    """
    Profiles metadata for a delimited text file.

    What it captures:
    - number of columns
    - column names
    - configured/expected data type (if provided), else "string"
    - min value per column
    - max value per column
    - total nulls per column
    - total unique values per column
    - total number of rows
    - metadata mismatch details

    Supported expected_columns formats:
    1) ["col1", "col2", "col3"]
    2) [{"name": "col1", "type": "int"}, {"name": "col2", "type": "datetime"}]

    Notes:
    - For delimited text files, actual parsed values are still treated as text.
    - If expected column definitions include types, those are reported as
      expected_data_type while actual_profiled_data_type remains "string".
    """

    def __init__(self, input_file: str, delimiter: str = "\t", expected_columns=None):
        self.input_file = input_file
        self.delimiter = delimiter
        self.expected_columns = expected_columns or []

        self.expected_column_names = self._normalize_expected_column_names(self.expected_columns)
        self.expected_column_defs = self._normalize_expected_column_defs(self.expected_columns)

    @staticmethod
    def _is_null(value: str) -> bool:
        """
        Treat common empty/null-like values as null.
        """
        if value is None:
            return True
        v = str(value).strip().lower()
        return v in ("", "null", "none", "na", "n/a")

    @staticmethod
    def _normalize_expected_column_names(expected_columns):
        names = []
        for col in expected_columns or []:
            if isinstance(col, str):
                names.append(col)
            elif isinstance(col, dict) and col.get("name"):
                names.append(col["name"])
        return names

    @staticmethod
    def _normalize_expected_column_defs(expected_columns):
        defs = {}
        for col in expected_columns or []:
            if isinstance(col, str):
                defs[col] = {"name": col, "type": "string"}
            elif isinstance(col, dict) and col.get("name"):
                defs[col["name"]] = {
                    "name": col["name"],
                    "type": col.get("type", "string")
                }
        return defs

    def profile(self, source_bucket: str, source_key: str) -> dict:
        """
        Read the file and generate metadata profile as a dictionary.
        """
        with open(self.input_file, "r", encoding="utf-8-sig", newline="") as f:
            reader = csv.DictReader(f, delimiter=self.delimiter)

            fieldnames = reader.fieldnames or []
            total_columns = len(fieldnames)

            null_counts = defaultdict(int)
            unique_values = {col: set() for col in fieldnames}
            min_values = {col: None for col in fieldnames}
            max_values = {col: None for col in fieldnames}

            total_rows = 0

            for row in reader:
                total_rows += 1

                for col in fieldnames:
                    val = row.get(col, "")
                    val_str = "" if val is None else str(val).strip()

                    if self._is_null(val_str):
                        null_counts[col] += 1
                    else:
                        unique_values[col].add(val_str)

                        if min_values[col] is None or val_str < min_values[col]:
                            min_values[col] = val_str

                        if max_values[col] is None or val_str > max_values[col]:
                            max_values[col] = val_str

            mismatches = []

            if self.expected_column_names:
                missing_cols = [c for c in self.expected_column_names if c not in fieldnames]
                extra_cols = [c for c in fieldnames if c not in self.expected_column_names]

                if missing_cols:
                    mismatches.append({
                        "type": "missing_columns",
                        "columns": missing_cols
                    })

                if extra_cols:
                    mismatches.append({
                        "type": "extra_columns",
                        "columns": extra_cols
                    })

                if len(fieldnames) != len(self.expected_column_names):
                    mismatches.append({
                        "type": "column_count_mismatch",
                        "expected": len(self.expected_column_names),
                        "actual": len(fieldnames)
                    })

                if fieldnames != self.expected_column_names:
                    mismatches.append({
                        "type": "column_order_mismatch",
                        "expected_order": self.expected_column_names,
                        "actual_order": fieldnames
                    })

            column_profiles = []
            for col in fieldnames:
                expected_def = self.expected_column_defs.get(col, {"name": col, "type": "string"})

                column_profiles.append({
                    "column_name": col,
                    "actual_profiled_data_type": "string",
                    "expected_data_type": expected_def.get("type", "string"),
                    "min_value": min_values[col],
                    "max_value": max_values[col],
                    "total_nulls": null_counts[col],
                    "total_unique_values": len(unique_values[col])
                })

            return {
                "source_bucket": source_bucket,
                "source_key": source_key,
                "profiled_at_utc": datetime.utcnow().isoformat(),
                "file_type": "delimited_text",
                "delimiter": self.delimiter,
                "number_of_columns": total_columns,
                "column_names": fieldnames,
                "total_rows": total_rows,
                "metadata_mismatch": len(mismatches) > 0,
                "metadata_mismatch_details": mismatches,
                "columns": column_profiles
            }
