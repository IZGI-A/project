"""Base validator classes for data validation."""
from dataclasses import dataclass, field


@dataclass
class ValidationResult:
    """Result of validating a single row."""
    row_number: int
    is_valid: bool = True
    errors: list = field(default_factory=list)

    def add_error(self, field_name, error_type, error_message, raw_value=None):
        self.is_valid = False
        self.errors.append({
            'row_number': self.row_number,
            'field_name': field_name,
            'error_type': error_type,
            'error_message': error_message,
            'raw_value': str(raw_value) if raw_value is not None else None,
        })


@dataclass
class BatchValidationResult:
    """Result of validating an entire batch."""
    total_rows: int = 0
    valid_rows: int = 0
    error_count: int = 0
    errors: list = field(default_factory=list)
    valid_records: list = field(default_factory=list)

    def add_row_result(self, result: ValidationResult, record: dict):
        self.total_rows += 1
        if result.is_valid:
            self.valid_rows += 1
            self.valid_records.append(record)
        else:
            self.error_count += len(result.errors)
            self.errors.extend(result.errors)

    @property
    def error_rate(self):
        if self.total_rows == 0:
            return 0.0
        return (self.total_rows - self.valid_rows) / self.total_rows

    def get_error_summary(self):
        summary = {}
        for err in self.errors:
            key = f"{err['field_name']}:{err['error_type']}"
            summary[key] = summary.get(key, 0) + 1
        return summary


class BaseValidator:
    """Base class for field validators."""

    def validate_required(self, result: ValidationResult, row: dict,
                          field_name: str, file_type: str):
        value = row.get(field_name, '').strip()
        if not value:
            result.add_error(
                field_name, 'REQUIRED',
                f'{field_name} is required',
                raw_value=row.get(field_name),
            )
            return False
        return True

    def validate_integer(self, result: ValidationResult, row: dict,
                         field_name: str, min_val=None, max_val=None):
        value = row.get(field_name, '').strip()
        if not value:
            return True
        try:
            int_val = int(value)
            if min_val is not None and int_val < min_val:
                result.add_error(
                    field_name, 'RANGE',
                    f'{field_name} must be >= {min_val}, got {int_val}',
                    raw_value=value,
                )
                return False
            if max_val is not None and int_val > max_val:
                result.add_error(
                    field_name, 'RANGE',
                    f'{field_name} must be <= {max_val}, got {int_val}',
                    raw_value=value,
                )
                return False
            return True
        except ValueError:
            result.add_error(
                field_name, 'TYPE',
                f'{field_name} must be an integer, got: {value}',
                raw_value=value,
            )
            return False

    def validate_decimal(self, result: ValidationResult, row: dict,
                         field_name: str, min_val=None):
        value = row.get(field_name, '').strip()
        if not value:
            return True
        try:
            float_val = float(value)
            if min_val is not None and float_val < min_val:
                result.add_error(
                    field_name, 'RANGE',
                    f'{field_name} must be >= {min_val}, got {float_val}',
                    raw_value=value,
                )
                return False
            return True
        except ValueError:
            result.add_error(
                field_name, 'TYPE',
                f'{field_name} must be a number, got: {value}',
                raw_value=value,
            )
            return False

    def validate_date(self, result: ValidationResult, row: dict, field_name: str):
        value = row.get(field_name, '').strip()
        if not value:
            return True
        # Accept YYYYMMDD or YYYY-MM-DD
        clean = value.replace('-', '')
        if len(clean) != 8 or not clean.isdigit():
            result.add_error(
                field_name, 'FORMAT',
                f'{field_name} must be YYYYMMDD or YYYY-MM-DD, got: {value}',
                raw_value=value,
            )
            return False
        year, month, day = int(clean[:4]), int(clean[4:6]), int(clean[6:8])
        if not (1900 <= year <= 2100 and 1 <= month <= 12 and 1 <= day <= 31):
            result.add_error(
                field_name, 'FORMAT',
                f'{field_name} has invalid date components: {value}',
                raw_value=value,
            )
            return False
        return True

    def validate_in_set(self, result: ValidationResult, row: dict,
                        field_name: str, valid_values: set):
        value = row.get(field_name, '').strip()
        if not value:
            return True
        if value not in valid_values:
            result.add_error(
                field_name, 'VALUE',
                f'{field_name} must be one of {valid_values}, got: {value}',
                raw_value=value,
            )
            return False
        return True
