# infer_pyspark_schema.py
from pyspark.sql.types import (
    StructType, StructField, StringType, LongType, DoubleType,
    BooleanType, TimestampType, ArrayType, DataType
)
import json
import re
from typing import Any, Dict, List

# Simple ISO8601-ish time recognition (covers common forms, including time zones and fractional seconds)
ISO8601_RE = re.compile(
    r"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(?:\.\d+)?(?:Z|[+-]\d{2}:?\d{2})?$"
)

def is_iso8601(s: str) -> bool:
    return isinstance(s, str) and bool(ISO8601_RE.match(s))

def infer_type_from_value(v: Any) -> Dict:
    """
    Returns a dict describing: {"dtype": pyspark DataType instance, "nullable": bool}
    For arrays/structs, dtype can be ArrayType or StructType.
    """
    # None -> unknown, choose StringType nullable
    if v is None:
        return {"dtype": StringType(), "nullable": True}

    # dict -> struct
    if isinstance(v, dict):
        fields = []
        for k, val in v.items():
            info = infer_type_from_value(val)
            fields.append(StructField(k, info["dtype"], info["nullable"]))
        return {"dtype": StructType(fields), "nullable": False}

    # list -> array: infer element types and try to unify
    if isinstance(v, list):
        if len(v) == 0:
            # empty array -> array<string>
            return {"dtype": ArrayType(StringType(), containsNull=True), "nullable": False}
        # infer each element type
        elem_infos = [infer_type_from_value(e) for e in v]
        # if all element dtypes are StructType -> merge struct fields
        elem_types = [ei["dtype"] for ei in elem_infos]
        # Helper: unify element type
        unified_elem = unify_types(elem_types)
        # element nullable = any element's nullable True OR unified containsNull
        contains_null = any(ei["nullable"] for ei in elem_infos)
        return {"dtype": ArrayType(unified_elem, containsNull=contains_null), "nullable": False}

    # primitive types
    if isinstance(v, bool):
        return {"dtype": BooleanType(), "nullable": False}
    if isinstance(v, int):
        return {"dtype": LongType(), "nullable": False}
    if isinstance(v, float):
        return {"dtype": DoubleType(), "nullable": False}
    if isinstance(v, str):
        if is_iso8601(v):
            return {"dtype": TimestampType(), "nullable": False}
        return {"dtype": StringType(), "nullable": False}

    # fallback
    return {"dtype": StringType(), "nullable": True}


def unify_types(dtypes: List[DataType]) -> DataType:
    """
    Merge multiple pyspark DataType into a unified DataType.
    Strategy (simplified):
      - If all are StructType -> merge fields (conflicting fields fallback to StringType)
      - If contains DoubleType then choose DoubleType (long->double)
      - If contains LongType and no DoubleType -> LongType
      - Otherwise choose StringType
    """
    # all structs?
    if all(isinstance(t, StructType) for t in dtypes):
        # merge fields by name
        merged: Dict[str, StructField] = {}
        for st in dtypes:
            for f in st.fields:
                if f.name in merged:
                    # if same name but different type, choose a promoted type
                    existing = merged[f.name].dataType
                    merged[f.name] = StructField(f.name, _promote_two_types(existing, f.dataType), merged[f.name].nullable or f.nullable)
                else:
                    merged[f.name] = StructField(f.name, f.dataType, f.nullable)
        return StructType(list(merged.values()))

    # if any DoubleType -> DoubleType
    if any(isinstance(t, DoubleType) for t in dtypes):
        return DoubleType()
    # if any LongType and no DoubleType -> LongType
    if any(isinstance(t, LongType) for t in dtypes):
        return LongType()
    # if any BooleanType and all are BooleanType -> BooleanType
    if all(isinstance(t, BooleanType) for t in dtypes):
        return BooleanType()
    # fallback to string
    return StringType()

def _promote_two_types(t1: DataType, t2: DataType) -> DataType:
    """
    Simple promotion logic for two types in case of Struct field conflicts.
    Priority: Double > Long > Timestamp > Boolean > String
    """
    from pyspark.sql.types import DoubleType, LongType, TimestampType, BooleanType, StringType, StructType, ArrayType

    priority = {DoubleType: 5, LongType: 4, TimestampType: 3, BooleanType: 2, StringType: 1}
    # map actual class to priority
    def p(t):
        for cls, pr in priority.items():
            if isinstance(t, cls):
                return pr
        # arrays/structs get highest fallback
        if isinstance(t, (StructType, ArrayType)):
            return 6
        return 0

    return t1 if p(t1) >= p(t2) else t2


def infer_schema_from_json_file(path: str) -> StructType:
    """
    Read JSON file from <path>, return pyspark StructType.
    """
    with open(path, "r", encoding="utf-8") as f:
        obj = json.load(f)

    if isinstance(obj, list):
        if len(obj) == 0:
            return StructType([])
        obj = obj[0]  # only use the first record
    if not isinstance(obj, dict):
        raise ValueError("Input JSON must be an object or an array of objects")

    fields = []
    for k, v in obj.items():
        info = infer_type_from_value(v)
        fields.append(StructField(k, info["dtype"], info["nullable"]))
    return StructType(fields)


if __name__ == "__main__":
    # sample json file path
    example_path = "sample_data/business_sample_data.json"

    try:
        schema = infer_schema_from_json_file(example_path)
        print(f"Here is the inferred schema from json file: {example_path}\n")
        print(schema)                 # detailed representation
    except Exception as e:
        print("Error inferring schema:", e)
