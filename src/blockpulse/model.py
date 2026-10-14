"""Isolation Forest trained on the manifest's train split only. Heavy imports stay inside functions."""

import hashlib
import json
import sys
import time
import tracemalloc
from pathlib import Path

from blockpulse.dataset import load_split
from blockpulse.features import FEATURE_NAMES, FEATURE_SET_VERSION

MODEL_SCHEMA = 1
DETECTOR_FAMILY = "isolation-forest-v1"

# Ordered model inputs. Heavy-tailed counts and amounts are log-transformed (log1p) because
# Isolation Forest picks split points uniformly between a column's min and max, and one huge value
# would otherwise leave almost all rows in a single sliver. Bounded fractions and flags stay as they are.
LOG1P_COLUMNS = ("n_in", "n_out", "value_out_sat", "fee_sat", "vsize", "fee_rate_sat_vb", "max_equal_output_count")
RAW_COLUMNS = ("round_amount_fraction",)
FLAG_COLUMNS = ("rbf_signaled",)
MODEL_COLUMNS = LOG1P_COLUMNS + RAW_COLUMNS + FLAG_COLUMNS
assert set(MODEL_COLUMNS) == set(FEATURE_NAMES), "model columns must match the shared feature set"

MISSING_VALUE_POLICY = (
    "rows with a null feature (for example rbf_signaled unknown) are excluded from training and scoring, "
    "never filled with a default"
)


def feature_matrix(rows: list[dict]):
    """Return (matrix, kept_row_indexes, skipped_count). Applies the transforms in MODEL_COLUMNS order."""
    import numpy as np
    kept, vectors = [], []
    for index, row in enumerate(rows):
        features = row["features"]
        if any(features.get(name) is None for name in MODEL_COLUMNS):
            continue
        vector = [np.log1p(float(features[name])) for name in LOG1P_COLUMNS]
        vector += [float(features[name]) for name in RAW_COLUMNS]
        vector += [1.0 if features[name] else 0.0 for name in FLAG_COLUMNS]
        vectors.append(vector)
        kept.append(index)
    matrix = np.array(vectors, dtype=float).reshape(len(vectors), len(MODEL_COLUMNS))
    return matrix, kept, len(rows) - len(kept)


def unusualness(model, matrix):
    """Published score: larger means more unusual. (scikit-learn's own score_samples is larger = more normal.)"""
    return -model.score_samples(matrix)


def _peak_working_set_bytes():
    """Windows peak process memory; None elsewhere. Includes the interpreter and libraries, not just this fit."""
    if sys.platform != "win32":
        return None
    import ctypes
    from ctypes import wintypes

    class Counters(ctypes.Structure):
        _fields_ = [("cb", wintypes.DWORD), ("PageFaultCount", wintypes.DWORD), ("PeakWorkingSetSize", ctypes.c_size_t),
                    ("WorkingSetSize", ctypes.c_size_t), ("QuotaPeakPagedPoolUsage", ctypes.c_size_t),
                    ("QuotaPagedPoolUsage", ctypes.c_size_t), ("QuotaPeakNonPagedPoolUsage", ctypes.c_size_t),
                    ("QuotaNonPagedPoolUsage", ctypes.c_size_t), ("PagefileUsage", ctypes.c_size_t),
                    ("PeakPagefileUsage", ctypes.c_size_t)]
    counters = Counters()
    counters.cb = ctypes.sizeof(Counters)
    kernel32, psapi = ctypes.WinDLL("kernel32"), ctypes.WinDLL("psapi")
    kernel32.GetCurrentProcess.restype = wintypes.HANDLE
    psapi.GetProcessMemoryInfo.argtypes = [wintypes.HANDLE, ctypes.POINTER(Counters), wintypes.DWORD]
    psapi.GetProcessMemoryInfo.restype = wintypes.BOOL
    if not psapi.GetProcessMemoryInfo(kernel32.GetCurrentProcess(), ctypes.byref(counters), counters.cb):
        return None
    return int(counters.PeakWorkingSetSize)


def _sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def train_isolation_forest(manifest: dict, root: Path, output: Path, *, seed: int = 7, n_estimators: int = 100,
                           max_samples: int = 256, max_train_rows: int = 50_000) -> dict:
    """Fit on the train split only, save the model plus a metadata file, and return the metadata."""
    import joblib
    import numpy as np
    import scipy
    import sklearn
    from sklearn.ensemble import IsolationForest

    rows = load_split(manifest, "train", root)
    matrix, _, skipped = feature_matrix(rows)
    if len(matrix) < 100:
        raise ValueError(f"Only {len(matrix)} usable training rows; need at least 100")
    sampled = False
    if len(matrix) > max_train_rows:  # deterministic bound on training size
        chosen = np.random.default_rng(seed).choice(len(matrix), size=max_train_rows, replace=False)
        matrix, sampled = matrix[np.sort(chosen)], True
    output.mkdir(parents=True, exist_ok=False)

    parameters = {"n_estimators": n_estimators, "max_samples": min(max_samples, len(matrix)),
                  "contamination": "auto", "random_state": seed}
    tracemalloc.start()
    started = time.perf_counter()
    model = IsolationForest(**parameters).fit(matrix)
    fit_seconds = time.perf_counter() - started
    _, traced_peak = tracemalloc.get_traced_memory()
    tracemalloc.stop()

    scores = unusualness(model, matrix)
    repeat = IsolationForest(**parameters).fit(matrix)  # same seed and data must give identical scores
    deterministic = bool(np.array_equal(scores, unusualness(repeat, matrix)))

    model_path = output / "model.joblib"
    joblib.dump(model, model_path)
    percentiles = {f"p{p}": round(float(np.percentile(scores, p)), 6) for p in (1, 50, 90, 95, 99, 100)}
    metadata = {
        "schema_version": MODEL_SCHEMA, "detector_family": DETECTOR_FAMILY,
        "dataset_manifest_id": manifest["manifest_id"], "train_sessions": manifest["splits"]["train"]["sessions"],
        "feature_set_version": FEATURE_SET_VERSION, "model_columns": list(MODEL_COLUMNS),
        "transforms": {"log1p": list(LOG1P_COLUMNS), "raw": list(RAW_COLUMNS), "boolean_to_0_1": list(FLAG_COLUMNS)},
        "missing_value_policy": MISSING_VALUE_POLICY, "score_direction": "larger = more unusual (negated score_samples)",
        "parameters": parameters, "seed": seed,
        "training": {"rows_in_split": len(rows), "rows_skipped_missing_values": skipped, "rows_used": len(matrix),
                     "subsampled_to_max_train_rows": sampled},
        "resources": {"fit_seconds": round(fit_seconds, 4), "python_peak_alloc_bytes": traced_peak,
                      "process_peak_working_set_bytes": _peak_working_set_bytes(),
                      "note": "peak alloc covers Python/NumPy allocations during the fit; working set covers the whole process"},
        "train_score_percentiles": percentiles, "deterministic_refit_identical": deterministic,
        "environment": {"python": sys.version.split()[0], "scikit_learn": sklearn.__version__,
                        "numpy": np.__version__, "scipy": scipy.__version__},
        "model_sha256": _sha256(model_path),
        "limits": ["No threshold is set; M3.3 calibrates one on the calibration split.",
                   "Training scores are in-sample and say nothing about detection quality.",
                   "Trained on a few short capture windows without labels."],
    }
    (output / "metadata.json").write_text(json.dumps(metadata, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    return metadata


def load_model(directory: Path):
    """Load a saved model after checking it against its recorded checksum and feature schema."""
    import joblib
    metadata = json.loads((directory / "metadata.json").read_text(encoding="utf-8"))
    if _sha256(directory / "model.joblib") != metadata["model_sha256"]:
        raise ValueError("model.joblib does not match the checksum in metadata.json")
    if metadata["feature_set_version"] != FEATURE_SET_VERSION or metadata["model_columns"] != list(MODEL_COLUMNS):
        raise ValueError("model was trained on a different feature schema than this code uses")
    return joblib.load(directory / "model.joblib"), metadata
