"""Quick diagnostic of preprocessing on Gently2/ce475ad5/embryo_3 t=1."""
from pathlib import Path
import numpy as np

from annotator.volume_io import load_volume, preprocess, normalize_for_3d

p = Path(r"D:\Kesavan\latest\Kesavan\Gently2\volumes\ce475ad5\embryo_3_t0001.tif")
print(f"file: {p}")
print(f"exists: {p.exists()}")
v = load_volume(p)
print(f"raw  shape={v.shape} dtype={v.dtype} min={v.min()} max={v.max()} mean={float(v.mean()):.2f}")
print()

pp = preprocess(v)
print(f"preprocess (left half, -100 clip): shape={pp.shape} dtype={pp.dtype}")
print(f"  min={pp.min()} max={pp.max()} mean={float(pp.mean()):.2f}")
edges = [0, 1, 5, 10, 20, 50, 100, 200, 500, 1000, 5000]
flat = pp.reshape(-1)
total = flat.size
print("  intensity histogram (post bg-subtract):")
for i in range(len(edges) - 1):
    c = int(((flat >= edges[i]) & (flat < edges[i+1])).sum())
    pct = 100 * c / total
    print(f"    [{edges[i]:>4d}, {edges[i+1]:>4d}): {c:>10d} ({pct:5.1f}%)")
print()

# percentile values used by normalize_for_3d
from annotator.volume_io import _signal_percentile
p1, p99 = _signal_percentile(pp.astype(np.float32), (1.0, 99.0))
print(f"sampled percentile_1={p1:.2f}  percentile_99={p99:.2f}")
print()

u8 = normalize_for_3d(pp)
print(f"normalize_for_3d uint8: shape={u8.shape} min={u8.min()} max={u8.max()} mean={float(u8.mean()):.2f}")
print("  uint8 histogram (post percentile stretch + Z-blur):")
edges8 = [0, 10, 20, 30, 50, 76, 100, 150, 200, 256]
flat8 = u8.reshape(-1)
for i in range(len(edges8) - 1):
    c = int(((flat8 >= edges8[i]) & (flat8 < edges8[i+1])).sum())
    pct = 100 * c / total
    print(f"    [{edges8[i]:>4d}, {edges8[i+1]:>4d}): {c:>10d} ({pct:5.1f}%)")
