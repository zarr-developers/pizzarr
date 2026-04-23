"""Benchmark xarray read/write performance on zarr stores.

Called by benchmark-zarrs-vs-xarray.R with subcommands:

  python benchmark-xarray.py generate <work_dir> <configs_json>
  python benchmark-xarray.py bench   <work_dir> <configs_json> <n_iter>

generate: creates V2 zarr stores with numpy random data.
bench:    times xarray read/write operations and prints JSON results to stdout.

Prerequisites: zarr>=3, xarray, numpy, numcodecs
"""

import json
import os
import sys
import time

import numpy as np
import numcodecs
import zarr
import xarray as xr


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def make_compressor(codec):
    if codec == "gzip":
        return numcodecs.GZip(level=6)
    return None


def get_regions(cfg):
    """Return (chunk_sel, subset_sel) as tuples of Python slices (0-based)."""
    shape = cfg["shape"]
    chunks = cfg["chunks"]
    chunk_sel = tuple(slice(0, c) for c in chunks)
    if shape == [100, 100, 100]:
        subset_sel = (slice(25, 75), slice(25, 75), slice(25, 75))
    else:
        subset_sel = (slice(50, 250), slice(50, 250), slice(10, 60))
    return chunk_sel, subset_sel


def subset_size_mb(sel):
    """Uncompressed size in MB of a float64 region defined by slices."""
    n = 1
    for s in sel:
        n *= (s.stop - s.start)
    return n * 8 / 1e6


# ---------------------------------------------------------------------------
# generate subcommand
# ---------------------------------------------------------------------------

def cmd_generate(work_dir, configs):
    for cfg in configs:
        path = os.path.join(work_dir, f"data_{cfg['name']}.zarr")
        if os.path.exists(path):
            print(f"  Store exists, skipping: {path}", file=sys.stderr)
            continue

        shape = tuple(cfg["shape"])
        chunks = tuple(cfg["chunks"])
        compressor = make_compressor(cfg["codec"])

        rng = np.random.default_rng(42)
        data = rng.random(shape)

        root = zarr.open_group(path, mode="w", zarr_format=2)
        arr = root.create_array(
            "data",
            shape=shape,
            chunks=chunks,
            dtype="float64",
            fill_value=0.0,
            compressor=compressor,
        )
        arr[:] = data
        arr.update_attributes({"_ARRAY_DIMENSIONS": ["x", "y", "z"]})
        size_mb = np.prod(shape) * 8 / 1e6
        print(f"  Generated {cfg['name']}: {path} ({size_mb:.1f} MB)",
              file=sys.stderr)


# ---------------------------------------------------------------------------
# bench subcommand
# ---------------------------------------------------------------------------

def cmd_bench(work_dir, configs, n_iter):
    results = []

    for cfg in configs:
        path = os.path.join(work_dir, f"data_{cfg['name']}.zarr")
        shape = tuple(cfg["shape"])
        size_mb = np.prod(shape) * 8 / 1e6
        chunk_sel, subset_sel = get_regions(cfg)
        chunk_mb = subset_size_mb(chunk_sel)
        sub_mb = subset_size_mb(subset_sel)

        print(f"  xarray benchmarking {cfg['name']} ...", file=sys.stderr)

        # -- read_all --
        for i in range(n_iter):
            t0 = time.perf_counter()
            ds = xr.open_zarr(path, consolidated=False)
            _ = ds["data"].values  # force full load
            ds.close()
            elapsed = time.perf_counter() - t0
            results.append({
                "scenario": "read_all",
                "array_config": cfg["name"],
                "codec": cfg["codec"],
                "implementation": "xarray",
                "iteration": i + 1,
                "time_seconds": round(elapsed, 6),
                "throughput_mb_s": round(size_mb / elapsed, 2),
            })

        # -- read_chunk --
        for i in range(n_iter):
            t0 = time.perf_counter()
            ds = xr.open_zarr(path, consolidated=False)
            _ = ds["data"][chunk_sel].values
            ds.close()
            elapsed = time.perf_counter() - t0
            results.append({
                "scenario": "read_chunk",
                "array_config": cfg["name"],
                "codec": cfg["codec"],
                "implementation": "xarray",
                "iteration": i + 1,
                "time_seconds": round(elapsed, 6),
                "throughput_mb_s": round(chunk_mb / elapsed, 2),
            })

        # -- read_subset --
        for i in range(n_iter):
            t0 = time.perf_counter()
            ds = xr.open_zarr(path, consolidated=False)
            _ = ds["data"][subset_sel].values
            ds.close()
            elapsed = time.perf_counter() - t0
            results.append({
                "scenario": "read_subset",
                "array_config": cfg["name"],
                "codec": cfg["codec"],
                "implementation": "xarray",
                "iteration": i + 1,
                "time_seconds": round(elapsed, 6),
                "throughput_mb_s": round(sub_mb / elapsed, 2),
            })

        # -- write_all --
        rng = np.random.default_rng(42)
        write_data = rng.random(shape)
        ds_write = xr.Dataset({"data": (["x", "y", "z"], write_data)})
        encoding = {"data": {"chunks": tuple(cfg["chunks"])}}
        if cfg["codec"] == "gzip":
            encoding["data"]["compressor"] = numcodecs.GZip(level=6)
        elif cfg["codec"] == "none":
            encoding["data"]["compressor"] = None

        for i in range(n_iter):
            out_path = os.path.join(
                work_dir, f"write_xarray_{cfg['name']}_{i}.zarr"
            )
            t0 = time.perf_counter()
            ds_write.to_zarr(out_path, mode="w", encoding=encoding,
                             zarr_format=2)
            elapsed = time.perf_counter() - t0
            results.append({
                "scenario": "write_all",
                "array_config": cfg["name"],
                "codec": cfg["codec"],
                "implementation": "xarray",
                "iteration": i + 1,
                "time_seconds": round(elapsed, 6),
                "throughput_mb_s": round(size_mb / elapsed, 2),
            })

    # JSON results to stdout; everything else went to stderr
    print(json.dumps(results))


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    if len(sys.argv) < 3:
        print("Usage: benchmark-xarray.py <generate|bench> <work_dir> "
              "<configs_json> [n_iter]", file=sys.stderr)
        sys.exit(1)

    subcmd = sys.argv[1]
    work_dir = sys.argv[2]
    configs = json.loads(sys.argv[3])

    if subcmd == "generate":
        cmd_generate(work_dir, configs)
    elif subcmd == "bench":
        n_iter = int(sys.argv[4]) if len(sys.argv) > 4 else 3
        cmd_bench(work_dir, configs, n_iter)
    else:
        print(f"Unknown subcommand: {subcmd}", file=sys.stderr)
        sys.exit(1)
