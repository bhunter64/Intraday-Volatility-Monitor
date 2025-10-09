import argparse, json, pathlib, yaml
from ivtool.pipeline import stream_prices
from ivtool.detectors import make

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--config", required=True)
    args = ap.parse_args()

    cfg = yaml.safe_load(open(args.config))
    det = make(cfg["detector"]["name"], **cfg["detector"].get("params", {}))

    out = []
    for t, x in stream_prices(cfg["data"]):
        if det.update(x):
            out.append({"timestamp": t, "event": "alarm", "state": det.state})

    outdir = pathlib.Path(cfg["output_dir"])
    outdir.mkdir(parents=True, exist_ok=True)
    with open(outdir / "events.json", "w") as f:
        json.dump(out, f, indent=2)

if __name__ == "__main__":
    main()
