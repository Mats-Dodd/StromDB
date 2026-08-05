"""Load and expose a benchmark suite JSON (see suites/write-throughput.json)."""
import json


class Suite:
    def __init__(self, data):
        self._d = data

    @classmethod
    def load(cls, path):
        with open(path) as f:
            return cls(json.load(f))

    @property
    def name(self):            return self._d["suite"]
    @property
    def workload(self):        return self._d.get("workload", "write-throughput")
    @property
    def modes(self):           return list(self._d["modes"])
    @property
    def stream_counts(self):   return list(self._d["stream_counts"])
    @property
    def cluster(self):         return dict(self._d["cluster"])
    @property
    def saturation(self):      return dict(self._d.get("saturation", {}))
    @property
    def sustained(self):       return dict(self._d.get("sustained", {}))
    @property
    def catchup(self):         return dict(self._d.get("catchup", {}))
    @property
    def reads(self):           return dict(self._d.get("reads", {}))
    @property
    def mixed(self):           return dict(self._d.get("mixed", {}))

    def ladder_for(self, stream_count):
        ladder = self._d["pod_ladder"]
        key = str(stream_count)
        if key not in ladder:
            raise KeyError(f"no pod_ladder entry for stream_count {stream_count}")
        return list(ladder[key])

    def configs_for(self, mode):
        """Server-config variants for a mode (the sweep axis for finding the
        optimal server config). Each is {"label", "args"} where args are extra
        server flags (e.g. "--tail-cache-bytes 65536"). A mode with no
        `server_configs` entry has one implicit baseline config = the mode name
        with empty args. Distinct labels get their own results/<label>/cells.json,
        so variants (e.g. wal vs wal-tailcache) appear side by side in the report."""
        entries = self._d.get("server_configs", {}).get(mode)
        if not entries:
            return [{"label": mode, "args": ""}]
        return [{"label": e["label"], "args": e.get("args", "")} for e in entries]

    def labels(self):
        """Ordered result labels across all modes (report columns / cells dirs)."""
        return [c["label"] for m in self.modes for c in self.configs_for(m)]
