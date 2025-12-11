from datetime import datetime
from collections import deque
import copy


class SnapshotTracker:
    def __init__(self, max_snapshots=20):
        self.snapshots = deque(maxlen=max_snapshots)

    def save(self, hyblock_data: dict, current_price: float):
        """Guarda copia del snapshot con metadata"""
        snapshot = {
            "timestamp": datetime.utcnow().isoformat() + "Z",
            "price_at_snapshot": current_price,
            "data": copy.deepcopy(hyblock_data)
        }
        self.snapshots.append(snapshot)
        return len(self.snapshots)

    def list_snapshots(self):
        """Lista timestamps disponibles"""
        return [
            {
                "timestamp": s["timestamp"],
                "price": s["price_at_snapshot"],
                "clusters_count": len(s["data"].get("data", []))
            }
            for s in self.snapshots
        ]

    def get_snapshot(self, timestamp: str):
        """Obtiene snapshot por timestamp exacto"""
        for s in self.snapshots:
            if s["timestamp"] == timestamp:
                return s
        return None

    def get_latest(self):
        """Obtiene último snapshot"""
        if self.snapshots:
            return self.snapshots[-1]
        return None


tracker = SnapshotTracker(max_snapshots=20)
