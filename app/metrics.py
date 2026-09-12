from __future__ import annotations

import time
from dataclasses import dataclass, field


@dataclass
class Metrics:
    start_time: float = field(default_factory=time.time)
    requests: int = 0
    db_queries: int = 0
    cache_hits: int = 0
    cache_misses: int = 0
    ws_global_connections: int = 0
    ws_system_connections: int = 0
    broadcaster_role: str = "disabled"  # "disabled" | "leader" | "follower"

    def snapshot(self) -> dict:
        return {
            "uptime": round(time.time() - self.start_time, 1),
            "requests": self.requests,
            "db_queries": self.db_queries,
            "cache_hits": self.cache_hits,
            "cache_misses": self.cache_misses,
            "ws_global_connections": self.ws_global_connections,
            "ws_system_connections": self.ws_system_connections,
            "broadcaster_role": self.broadcaster_role,
        }


metrics = Metrics()
