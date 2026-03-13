from dataclasses import dataclass
from typing import Any, Callable, Dict, List, Tuple


@dataclass
class MinistryModule:
    key: str
    display_name: str
    supports_mission_toggle: bool
    analyzer: Callable[..., Any]

    def weighting_profile(self, mission_mode: bool = False) -> Dict[str, float]:
        return {
            "market_size": 0.35,
            "income": 0.25,
            "competition": 0.25,
            "family_density": 0.15,
        }

    def income_higher_is_better(self, mission_mode: bool = False) -> bool:
        return True

    def recommendation_text(
        self, score: int, competitor_count: int, market_pop: int, mission_mode: bool = False
    ) -> Tuple[str, str]:
        raise NotImplementedError

    def load_competitors(
        self, lat: float, lon: float, radius_miles: float, mission_mode: bool = False
    ) -> List[Dict[str, Any]]:
        raise NotImplementedError
