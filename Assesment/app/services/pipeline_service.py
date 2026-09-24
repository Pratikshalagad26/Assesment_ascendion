import asyncio
import random
import re
from collections import Counter

from app.config import Settings
from app.logging_config import get_logger

logger = get_logger(__name__)

_WORD_RE = re.compile(r"[a-zA-Z0-9']+")


class StageFailed(Exception):
    """Simulated random stage failure."""


class PipelineService:
    """Simulates the two processing stages with configurable delays and failure rate."""

    def __init__(self, settings: Settings):
        self._settings = settings

    async def run_processing(self, content: str) -> str:
        delay = random.uniform(
            self._settings.processing_delay_min,
            self._settings.processing_delay_max,
        )
        logger.info("processing_started", delay_seconds=round(delay, 2))
        await asyncio.sleep(delay)
        self._maybe_fail("processing")
        return self.generate_summary(content)

    async def run_enriching(self, summary: str) -> list[str]:
        delay = random.uniform(
            self._settings.enriching_delay_min,
            self._settings.enriching_delay_max,
        )
        logger.info("enriching_started", delay_seconds=round(delay, 2))
        await asyncio.sleep(delay)
        self._maybe_fail("enriching")
        return self.generate_tags(summary)

    def _maybe_fail(self, stage: str) -> None:
        if random.random() < self._settings.stage_failure_rate:
            raise StageFailed(f"Simulated {stage} failure")

    @staticmethod
    def generate_summary(content: str) -> str:
        """Deterministic mock summary from the first ~40 words."""
        words = _WORD_RE.findall(content)
        if not words:
            return "Empty content summary."
        excerpt = " ".join(words[:40])
        word_count = len(words)
        return f"Summary ({word_count} words): {excerpt}"

    @staticmethod
    def generate_tags(summary: str) -> list[str]:
        """Deterministic mock keyword tags from the most common words in the summary."""
        words = [w.lower() for w in _WORD_RE.findall(summary)]
        stop = {"summary", "words", "the", "a", "an", "and", "or", "of", "to", "in", "is"}
        filtered = [w for w in words if w not in stop and len(w) > 2]
        if not filtered:
            return ["general"]
        counts = Counter(filtered)
        return [word for word, _ in counts.most_common(5)]
