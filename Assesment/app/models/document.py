from enum import Enum


class DocumentStatus(str, Enum):
    QUEUED = "queued"
    PROCESSING = "processing"
    ENRICHING = "enriching"
    COMPLETED = "completed"
    FAILED = "failed"


# States that count toward the per-user active pipeline limit
ACTIVE_STATUSES = frozenset(
    {
        DocumentStatus.QUEUED,
        DocumentStatus.PROCESSING,
        DocumentStatus.ENRICHING,
    }
)


class FailedStage(str, Enum):
    PROCESSING = "processing"
    ENRICHING = "enriching"
