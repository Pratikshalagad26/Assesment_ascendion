import hashlib


def content_hash(content: str) -> str:
    """SHA-256 hex digest of UTF-8 content. Used for cache keys and version identity."""
    return hashlib.sha256(content.encode("utf-8")).hexdigest()
