"""S3-compatible immutable object storage adapter."""

from artifact_store.archive import WorkspaceArchiver
from artifact_store.s3 import S3ObjectStore, S3ObjectStoreSettings, create_s3_object_store
from artifact_store.validation import (
    HighConfidenceSecretDetector,
    SecretDetector,
    SnapshotEntryType,
    SnapshotManifestEntry,
    SnapshotValidationResult,
    SnapshotValidationWorker,
    SnapshotValidator,
)

__all__ = [
    "HighConfidenceSecretDetector",
    "S3ObjectStore",
    "S3ObjectStoreSettings",
    "SecretDetector",
    "SnapshotEntryType",
    "SnapshotManifestEntry",
    "SnapshotValidationResult",
    "SnapshotValidationWorker",
    "SnapshotValidator",
    "WorkspaceArchiver",
    "create_s3_object_store",
]
