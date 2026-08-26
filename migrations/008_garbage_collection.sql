BEGIN;

CREATE TABLE gc_runs (
    run_id UUID PRIMARY KEY,
    target_node_id TEXT NOT NULL,
    mode TEXT NOT NULL,
    state TEXT NOT NULL,
    physical_chunks_scanned BIGINT NOT NULL DEFAULT 0,
    physical_bytes_scanned BIGINT NOT NULL DEFAULT 0,
    collectible_chunks BIGINT NOT NULL DEFAULT 0,
    collectible_bytes BIGINT NOT NULL DEFAULT 0,
    physically_deleted_chunks BIGINT NOT NULL DEFAULT 0,
    physically_deleted_bytes BIGINT NOT NULL DEFAULT 0,
    storage_locations_swept BIGINT NOT NULL DEFAULT 0,
    chunk_rows_swept BIGINT NOT NULL DEFAULT 0,
    object_layouts_swept BIGINT NOT NULL DEFAULT 0,
    objects_swept BIGINT NOT NULL DEFAULT 0,
    created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    completed_at TIMESTAMPTZ,

    CONSTRAINT gc_runs_target_node_id_check
        CHECK (
            target_node_id ~ '^[A-Za-z0-9._-]{1,128}$'
        ),

    CONSTRAINT gc_runs_mode_check
        CHECK (
            mode IN (
                'apply',
                'dry-run'
            )
        ),

    CONSTRAINT gc_runs_state_check
        CHECK (
            state IN (
                'open',
                'completed'
            )
        ),

    CONSTRAINT gc_runs_physical_chunks_scanned_check
        CHECK (
            physical_chunks_scanned >= 0
        ),

    CONSTRAINT gc_runs_physical_bytes_scanned_check
        CHECK (
            physical_bytes_scanned >= 0
        ),

    CONSTRAINT gc_runs_collectible_chunks_check
        CHECK (
            collectible_chunks >= 0
        ),

    CONSTRAINT gc_runs_collectible_bytes_check
        CHECK (
            collectible_bytes >= 0
        ),

    CONSTRAINT gc_runs_physically_deleted_chunks_check
        CHECK (
            physically_deleted_chunks >= 0
        ),

    CONSTRAINT gc_runs_physically_deleted_bytes_check
        CHECK (
            physically_deleted_bytes >= 0
        ),

    CONSTRAINT gc_runs_storage_locations_swept_check
        CHECK (
            storage_locations_swept >= 0
        ),

    CONSTRAINT gc_runs_chunk_rows_swept_check
        CHECK (
            chunk_rows_swept >= 0
        ),

    CONSTRAINT gc_runs_object_layouts_swept_check
        CHECK (
            object_layouts_swept >= 0
        ),

    CONSTRAINT gc_runs_objects_swept_check
        CHECK (
            objects_swept >= 0
        ),

    CONSTRAINT gc_runs_state_completed_at_consistency_check
        CHECK (
            (
                state = 'open'
                AND completed_at IS NULL
            )
            OR
            (
                state = 'completed'
                AND completed_at IS NOT NULL
            )
        )
);

CREATE UNIQUE INDEX gc_runs_single_open_idx
    ON gc_runs ((1))
    WHERE state = 'open';

CREATE INDEX artifact_versions_root_object_id_idx
    ON artifact_versions (
        root_object_id
    );

INSERT INTO schema_migrations (
    version,
    name
)
VALUES (
    8,
    'garbage_collection'
);

COMMIT;
