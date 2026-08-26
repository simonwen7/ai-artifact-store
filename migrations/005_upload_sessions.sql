BEGIN;

CREATE TABLE upload_sessions (
    session_id UUID PRIMARY KEY,
    artifact_id UUID NOT NULL,
    target_node_id TEXT NOT NULL,
    chunking_strategy TEXT NOT NULL,
    chunk_size_bytes BIGINT NOT NULL,
    parent_version_id TEXT,
    state TEXT NOT NULL DEFAULT 'open',
    finalized_version_id TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,

    CONSTRAINT upload_sessions_artifact_fk
        FOREIGN KEY (
            artifact_id
        )
        REFERENCES artifacts (
            artifact_id
        )
        ON DELETE RESTRICT,

    CONSTRAINT upload_sessions_target_node_id_check
        CHECK (
            target_node_id ~ '^[A-Za-z0-9._-]{1,128}$'
        ),

    CONSTRAINT upload_sessions_chunking_strategy_check
        CHECK (
            chunking_strategy IN (
                'fixed-size'
            )
        ),

    CONSTRAINT upload_sessions_chunk_size_bytes_check
        CHECK (
            chunk_size_bytes > 0
        ),

    CONSTRAINT upload_sessions_parent_version_id_format_check
        CHECK (
            parent_version_id IS NULL
            OR parent_version_id ~ '^[0-9a-f]{64}$'
        ),

    CONSTRAINT upload_sessions_finalized_version_id_format_check
        CHECK (
            finalized_version_id IS NULL
            OR finalized_version_id ~ '^[0-9a-f]{64}$'
        ),

    CONSTRAINT upload_sessions_state_check
        CHECK (
            state IN (
                'open',
                'committed',
                'aborted'
            )
        ),

    CONSTRAINT upload_sessions_state_finalized_consistency_check
        CHECK (
            (
                state = 'committed'
                AND finalized_version_id IS NOT NULL
            )
            OR
            (
                state IN (
                    'open',
                    'aborted'
                )
                AND finalized_version_id IS NULL
            )
        ),

    CONSTRAINT upload_sessions_parent_version_fk
        FOREIGN KEY (
            artifact_id,
            parent_version_id
        )
        REFERENCES artifact_versions (
            artifact_id,
            version_id
        )
        ON DELETE RESTRICT,

    CONSTRAINT upload_sessions_finalized_version_fk
        FOREIGN KEY (
            artifact_id,
            finalized_version_id
        )
        REFERENCES artifact_versions (
            artifact_id,
            version_id
        )
        ON DELETE RESTRICT
);

CREATE TABLE upload_session_metadata (
    session_id UUID NOT NULL,
    metadata_key TEXT NOT NULL,
    metadata_value TEXT NOT NULL,

    CONSTRAINT upload_session_metadata_primary_key
        PRIMARY KEY (
            session_id,
            metadata_key
        ),

    CONSTRAINT upload_session_metadata_session_fk
        FOREIGN KEY (
            session_id
        )
        REFERENCES upload_sessions (
            session_id
        )
        ON DELETE CASCADE,

    CONSTRAINT upload_session_metadata_key_not_empty
        CHECK (
            length(metadata_key) > 0
        )
);

CREATE INDEX upload_sessions_artifact_created_at_idx
    ON upload_sessions (
        artifact_id,
        created_at DESC
    );

CREATE INDEX upload_sessions_state_idx
    ON upload_sessions (
        state
    );

INSERT INTO schema_migrations (
    version,
    name
)
VALUES (
    5,
    'upload_sessions'
);

COMMIT;
