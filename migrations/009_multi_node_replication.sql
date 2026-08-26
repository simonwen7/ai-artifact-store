BEGIN;

CREATE TABLE storage_nodes (
    node_id TEXT PRIMARY KEY,
    address TEXT NOT NULL,
    port INTEGER NOT NULL,
    state TEXT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,

    CONSTRAINT storage_nodes_node_id_check
        CHECK (
            node_id ~ '^[A-Za-z0-9._-]{1,128}$'
        ),

    CONSTRAINT storage_nodes_address_check
        CHECK (
            length(address) > 0
        ),

    CONSTRAINT storage_nodes_port_check
        CHECK (
            port BETWEEN 1 AND 65535
        ),

    CONSTRAINT storage_nodes_state_check
        CHECK (
            state IN (
                'active',
                'draining',
                'disabled'
            )
        )
);

CREATE INDEX storage_nodes_state_node_id_idx
    ON storage_nodes (
        state,
        node_id
    );

ALTER TABLE upload_sessions
    ADD COLUMN replication_factor SMALLINT NOT NULL DEFAULT 1;

ALTER TABLE upload_sessions
    ADD CONSTRAINT upload_sessions_replication_factor_check
        CHECK (
            replication_factor BETWEEN 1 AND 8
        );

CREATE TABLE upload_session_nodes (
    session_id UUID NOT NULL,
    node_rank SMALLINT NOT NULL,
    node_id TEXT NOT NULL,

    CONSTRAINT upload_session_nodes_pk
        PRIMARY KEY (
            session_id,
            node_rank
        ),

    CONSTRAINT upload_session_nodes_session_node_unique
        UNIQUE (
            session_id,
            node_id
        ),

    CONSTRAINT upload_session_nodes_session_fk
        FOREIGN KEY (
            session_id
        )
        REFERENCES upload_sessions (
            session_id
        )
        ON DELETE CASCADE,

    CONSTRAINT upload_session_nodes_node_rank_check
        CHECK (
            node_rank >= 0
        ),

    CONSTRAINT upload_session_nodes_node_id_check
        CHECK (
            node_id ~ '^[A-Za-z0-9._-]{1,128}$'
        )
);

INSERT INTO upload_session_nodes (
    session_id,
    node_rank,
    node_id
)
SELECT
    session_id,
    0,
    target_node_id
FROM upload_sessions
WHERE NOT EXISTS (
    SELECT 1
    FROM upload_session_nodes usn
    WHERE usn.session_id = upload_sessions.session_id
);

CREATE TABLE replication_runs (
    run_id UUID PRIMARY KEY,
    version_id TEXT NOT NULL,
    layout_id TEXT NOT NULL,
    replication_factor SMALLINT NOT NULL,
    state TEXT NOT NULL,
    chunks_scanned BIGINT NOT NULL DEFAULT 0,
    chunks_under_replicated BIGINT NOT NULL DEFAULT 0,
    replicas_verified BIGINT NOT NULL DEFAULT 0,
    replicas_written BIGINT NOT NULL DEFAULT 0,
    bytes_copied BIGINT NOT NULL DEFAULT 0,
    source_failovers BIGINT NOT NULL DEFAULT 0,
    created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    completed_at TIMESTAMPTZ,

    CONSTRAINT replication_runs_version_fk
        FOREIGN KEY (
            version_id
        )
        REFERENCES artifact_versions (
            version_id
        )
        ON DELETE RESTRICT,

    CONSTRAINT replication_runs_layout_fk
        FOREIGN KEY (
            layout_id
        )
        REFERENCES object_layouts (
            layout_id
        )
        ON DELETE RESTRICT,

    CONSTRAINT replication_runs_replication_factor_check
        CHECK (
            replication_factor BETWEEN 1 AND 8
        ),

    CONSTRAINT replication_runs_state_check
        CHECK (
            state IN (
                'open',
                'completed'
            )
        ),

    CONSTRAINT replication_runs_chunks_scanned_check
        CHECK (
            chunks_scanned >= 0
        ),

    CONSTRAINT replication_runs_chunks_under_replicated_check
        CHECK (
            chunks_under_replicated >= 0
        ),

    CONSTRAINT replication_runs_replicas_verified_check
        CHECK (
            replicas_verified >= 0
        ),

    CONSTRAINT replication_runs_replicas_written_check
        CHECK (
            replicas_written >= 0
        ),

    CONSTRAINT replication_runs_bytes_copied_check
        CHECK (
            bytes_copied >= 0
        ),

    CONSTRAINT replication_runs_source_failovers_check
        CHECK (
            source_failovers >= 0
        ),

    CONSTRAINT replication_runs_state_completed_at_consistency_check
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

CREATE UNIQUE INDEX replication_runs_single_open_idx
    ON replication_runs ((1))
    WHERE state = 'open';

CREATE TABLE replication_run_nodes (
    run_id UUID NOT NULL,
    node_rank SMALLINT NOT NULL,
    node_id TEXT NOT NULL,

    CONSTRAINT replication_run_nodes_pk
        PRIMARY KEY (
            run_id,
            node_rank
        ),

    CONSTRAINT replication_run_nodes_run_node_unique
        UNIQUE (
            run_id,
            node_id
        ),

    CONSTRAINT replication_run_nodes_run_fk
        FOREIGN KEY (
            run_id
        )
        REFERENCES replication_runs (
            run_id
        )
        ON DELETE CASCADE,

    CONSTRAINT replication_run_nodes_node_rank_check
        CHECK (
            node_rank >= 0
        ),

    CONSTRAINT replication_run_nodes_node_id_check
        CHECK (
            node_id ~ '^[A-Za-z0-9._-]{1,128}$'
        )
);

INSERT INTO schema_migrations (
    version,
    name
)
VALUES (
    9,
    'multi_node_replication'
);

COMMIT;
