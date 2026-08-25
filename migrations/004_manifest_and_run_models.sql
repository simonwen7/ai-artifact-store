BEGIN;

CREATE TABLE manifests (
    manifest_id TEXT PRIMARY KEY,
    descriptor_version INTEGER NOT NULL,
    canonical_descriptor BYTEA NOT NULL,
    entry_count BIGINT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,

    CONSTRAINT manifests_manifest_id_format_check
        CHECK (
            manifest_id ~ '^[0-9a-f]{64}$'
        ),

    CONSTRAINT manifests_descriptor_version_check
        CHECK (
            descriptor_version > 0
        ),

    CONSTRAINT manifests_entry_count_check
        CHECK (
            entry_count >= 0
        )
);

CREATE TABLE manifest_entries (
    manifest_id TEXT NOT NULL,
    role TEXT NOT NULL,
    version_id TEXT NOT NULL,

    CONSTRAINT manifest_entries_primary_key
        PRIMARY KEY (
            manifest_id,
            role
        ),

    CONSTRAINT manifest_entries_manifest_fk
        FOREIGN KEY (
            manifest_id
        )
        REFERENCES manifests (
            manifest_id
        )
        ON DELETE CASCADE,

    CONSTRAINT manifest_entries_version_fk
        FOREIGN KEY (
            version_id
        )
        REFERENCES artifact_versions (
            version_id
        )
        ON DELETE RESTRICT,

    CONSTRAINT manifest_entries_role_not_empty
        CHECK (
            length(role) > 0
        ),

    CONSTRAINT manifest_entries_version_id_format_check
        CHECK (
            version_id ~ '^[0-9a-f]{64}$'
        )
);

CREATE TABLE runs (
    run_id UUID PRIMARY KEY,
    manifest_id TEXT NOT NULL,
    name TEXT NOT NULL,
    source_commit TEXT,
    started_at TIMESTAMPTZ NOT NULL,
    completed_at TIMESTAMPTZ,

    CONSTRAINT runs_manifest_fk
        FOREIGN KEY (
            manifest_id
        )
        REFERENCES manifests (
            manifest_id
        )
        ON DELETE RESTRICT,

    CONSTRAINT runs_manifest_id_format_check
        CHECK (
            manifest_id ~ '^[0-9a-f]{64}$'
        ),

    CONSTRAINT runs_name_not_empty
        CHECK (
            length(name) > 0
        ),

    CONSTRAINT runs_source_commit_not_empty
        CHECK (
            source_commit IS NULL
            OR length(source_commit) > 0
        ),

    CONSTRAINT runs_completion_order_check
        CHECK (
            completed_at IS NULL
            OR completed_at >= started_at
        )
);

CREATE TABLE run_tags (
    run_id UUID NOT NULL,
    tag_key TEXT NOT NULL,
    tag_value TEXT NOT NULL,

    CONSTRAINT run_tags_primary_key
        PRIMARY KEY (
            run_id,
            tag_key
        ),

    CONSTRAINT run_tags_run_fk
        FOREIGN KEY (
            run_id
        )
        REFERENCES runs (
            run_id
        )
        ON DELETE CASCADE,

    CONSTRAINT run_tags_key_not_empty
        CHECK (
            length(tag_key) > 0
        )
);

CREATE INDEX manifest_entries_version_id_idx
    ON manifest_entries (
        version_id
    );

CREATE INDEX runs_manifest_id_idx
    ON runs (
        manifest_id
    );

INSERT INTO schema_migrations (
    version,
    name
)
VALUES (
    4,
    'manifest_and_run_models'
);

COMMIT;
