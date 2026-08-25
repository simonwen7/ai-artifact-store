BEGIN;

CREATE TABLE schema_migrations (
    version INTEGER PRIMARY KEY,
    name TEXT NOT NULL,
    applied_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE chunks (
    chunk_id TEXT PRIMARY KEY,
    size_bytes BIGINT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,

    CONSTRAINT chunks_chunk_id_format_check
        CHECK (
            chunk_id ~ '^[0-9a-f]{64}$'
        ),

    CONSTRAINT chunks_size_bytes_check
        CHECK (
            size_bytes > 0
        ),

    CONSTRAINT chunks_id_size_unique
        UNIQUE (
            chunk_id,
            size_bytes
        )
);

CREATE TABLE objects (
    object_id TEXT PRIMARY KEY,
    descriptor_version INTEGER NOT NULL,
    canonical_descriptor BYTEA NOT NULL,
    total_size_bytes BIGINT NOT NULL,
    chunk_count BIGINT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,

    CONSTRAINT objects_object_id_format_check
        CHECK (
            object_id ~ '^[0-9a-f]{64}$'
        ),

    CONSTRAINT objects_descriptor_version_check
        CHECK (
            descriptor_version > 0
        ),

    CONSTRAINT objects_total_size_bytes_check
        CHECK (
            total_size_bytes >= 0
        ),

    CONSTRAINT objects_chunk_count_check
        CHECK (
            chunk_count >= 0
        ),

    CONSTRAINT objects_empty_layout_check
        CHECK (
            chunk_count > 0
            OR total_size_bytes = 0
        )
);

CREATE TABLE object_chunks (
    object_id TEXT NOT NULL,
    chunk_index BIGINT NOT NULL,
    chunk_id TEXT NOT NULL,
    byte_offset BIGINT NOT NULL,
    chunk_size_bytes BIGINT NOT NULL,

    CONSTRAINT object_chunks_primary_key
        PRIMARY KEY (
            object_id,
            chunk_index
        ),

    CONSTRAINT object_chunks_object_fk
        FOREIGN KEY (
            object_id
        )
        REFERENCES objects (
            object_id
        )
        ON DELETE CASCADE,

    CONSTRAINT object_chunks_chunk_fk
        FOREIGN KEY (
            chunk_id,
            chunk_size_bytes
        )
        REFERENCES chunks (
            chunk_id,
            size_bytes
        )
        ON DELETE RESTRICT,

    CONSTRAINT object_chunks_chunk_index_check
        CHECK (
            chunk_index >= 0
        ),

    CONSTRAINT object_chunks_byte_offset_check
        CHECK (
            byte_offset >= 0
        ),

    CONSTRAINT object_chunks_chunk_size_check
        CHECK (
            chunk_size_bytes > 0
        ),

    CONSTRAINT object_chunks_unique_offset
        UNIQUE (
            object_id,
            byte_offset
        )
);

CREATE TABLE artifacts (
    artifact_id UUID PRIMARY KEY,
    project TEXT NOT NULL,
    name TEXT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,

    CONSTRAINT artifacts_project_not_empty
        CHECK (
            length(project) > 0
        ),

    CONSTRAINT artifacts_name_not_empty
        CHECK (
            length(name) > 0
        ),

    CONSTRAINT artifacts_project_name_unique
        UNIQUE (
            project,
            name
        )
);

CREATE TABLE artifact_versions (
    version_id UUID PRIMARY KEY,
    artifact_id UUID NOT NULL,
    root_object_id TEXT NOT NULL,
    state TEXT NOT NULL,
    metadata JSONB NOT NULL DEFAULT '{}'::jsonb,
    created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,

    CONSTRAINT artifact_versions_artifact_fk
        FOREIGN KEY (
            artifact_id
        )
        REFERENCES artifacts (
            artifact_id
        )
        ON DELETE RESTRICT,

    CONSTRAINT artifact_versions_root_object_fk
        FOREIGN KEY (
            root_object_id
        )
        REFERENCES objects (
            object_id
        )
        ON DELETE RESTRICT,

    CONSTRAINT artifact_versions_state_check
        CHECK (
            state IN (
                'staging',
                'committed',
                'failed'
            )
        ),

    CONSTRAINT artifact_versions_artifact_version_unique
        UNIQUE (
            artifact_id,
            version_id
        )
);

CREATE TABLE tags (
    artifact_id UUID NOT NULL,
    tag_name TEXT NOT NULL,
    version_id UUID NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,

    CONSTRAINT tags_primary_key
        PRIMARY KEY (
            artifact_id,
            tag_name
        ),

    CONSTRAINT tags_artifact_fk
        FOREIGN KEY (
            artifact_id
        )
        REFERENCES artifacts (
            artifact_id
        )
        ON DELETE CASCADE,

    CONSTRAINT tags_version_fk
        FOREIGN KEY (
            artifact_id,
            version_id
        )
        REFERENCES artifact_versions (
            artifact_id,
            version_id
        )
        ON DELETE CASCADE,

    CONSTRAINT tags_name_not_empty
        CHECK (
            length(tag_name) > 0
        )
);

CREATE TABLE storage_locations (
    location_id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    chunk_id TEXT NOT NULL,
    node_id TEXT NOT NULL,
    storage_path TEXT NOT NULL,
    state TEXT NOT NULL DEFAULT 'available',
    verified_at TIMESTAMPTZ,
    created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,

    CONSTRAINT storage_locations_chunk_fk
        FOREIGN KEY (
            chunk_id
        )
        REFERENCES chunks (
            chunk_id
        )
        ON DELETE CASCADE,

    CONSTRAINT storage_locations_node_not_empty
        CHECK (
            length(node_id) > 0
        ),

    CONSTRAINT storage_locations_path_not_empty
        CHECK (
            length(storage_path) > 0
        ),

    CONSTRAINT storage_locations_state_check
        CHECK (
            state IN (
                'available',
                'missing',
                'corrupt'
            )
        ),

    CONSTRAINT storage_locations_chunk_node_unique
        UNIQUE (
            chunk_id,
            node_id
        ),

    CONSTRAINT storage_locations_node_path_unique
        UNIQUE (
            node_id,
            storage_path
        )
);

CREATE INDEX artifact_versions_artifact_created_at_idx
    ON artifact_versions (
        artifact_id,
        created_at DESC
    );

CREATE INDEX object_chunks_chunk_id_idx
    ON object_chunks (
        chunk_id
    );

CREATE INDEX storage_locations_chunk_id_idx
    ON storage_locations (
        chunk_id
    );

CREATE INDEX tags_version_id_idx
    ON tags (
        version_id
    );

INSERT INTO schema_migrations (
    version,
    name
)
VALUES (
    1,
    'initial_schema'
);

COMMIT;
