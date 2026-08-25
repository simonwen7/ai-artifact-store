BEGIN;

DO $$
BEGIN
    IF EXISTS (
        SELECT 1
        FROM objects
        LIMIT 1
    )
    OR EXISTS (
        SELECT 1
        FROM object_chunks
        LIMIT 1
    )
    OR EXISTS (
        SELECT 1
        FROM artifact_versions
        LIMIT 1
    )
    OR EXISTS (
        SELECT 1
        FROM tags
        LIMIT 1
    ) THEN
        RAISE EXCEPTION
            'migration 002 cannot convert legacy object/layout identity data; recreate the development database from migrations because raw object bytes are required to recover content object IDs';
    END IF;
END
$$;

DROP INDEX object_chunks_chunk_id_idx;

DROP TABLE object_chunks;

ALTER TABLE objects
    DROP CONSTRAINT objects_descriptor_version_check,
    DROP CONSTRAINT objects_chunk_count_check,
    DROP CONSTRAINT objects_empty_layout_check,
    DROP COLUMN descriptor_version,
    DROP COLUMN canonical_descriptor,
    DROP COLUMN chunk_count;

ALTER TABLE objects
    ADD CONSTRAINT objects_id_size_unique
        UNIQUE (
            object_id,
            total_size_bytes
        );

CREATE TABLE object_layouts (
    layout_id TEXT PRIMARY KEY,
    object_id TEXT NOT NULL,
    descriptor_version INTEGER NOT NULL,
    chunking_strategy TEXT NOT NULL,
    canonical_descriptor BYTEA NOT NULL,
    total_size_bytes BIGINT NOT NULL,
    chunk_count BIGINT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,

    CONSTRAINT object_layouts_layout_id_format_check
        CHECK (
            layout_id ~ '^[0-9a-f]{64}$'
        ),

    CONSTRAINT object_layouts_descriptor_version_check
        CHECK (
            descriptor_version > 0
        ),

    CONSTRAINT object_layouts_chunking_strategy_check
        CHECK (
            chunking_strategy IN (
                'fixed-size'
            )
        ),

    CONSTRAINT object_layouts_total_size_bytes_check
        CHECK (
            total_size_bytes >= 0
        ),

    CONSTRAINT object_layouts_chunk_count_check
        CHECK (
            chunk_count >= 0
        ),

    CONSTRAINT object_layouts_empty_layout_check
        CHECK (
            chunk_count > 0
            OR total_size_bytes = 0
        ),

    CONSTRAINT object_layouts_object_fk
        FOREIGN KEY (
            object_id,
            total_size_bytes
        )
        REFERENCES objects (
            object_id,
            total_size_bytes
        )
        ON DELETE CASCADE
);

CREATE TABLE object_layout_chunks (
    layout_id TEXT NOT NULL,
    chunk_index BIGINT NOT NULL,
    chunk_id TEXT NOT NULL,
    byte_offset BIGINT NOT NULL,
    chunk_size_bytes BIGINT NOT NULL,

    CONSTRAINT object_layout_chunks_primary_key
        PRIMARY KEY (
            layout_id,
            chunk_index
        ),

    CONSTRAINT object_layout_chunks_layout_fk
        FOREIGN KEY (
            layout_id
        )
        REFERENCES object_layouts (
            layout_id
        )
        ON DELETE CASCADE,

    CONSTRAINT object_layout_chunks_chunk_fk
        FOREIGN KEY (
            chunk_id,
            chunk_size_bytes
        )
        REFERENCES chunks (
            chunk_id,
            size_bytes
        )
        ON DELETE RESTRICT,

    CONSTRAINT object_layout_chunks_chunk_index_check
        CHECK (
            chunk_index >= 0
        ),

    CONSTRAINT object_layout_chunks_byte_offset_check
        CHECK (
            byte_offset >= 0
        ),

    CONSTRAINT object_layout_chunks_chunk_size_check
        CHECK (
            chunk_size_bytes > 0
        ),

    CONSTRAINT object_layout_chunks_unique_offset
        UNIQUE (
            layout_id,
            byte_offset
        )
);

CREATE INDEX object_layouts_object_id_idx
    ON object_layouts (
        object_id
    );

CREATE INDEX object_layout_chunks_chunk_id_idx
    ON object_layout_chunks (
        chunk_id
    );

INSERT INTO schema_migrations (
    version,
    name
)
VALUES (
    2,
    'separate_object_layouts'
);

COMMIT;
