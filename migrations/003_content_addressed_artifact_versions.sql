BEGIN;

DO $$
BEGIN
    IF EXISTS (
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
            'migration 003 cannot convert legacy UUID artifact version identities; recreate or clean version/tag development data before applying content-addressed ArtifactVersion identity';
    END IF;
END
$$;

DROP INDEX tags_version_id_idx;

ALTER TABLE tags
    DROP CONSTRAINT tags_version_fk;

ALTER TABLE artifact_versions
    DROP CONSTRAINT artifact_versions_artifact_version_unique;

ALTER TABLE artifact_versions
    DROP CONSTRAINT artifact_versions_pkey;

ALTER TABLE tags
    ALTER COLUMN version_id
        TYPE TEXT
        USING version_id::text;

ALTER TABLE artifact_versions
    ALTER COLUMN version_id
        TYPE TEXT
        USING version_id::text;

ALTER TABLE artifact_versions
    DROP COLUMN metadata,
    ADD COLUMN parent_version_id TEXT,
    ADD COLUMN descriptor_version INTEGER NOT NULL,
    ADD COLUMN canonical_descriptor BYTEA NOT NULL;

ALTER TABLE artifact_versions
    ADD CONSTRAINT artifact_versions_primary_key
        PRIMARY KEY (
            version_id
        ),

    ADD CONSTRAINT artifact_versions_version_id_format_check
        CHECK (
            version_id ~ '^[0-9a-f]{64}$'
        ),

    ADD CONSTRAINT artifact_versions_parent_version_id_format_check
        CHECK (
            parent_version_id IS NULL
            OR parent_version_id ~ '^[0-9a-f]{64}$'
        ),

    ADD CONSTRAINT artifact_versions_parent_not_self_check
        CHECK (
            parent_version_id IS NULL
            OR parent_version_id <> version_id
        ),

    ADD CONSTRAINT artifact_versions_descriptor_version_check
        CHECK (
            descriptor_version > 0
        ),

    ADD CONSTRAINT artifact_versions_artifact_version_unique
        UNIQUE (
            artifact_id,
            version_id
        ),

    ADD CONSTRAINT artifact_versions_parent_fk
        FOREIGN KEY (
            artifact_id,
            parent_version_id
        )
        REFERENCES artifact_versions (
            artifact_id,
            version_id
        )
        ON DELETE RESTRICT;

ALTER TABLE tags
    ADD CONSTRAINT tags_version_id_format_check
        CHECK (
            version_id ~ '^[0-9a-f]{64}$'
        ),

    ADD CONSTRAINT tags_version_fk
        FOREIGN KEY (
            artifact_id,
            version_id
        )
        REFERENCES artifact_versions (
            artifact_id,
            version_id
        )
        ON DELETE CASCADE;

CREATE TABLE artifact_version_metadata (
    version_id TEXT NOT NULL,
    metadata_key TEXT NOT NULL,
    metadata_value TEXT NOT NULL,

    CONSTRAINT artifact_version_metadata_primary_key
        PRIMARY KEY (
            version_id,
            metadata_key
        ),

    CONSTRAINT artifact_version_metadata_version_fk
        FOREIGN KEY (
            version_id
        )
        REFERENCES artifact_versions (
            version_id
        )
        ON DELETE CASCADE,

    CONSTRAINT artifact_version_metadata_key_not_empty
        CHECK (
            length(metadata_key) > 0
        )
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
    3,
    'content_addressed_artifact_versions'
);

COMMIT;
