BEGIN;

ALTER TABLE object_layouts
    ADD COLUMN fastcdc_min_chunk_size_bytes BIGINT,
    ADD COLUMN fastcdc_avg_chunk_size_bytes BIGINT,
    ADD COLUMN fastcdc_max_chunk_size_bytes BIGINT;

ALTER TABLE object_layouts
    DROP CONSTRAINT object_layouts_chunking_strategy_check;

ALTER TABLE object_layouts
    ADD CONSTRAINT object_layouts_chunking_strategy_check
        CHECK (
            chunking_strategy IN (
                'fixed-size',
                'fastcdc'
            )
        );

ALTER TABLE object_layouts
    ADD CONSTRAINT object_layouts_fastcdc_configuration_check
        CHECK (
            (
                chunking_strategy = 'fixed-size'
                AND fastcdc_min_chunk_size_bytes IS NULL
                AND fastcdc_avg_chunk_size_bytes IS NULL
                AND fastcdc_max_chunk_size_bytes IS NULL
            )
            OR
            (
                chunking_strategy = 'fastcdc'
                AND fastcdc_min_chunk_size_bytes IS NOT NULL
                AND fastcdc_avg_chunk_size_bytes IS NOT NULL
                AND fastcdc_max_chunk_size_bytes IS NOT NULL
                AND fastcdc_min_chunk_size_bytes >= 64
                AND fastcdc_avg_chunk_size_bytes >= 64
                AND fastcdc_min_chunk_size_bytes <= fastcdc_avg_chunk_size_bytes
                AND fastcdc_avg_chunk_size_bytes <= fastcdc_max_chunk_size_bytes
                AND fastcdc_max_chunk_size_bytes <= 8388608
                AND (
                    fastcdc_avg_chunk_size_bytes
                    & (fastcdc_avg_chunk_size_bytes - 1)
                ) = 0
            )
        );

ALTER TABLE upload_sessions
    ALTER COLUMN chunk_size_bytes DROP NOT NULL;

ALTER TABLE upload_sessions
    ADD COLUMN fastcdc_min_chunk_size_bytes BIGINT,
    ADD COLUMN fastcdc_avg_chunk_size_bytes BIGINT,
    ADD COLUMN fastcdc_max_chunk_size_bytes BIGINT;

ALTER TABLE upload_sessions
    DROP CONSTRAINT upload_sessions_chunking_strategy_check;

ALTER TABLE upload_sessions
    DROP CONSTRAINT upload_sessions_chunk_size_bytes_check;

ALTER TABLE upload_sessions
    ADD CONSTRAINT upload_sessions_chunking_strategy_check
        CHECK (
            chunking_strategy IN (
                'fixed-size',
                'fastcdc'
            )
        );

ALTER TABLE upload_sessions
    ADD CONSTRAINT upload_sessions_chunking_configuration_check
        CHECK (
            (
                chunking_strategy = 'fixed-size'
                AND chunk_size_bytes IS NOT NULL
                AND chunk_size_bytes > 0
                AND fastcdc_min_chunk_size_bytes IS NULL
                AND fastcdc_avg_chunk_size_bytes IS NULL
                AND fastcdc_max_chunk_size_bytes IS NULL
            )
            OR
            (
                chunking_strategy = 'fastcdc'
                AND chunk_size_bytes IS NULL
                AND fastcdc_min_chunk_size_bytes IS NOT NULL
                AND fastcdc_avg_chunk_size_bytes IS NOT NULL
                AND fastcdc_max_chunk_size_bytes IS NOT NULL
                AND fastcdc_min_chunk_size_bytes >= 64
                AND fastcdc_avg_chunk_size_bytes >= 64
                AND fastcdc_min_chunk_size_bytes <= fastcdc_avg_chunk_size_bytes
                AND fastcdc_avg_chunk_size_bytes <= fastcdc_max_chunk_size_bytes
                AND fastcdc_max_chunk_size_bytes <= 8388608
                AND (
                    fastcdc_avg_chunk_size_bytes
                    & (fastcdc_avg_chunk_size_bytes - 1)
                ) = 0
            )
        );

INSERT INTO schema_migrations (
    version,
    name
)
VALUES (
    7,
    'fastcdc_chunking'
);

COMMIT;
