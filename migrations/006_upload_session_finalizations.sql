BEGIN;

DO $$
BEGIN
    IF EXISTS (
        SELECT 1
        FROM upload_sessions
        WHERE state = 'committed'
        LIMIT 1
    ) THEN
        RAISE EXCEPTION
            'migration 006 cannot proceed: committed upload_sessions rows exist without durable finalized-layout identity; remove or recreate those sessions before applying upload_session_finalizations';
    END IF;
END
$$;

CREATE TABLE upload_session_finalizations (
    session_id UUID NOT NULL,
    layout_id TEXT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,

    CONSTRAINT upload_session_finalizations_primary_key
        PRIMARY KEY (
            session_id
        ),

    CONSTRAINT upload_session_finalizations_session_fk
        FOREIGN KEY (
            session_id
        )
        REFERENCES upload_sessions (
            session_id
        )
        ON DELETE CASCADE,

    CONSTRAINT upload_session_finalizations_layout_id_format_check
        CHECK (
            layout_id ~ '^[0-9a-f]{64}$'
        ),

    CONSTRAINT upload_session_finalizations_layout_fk
        FOREIGN KEY (
            layout_id
        )
        REFERENCES object_layouts (
            layout_id
        )
        ON DELETE RESTRICT
);

CREATE INDEX upload_session_finalizations_layout_id_idx
    ON upload_session_finalizations (
        layout_id
    );

INSERT INTO schema_migrations (
    version,
    name
)
VALUES (
    6,
    'upload_session_finalizations'
);

COMMIT;
