BEGIN;

ALTER TABLE upload_session_finalizations
    DROP CONSTRAINT upload_session_finalizations_layout_fk;

ALTER TABLE upload_session_finalizations
    ALTER COLUMN layout_id DROP NOT NULL;

ALTER TABLE upload_session_finalizations
    ADD CONSTRAINT upload_session_finalizations_layout_fk
        FOREIGN KEY (
            layout_id
        )
        REFERENCES object_layouts (
            layout_id
        )
        ON DELETE SET NULL;

INSERT INTO schema_migrations (
    version,
    name
)
VALUES (
    11,
    'retired_finalization_reclamation'
);

COMMIT;
