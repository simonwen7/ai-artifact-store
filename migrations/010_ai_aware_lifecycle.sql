BEGIN;

CREATE TABLE lifecycle_policies (
    policy_id UUID PRIMARY KEY,
    name TEXT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,

    CONSTRAINT lifecycle_policies_name_check
        CHECK (
            length(name) BETWEEN 1 AND 128
        )
);

CREATE TABLE lifecycle_policy_rules (
    policy_id UUID NOT NULL,
    artifact_kind TEXT NOT NULL,
    keep_last_n INTEGER NOT NULL,
    max_age_seconds BIGINT,

    CONSTRAINT lifecycle_policy_rules_pk
        PRIMARY KEY (
            policy_id,
            artifact_kind
        ),

    CONSTRAINT lifecycle_policy_rules_policy_fk
        FOREIGN KEY (
            policy_id
        )
        REFERENCES lifecycle_policies (
            policy_id
        )
        ON DELETE CASCADE,

    CONSTRAINT lifecycle_policy_rules_artifact_kind_check
        CHECK (
            artifact_kind IN (
                'generic',
                'model-checkpoint',
                'dataset-snapshot',
                'embedding-index',
                'evaluation-output'
            )
        ),

    CONSTRAINT lifecycle_policy_rules_keep_last_n_check
        CHECK (
            keep_last_n BETWEEN 0 AND 1000
        ),

    CONSTRAINT lifecycle_policy_rules_max_age_seconds_check
        CHECK (
            max_age_seconds IS NULL
            OR max_age_seconds BETWEEN 0 AND 315360000
        )
);

CREATE TABLE artifact_version_pins (
    version_id TEXT PRIMARY KEY,
    reason TEXT NOT NULL,
    pinned_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,

    CONSTRAINT artifact_version_pins_version_fk
        FOREIGN KEY (
            version_id
        )
        REFERENCES artifact_versions (
            version_id
        )
        ON DELETE RESTRICT,

    CONSTRAINT artifact_version_pins_reason_check
        CHECK (
            length(reason) BETWEEN 1 AND 256
        )
);

CREATE TABLE lifecycle_runs (
    run_id UUID PRIMARY KEY,
    policy_id UUID NOT NULL,
    mode TEXT NOT NULL,
    evaluated_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    versions_scanned BIGINT NOT NULL,
    versions_protected BIGINT NOT NULL,
    versions_retained_by_policy BIGINT NOT NULL,
    versions_candidates BIGINT NOT NULL,
    versions_retired BIGINT NOT NULL,
    logical_bytes_candidates BIGINT NOT NULL,
    logical_bytes_retired BIGINT NOT NULL,

    CONSTRAINT lifecycle_runs_policy_fk
        FOREIGN KEY (
            policy_id
        )
        REFERENCES lifecycle_policies (
            policy_id
        )
        ON DELETE RESTRICT,

    CONSTRAINT lifecycle_runs_mode_check
        CHECK (
            mode IN (
                'dry-run',
                'apply'
            )
        ),

    CONSTRAINT lifecycle_runs_versions_scanned_check
        CHECK (
            versions_scanned >= 0
        ),

    CONSTRAINT lifecycle_runs_versions_protected_check
        CHECK (
            versions_protected >= 0
        ),

    CONSTRAINT lifecycle_runs_versions_retained_by_policy_check
        CHECK (
            versions_retained_by_policy >= 0
        ),

    CONSTRAINT lifecycle_runs_versions_candidates_check
        CHECK (
            versions_candidates >= 0
        ),

    CONSTRAINT lifecycle_runs_versions_retired_check
        CHECK (
            versions_retired >= 0
        ),

    CONSTRAINT lifecycle_runs_logical_bytes_candidates_check
        CHECK (
            logical_bytes_candidates >= 0
        ),

    CONSTRAINT lifecycle_runs_logical_bytes_retired_check
        CHECK (
            logical_bytes_retired >= 0
        )
);

CREATE TABLE lifecycle_run_decisions (
    run_id UUID NOT NULL,
    version_id TEXT NOT NULL,
    artifact_id UUID NOT NULL,
    artifact_kind TEXT NOT NULL,
    decision TEXT NOT NULL,
    reason TEXT NOT NULL,
    logical_size_bytes BIGINT NOT NULL,

    CONSTRAINT lifecycle_run_decisions_pk
        PRIMARY KEY (
            run_id,
            version_id
        ),

    CONSTRAINT lifecycle_run_decisions_run_fk
        FOREIGN KEY (
            run_id
        )
        REFERENCES lifecycle_runs (
            run_id
        )
        ON DELETE CASCADE,

    CONSTRAINT lifecycle_run_decisions_version_fk
        FOREIGN KEY (
            version_id
        )
        REFERENCES artifact_versions (
            version_id
        )
        ON DELETE RESTRICT,

    CONSTRAINT lifecycle_run_decisions_artifact_fk
        FOREIGN KEY (
            artifact_id
        )
        REFERENCES artifacts (
            artifact_id
        )
        ON DELETE RESTRICT,

    CONSTRAINT lifecycle_run_decisions_artifact_kind_check
        CHECK (
            artifact_kind IN (
                'generic',
                'model-checkpoint',
                'dataset-snapshot',
                'embedding-index',
                'evaluation-output'
            )
        ),

    CONSTRAINT lifecycle_run_decisions_decision_check
        CHECK (
            decision IN (
                'retain',
                'retire'
            )
        ),

    CONSTRAINT lifecycle_run_decisions_reason_check
        CHECK (
            reason IN (
                'pinned',
                'tagged',
                'manifest-referenced',
                'keep-last-n',
                'age-not-reached',
                'policy-retire'
            )
        ),

    CONSTRAINT lifecycle_run_decisions_logical_size_bytes_check
        CHECK (
            logical_size_bytes >= 0
        )
);

CREATE TABLE artifact_version_retirements (
    version_id TEXT PRIMARY KEY,
    lifecycle_run_id UUID NOT NULL,
    artifact_kind TEXT NOT NULL,
    reason TEXT NOT NULL,
    retired_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,

    CONSTRAINT artifact_version_retirements_version_fk
        FOREIGN KEY (
            version_id
        )
        REFERENCES artifact_versions (
            version_id
        )
        ON DELETE RESTRICT,

    CONSTRAINT artifact_version_retirements_run_fk
        FOREIGN KEY (
            lifecycle_run_id
        )
        REFERENCES lifecycle_runs (
            run_id
        )
        ON DELETE RESTRICT,

    CONSTRAINT artifact_version_retirements_artifact_kind_check
        CHECK (
            artifact_kind IN (
                'generic',
                'model-checkpoint',
                'dataset-snapshot',
                'embedding-index',
                'evaluation-output'
            )
        ),

    CONSTRAINT artifact_version_retirements_reason_check
        CHECK (
            reason = 'policy-retire'
        )
);

INSERT INTO schema_migrations (
    version,
    name
)
VALUES (
    10,
    'ai_aware_lifecycle'
);

COMMIT;
