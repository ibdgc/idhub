-- ============================================================================
-- CORE TABLES
-- ============================================================================
CREATE TABLE centers (
    center_id SERIAL PRIMARY KEY,
    name VARCHAR NOT NULL,
    investigator VARCHAR NOT NULL,
    country VARCHAR,
    consortium VARCHAR,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE family (
    family_id VARCHAR PRIMARY KEY,
    public_id INT,
    public_family_id INT,
    local_pedigree VARCHAR,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE subjects (
    global_subject_id VARCHAR(21) PRIMARY KEY,
    center_id INT NOT NULL REFERENCES centers(center_id),
    registration_year DATE,
    control BOOLEAN DEFAULT FALSE,
    withdrawn BOOLEAN DEFAULT FALSE,
    family_id VARCHAR REFERENCES family(family_id),
    flagged_for_review BOOLEAN DEFAULT FALSE,
    review_notes TEXT,
    created_by VARCHAR(100) DEFAULT 'system',
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE subject_alias (
    alias VARCHAR(14) NOT NULL,
    global_subject_id VARCHAR(21) REFERENCES subjects(global_subject_id),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- ============================================================================
-- LOCAL SUBJECT IDS - Multi-Candidate Support
-- ============================================================================
CREATE TABLE local_subject_ids (
    center_id INT,
    local_subject_id VARCHAR,
    identifier_type VARCHAR DEFAULT 'primary',
    global_subject_id VARCHAR(21) REFERENCES subjects(global_subject_id),
    created_by VARCHAR(100) DEFAULT 'system',
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (center_id, local_subject_id, identifier_type)
);

-- ============================================================================
-- IDENTITY RESOLUTIONS - Enhanced for Center-Agnostic Matching
-- ============================================================================
CREATE TABLE identity_resolutions (
    resolution_id SERIAL PRIMARY KEY,
    
    -- Input identifiers
    local_subject_id VARCHAR NOT NULL,
    identifier_type VARCHAR NOT NULL,
    input_center_id INT NOT NULL,
    
    -- Resolution results
    gsid VARCHAR(21),
    matched_gsid VARCHAR(21) REFERENCES subjects(global_subject_id),
    action VARCHAR NOT NULL,
    match_strategy VARCHAR NOT NULL,
    confidence DECIMAL(3,2),
    
    -- Multi-candidate tracking
    candidate_ids JSONB,  -- Array of all candidate IDs submitted
    matched_gsids JSONB,  -- Array of GSIDs matched (for conflicts)
    
    -- Review tracking
    requires_review BOOLEAN DEFAULT FALSE,
    review_reason TEXT,
    reviewed_by VARCHAR,
    reviewed_at TIMESTAMP,
    resolution_notes TEXT,
    
    -- Validation tracking
    validation_warnings JSONB,  -- Array of validation warnings
    
    -- Metadata
    metadata JSONB,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    created_by VARCHAR DEFAULT 'system',
    
    -- Constraints
    CONSTRAINT valid_action CHECK (action IN (
        'create_new',
        'link_existing',
        'conflict_resolved',  -- NEW: For multi-GSID conflicts
        'center_promoted',
        'review_required',
        'error'
    )),
    CONSTRAINT valid_match_strategy CHECK (match_strategy IN (
        'no_match',
        'exact_match',
        'center_agnostic_match',  -- NEW: Match on local_subject_id alone
        'exact_withdrawn',
        'center_promotion',
        'multiple_gsid_conflict',  -- Multi-GSID conflict (same ID → different GSIDs)
        'center_mismatch',  -- NEW: Same ID, different centers
        'cross_center_conflict',
        'validation_failed',
        'error'
    ))
);

-- ============================================================================
-- DATA CHANGE AUDIT - Track all data modifications
-- ============================================================================
CREATE TABLE data_change_audit (
    id SERIAL PRIMARY KEY,
    table_name VARCHAR(100) NOT NULL,
    record_key JSONB NOT NULL,  -- Natural key values: {"global_subject_id": "GSID-001", "sample_id": "SMP050"}
    changes JSONB NOT NULL,      -- Field changes: {"volume_ml": {"old": 5.0, "new": 5.5}}
    changed_by VARCHAR(255),     -- Source: "fragment_loader", "redcap_pipeline", etc.
    changed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    batch_id VARCHAR(100),       -- Link to fragment batch
    source_fragment VARCHAR(500) -- S3 path to source file
);

-- ============================================================================
-- SAMPLE TABLES
-- ============================================================================
CREATE TABLE specimen (
    sample_id VARCHAR PRIMARY KEY,
    global_subject_id VARCHAR(21) REFERENCES subjects(global_subject_id),
    sample_type VARCHAR,
    year_collected DATE,
    redcap_event VARCHAR,
    region_location VARCHAR,
    sample_available BOOLEAN DEFAULT TRUE,
    project VARCHAR,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE lcl (
    niddk_no INT PRIMARY KEY,
    knumber VARCHAR(7),
    global_subject_id VARCHAR(21) REFERENCES subjects(global_subject_id),
    date_collected DATE,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE (global_subject_id, knumber)
);

CREATE TABLE enteroid (
    sample_id VARCHAR PRIMARY KEY,
    global_subject_id VARCHAR(21) REFERENCES subjects(global_subject_id),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE sequence (
    sample_id TEXT PRIMARY KEY,
    global_subject_id VARCHAR(21) REFERENCES subjects(global_subject_id),
    sample_type VARCHAR,
    batch TEXT,
    vcf_sample_id VARCHAR,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE genotype (
    genotype_id TEXT PRIMARY KEY,
    global_subject_id VARCHAR(21) REFERENCES subjects(global_subject_id),
    genotyping_project TEXT,
    genotyping_barcode TEXT,
    batch TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE olink (
    sample_id VARCHAR PRIMARY KEY,
    global_subject_id VARCHAR(21) REFERENCES subjects(global_subject_id),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- ============================================================================
-- RESOLUTION TRACKING TABLES
-- ============================================================================
CREATE TABLE sample_resolutions (
    resolution_id SERIAL PRIMARY KEY,
    sample_id VARCHAR NOT NULL,
    sample_type VARCHAR NOT NULL,
    source_table VARCHAR NOT NULL,
    global_subject_id VARCHAR(21) REFERENCES subjects(global_subject_id),
    requires_review BOOLEAN DEFAULT FALSE,
    review_reason TEXT,
    reviewed_by VARCHAR,
    reviewed_at TIMESTAMP,
    resolution_notes TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    created_by VARCHAR DEFAULT 'system',
    
    CONSTRAINT valid_source_table CHECK (source_table IN (
        'specimen',
        'lcl',
        'enteroid',
        'sequence',
        'genotype',
        'olink'
    ))
);

CREATE TABLE fragment_resolutions (
    resolution_id SERIAL PRIMARY KEY,
    batch_id VARCHAR NOT NULL,
    table_name VARCHAR NOT NULL,
    fragment_key VARCHAR NOT NULL,
    load_status VARCHAR NOT NULL,
    load_strategy VARCHAR NOT NULL,
    rows_attempted INT DEFAULT 0,
    rows_loaded INT DEFAULT 0,
    rows_failed INT DEFAULT 0,
    execution_time_ms INT,
    error_message TEXT,
    requires_review BOOLEAN DEFAULT FALSE,
    review_reason TEXT,
    reviewed_by VARCHAR,
    reviewed_at TIMESTAMP,
    resolution_notes TEXT,
    metadata JSONB,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    created_by VARCHAR DEFAULT 'table_loader',
    
    CONSTRAINT valid_load_status CHECK (load_status IN (
        'success',
        'partial',
        'failed',
        'skipped',
        'preview'
    )),
    CONSTRAINT valid_load_strategy CHECK (load_strategy IN (
        'standard_insert',
        'upsert'
    ))
);

CREATE TYPE conflict_type_enum AS ENUM (
    'center_mismatch',
    'duplicate_id',
    'multi_gsid'
);

CREATE TYPE resolution_action_enum AS ENUM (
    'keep_existing',
    'use_incoming',
    'delete_both',
    'merge',
    'pending'
);

CREATE TABLE conflict_resolutions (
    id SERIAL PRIMARY KEY,
    batch_id VARCHAR(100) NOT NULL,
    conflict_type conflict_type_enum NOT NULL,

    -- Conflicting record details
    local_subject_id VARCHAR(255) NOT NULL,
    identifier_type VARCHAR(50) DEFAULT 'consortium_id',

    -- Center conflict details
    existing_center_id INTEGER,
    incoming_center_id INTEGER,
    existing_gsid VARCHAR(21),
    incoming_gsid VARCHAR(21),

    -- Resolution decision
    resolution_action resolution_action_enum,
    resolution_notes TEXT,
    resolved BOOLEAN DEFAULT FALSE NOT NULL,  -- Replaces status='applied'

    -- Metadata
    detected_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    resolved_at TIMESTAMP,
    resolved_by VARCHAR(100),

    -- Audit trail
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- ============================================================================
-- INDEXES
-- ============================================================================
-- Subject indexes
CREATE INDEX idx_subjects_center ON subjects(center_id);
CREATE INDEX idx_subjects_family ON subjects(family_id);
CREATE INDEX idx_subjects_withdrawn ON subjects(withdrawn) WHERE withdrawn = TRUE;
CREATE INDEX idx_subjects_flagged ON subjects(flagged_for_review) WHERE flagged_for_review = TRUE;
CREATE INDEX idx_subjects_created_by ON subjects(created_by);

-- Local subject IDs indexes
CREATE INDEX idx_local_ids_gsid ON local_subject_ids(global_subject_id);
CREATE INDEX idx_local_ids_lookup ON local_subject_ids(center_id, local_subject_id);
CREATE INDEX idx_local_ids_type ON local_subject_ids(identifier_type);
CREATE INDEX idx_local_ids_composite ON local_subject_ids(global_subject_id, identifier_type);
CREATE INDEX idx_local_ids_created_by ON local_subject_ids(created_by);
-- NEW: Center-agnostic lookup (for matching on local_subject_id alone)
CREATE INDEX idx_local_ids_subject_only ON local_subject_ids(local_subject_id, identifier_type);

-- Identity resolutions indexes
CREATE INDEX idx_resolutions_review ON identity_resolutions(requires_review) WHERE requires_review = TRUE;
CREATE INDEX idx_resolutions_gsid ON identity_resolutions(matched_gsid);
CREATE INDEX idx_resolutions_input ON identity_resolutions(input_center_id, local_subject_id);
CREATE INDEX idx_resolutions_action ON identity_resolutions(action);
CREATE INDEX idx_resolutions_strategy ON identity_resolutions(match_strategy);
CREATE INDEX idx_resolutions_created ON identity_resolutions(created_at DESC);
CREATE INDEX idx_resolutions_identifier_type ON identity_resolutions(identifier_type);
-- JSONB indexes for multi-candidate queries
CREATE INDEX idx_resolutions_candidate_ids ON identity_resolutions USING GIN (candidate_ids);
CREATE INDEX idx_resolutions_matched_gsids ON identity_resolutions USING GIN (matched_gsids);
CREATE INDEX idx_resolutions_validation_warnings ON identity_resolutions USING GIN (validation_warnings);

-- Data change audit indexes
CREATE INDEX idx_audit_table ON data_change_audit(table_name);
CREATE INDEX idx_audit_time ON data_change_audit(changed_at);
CREATE INDEX idx_audit_batch ON data_change_audit(batch_id);
CREATE INDEX idx_audit_record_key ON data_change_audit USING GIN(record_key);
CREATE INDEX idx_audit_changed_by ON data_change_audit(changed_by);

-- Sample resolutions indexes
CREATE INDEX idx_sample_resolutions_review ON sample_resolutions(requires_review) WHERE requires_review = TRUE;
CREATE INDEX idx_sample_resolutions_sample ON sample_resolutions(sample_id, source_table);
CREATE INDEX idx_sample_resolutions_gsid ON sample_resolutions(global_subject_id);
CREATE INDEX idx_sample_resolutions_source ON sample_resolutions(source_table);

-- Fragment resolutions indexes
CREATE INDEX idx_fragment_resolutions_batch ON fragment_resolutions(batch_id);
CREATE INDEX idx_fragment_resolutions_table ON fragment_resolutions(table_name);
CREATE INDEX idx_fragment_resolutions_status ON fragment_resolutions(load_status);
CREATE INDEX idx_fragment_resolutions_review ON fragment_resolutions(requires_review) WHERE requires_review = TRUE;
CREATE INDEX idx_fragment_resolutions_created ON fragment_resolutions(created_at DESC);
CREATE UNIQUE INDEX idx_fragment_resolutions_unique ON fragment_resolutions(batch_id, table_name, fragment_key);
CREATE INDEX idx_conflict_resolutions_batch ON conflict_resolutions(batch_id);
CREATE INDEX idx_conflict_resolutions_resolved ON conflict_resolutions(resolved);
CREATE INDEX idx_conflict_resolutions_local_id ON conflict_resolutions(local_subject_id);
CREATE INDEX idx_conflict_resolutions_pending ON conflict_resolutions(resolution_action) WHERE resolution_action IS NULL;

-- ============================================================================
-- TRIGGERS
-- ============================================================================
CREATE OR REPLACE FUNCTION update_updated_at()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = CURRENT_TIMESTAMP;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER subjects_updated_at
    BEFORE UPDATE ON subjects
    FOR EACH ROW
    EXECUTE FUNCTION update_updated_at();

CREATE TRIGGER local_subject_ids_updated_at
    BEFORE UPDATE ON local_subject_ids
    FOR EACH ROW
    EXECUTE FUNCTION update_updated_at();

-- ============================================================================
-- HELPER FUNCTIONS
-- ============================================================================
-- Function to get all local IDs for a GSID
CREATE OR REPLACE FUNCTION get_local_ids_for_gsid(p_gsid VARCHAR)
RETURNS TABLE (
    center_id INT,
    local_subject_id VARCHAR,
    identifier_type VARCHAR,
    created_by VARCHAR,
    created_at TIMESTAMP
) AS $$
BEGIN
    RETURN QUERY
    SELECT 
        l.center_id,
        l.local_subject_id,
        l.identifier_type,
        l.created_by,
        l.created_at
    FROM local_subject_ids l
    WHERE l.global_subject_id = p_gsid
    ORDER BY l.created_at ASC;
END;
$$ LANGUAGE plpgsql;

-- NEW: Function to check for center conflicts (same ID, different centers)
CREATE OR REPLACE FUNCTION check_center_conflicts(
    p_local_subject_id VARCHAR,
    p_identifier_type VARCHAR)
RETURNS TABLE (
    global_subject_id VARCHAR,
    center_id INT,
    center_name VARCHAR,
    conflict_detected BOOLEAN
) AS $$
BEGIN
    RETURN QUERY
    WITH id_centers AS (
        SELECT 
            l.global_subject_id,
            l.center_id,
            c.name as center_name
        FROM local_subject_ids l
        JOIN centers c ON l.center_id = c.center_id
        WHERE l.local_subject_id = p_local_subject_id
          AND l.identifier_type = p_identifier_type
    )
    SELECT 
        ic.global_subject_id,
        ic.center_id,
        ic.center_name,
        COUNT(DISTINCT ic.center_id) OVER () > 1 as conflict_detected
    FROM id_centers ic;
END;
$$ LANGUAGE plpgsql;

-- Function to check for multi-GSID conflicts
CREATE OR REPLACE FUNCTION check_multi_gsid_conflicts(
    p_candidate_ids JSONB)
RETURNS TABLE (
    local_subject_id VARCHAR,
    identifier_type VARCHAR,
    global_subject_id VARCHAR,
    center_id INT,
    conflict_detected BOOLEAN
) AS $$
BEGIN
    RETURN QUERY
    WITH candidate_list AS (
        SELECT 
            (value->>'local_subject_id')::VARCHAR as local_id,
            (value->>'identifier_type')::VARCHAR as id_type
        FROM jsonb_array_elements(p_candidate_ids)
    )
    SELECT 
        c.local_id,
        c.id_type,
        l.global_subject_id,
        l.center_id,
        COUNT(DISTINCT l.global_subject_id) OVER () > 1 as conflict_detected
    FROM candidate_list c
    LEFT JOIN local_subject_ids l 
        ON l.local_subject_id = c.local_id
        AND l.identifier_type = c.id_type
    WHERE l.global_subject_id IS NOT NULL;
END;
$$ LANGUAGE plpgsql;

-- Function to get resolution statistics
CREATE OR REPLACE FUNCTION get_resolution_stats(
    p_start_date TIMESTAMP DEFAULT NULL,
    p_end_date TIMESTAMP DEFAULT NULL)
RETURNS TABLE (
    action VARCHAR,
    match_strategy VARCHAR,
    count BIGINT,
    avg_confidence DECIMAL
) AS $$
BEGIN
    RETURN QUERY
    SELECT 
        ir.action,
        ir.match_strategy,
        COUNT(*) as count,
        AVG(ir.confidence) as avg_confidence
    FROM identity_resolutions ir
    WHERE 
        (p_start_date IS NULL OR ir.created_at >= p_start_date)
        AND (p_end_date IS NULL OR ir.created_at <= p_end_date)
    GROUP BY ir.action, ir.match_strategy
    ORDER BY count DESC;
END;
$$ LANGUAGE plpgsql;

-- Function to get subjects with multiple identifier types
CREATE OR REPLACE FUNCTION get_subjects_with_multiple_id_types()
RETURNS TABLE (
    global_subject_id VARCHAR,
    center_id INT,
    identifier_types TEXT[],
    id_count BIGINT,
    created_by_sources TEXT[]
) AS $$
BEGIN
    RETURN QUERY
    SELECT 
        l.global_subject_id,
        l.center_id,
        ARRAY_AGG(DISTINCT l.identifier_type ORDER BY l.identifier_type) as identifier_types,
        COUNT(DISTINCT l.identifier_type) as id_count,
        ARRAY_AGG(DISTINCT l.created_by ORDER BY l.created_by) as created_by_sources
    FROM local_subject_ids l
    GROUP BY l.global_subject_id, l.center_id
    HAVING COUNT(DISTINCT l.identifier_type) > 1
    ORDER BY id_count DESC, l.global_subject_id;
END;
$$ LANGUAGE plpgsql;

-- Function to get subjects by source
CREATE OR REPLACE FUNCTION get_subjects_by_source(p_source VARCHAR)
RETURNS TABLE (
    global_subject_id VARCHAR,
    center_id INT,
    center_name VARCHAR,
    registration_year DATE,
    control BOOLEAN,
    num_local_ids BIGINT,
    created_at TIMESTAMP
) AS $$
BEGIN
    RETURN QUERY
    SELECT 
        s.global_subject_id,
        s.center_id,
        c.name as center_name,
        s.registration_year,
        s.control,
        COUNT(l.local_subject_id) as num_local_ids,
        s.created_at
    FROM subjects s
    JOIN centers c ON s.center_id = c.center_id
    LEFT JOIN local_subject_ids l ON s.global_subject_id = l.global_subject_id
    WHERE s.created_by = p_source
    GROUP BY s.global_subject_id, s.center_id, c.name, s.registration_year, 
             s.control, s.created_at
    ORDER BY s.created_at DESC;
END;
$$ LANGUAGE plpgsql;

-- NEW: Function to find duplicate subjects (same local_subject_id across centers)
CREATE OR REPLACE FUNCTION find_duplicate_subjects()
RETURNS TABLE (
    local_subject_id VARCHAR,
    identifier_type VARCHAR,
    gsid_count BIGINT,
    gsids TEXT[],
    center_ids INT[],
    center_names TEXT[]
) AS $$
BEGIN
    RETURN QUERY
    SELECT 
        l.local_subject_id,
        l.identifier_type,
        COUNT(DISTINCT l.global_subject_id) as gsid_count,
        ARRAY_AGG(DISTINCT l.global_subject_id ORDER BY l.global_subject_id) as gsids,
        ARRAY_AGG(DISTINCT l.center_id ORDER BY l.center_id) as center_ids,
        ARRAY_AGG(DISTINCT c.name ORDER BY c.name) as center_names
    FROM local_subject_ids l
    JOIN centers c ON l.center_id = c.center_id
    GROUP BY l.local_subject_id, l.identifier_type
    HAVING COUNT(DISTINCT l.global_subject_id) > 1
    ORDER BY gsid_count DESC, l.local_subject_id;
END;
$$ LANGUAGE plpgsql;

-- NEW: Function to query audit trail for a specific record
CREATE OR REPLACE FUNCTION get_change_history(
    p_table_name VARCHAR,
    p_record_key JSONB,
    p_limit INT DEFAULT 100)
RETURNS TABLE (
    id INT,
    changes JSONB,
    changed_by VARCHAR,
    changed_at TIMESTAMP,
    batch_id VARCHAR,
    source_fragment VARCHAR
) AS $$
BEGIN
    RETURN QUERY
    SELECT 
        a.id,
        a.changes,
        a.changed_by,
        a.changed_at,
        a.batch_id,
        a.source_fragment
    FROM data_change_audit a
    WHERE a.table_name = p_table_name
      AND a.record_key = p_record_key
    ORDER BY a.changed_at DESC
    LIMIT p_limit;
END;
$$ LANGUAGE plpgsql;

-- NEW: Function to get change statistics by table
CREATE OR REPLACE FUNCTION get_change_stats_by_table(
    p_start_date TIMESTAMP DEFAULT NULL,
    p_end_date TIMESTAMP DEFAULT NULL)
RETURNS TABLE (
    table_name VARCHAR,
    total_changes BIGINT,
    unique_records BIGINT,
    changed_by_sources TEXT[],
    first_change TIMESTAMP,
    last_change TIMESTAMP
) AS $$
BEGIN
    RETURN QUERY
    SELECT 
        a.table_name,
        COUNT(*) as total_changes,
        COUNT(DISTINCT a.record_key) as unique_records,
        ARRAY_AGG(DISTINCT a.changed_by ORDER BY a.changed_by) as changed_by_sources,
        MIN(a.changed_at) as first_change,
        MAX(a.changed_at) as last_change
    FROM data_change_audit a
    WHERE 
        (p_start_date IS NULL OR a.changed_at >= p_start_date)
        AND (p_end_date IS NULL OR a.changed_at <= p_end_date)
    GROUP BY a.table_name
    ORDER BY total_changes DESC;
END;
$$ LANGUAGE plpgsql;

-- ============================================================================
-- VIEWS
-- ============================================================================
-- View for subjects with review flags
CREATE OR REPLACE VIEW v_subjects_requiring_review AS
SELECT 
    s.global_subject_id,
    s.center_id,
    c.name as center_name,
    s.flagged_for_review,
    s.review_notes,
    s.withdrawn,
    s.created_by,
    COUNT(DISTINCT l.identifier_type) as num_identifier_types,
    COUNT(DISTINCT l.local_subject_id) as num_local_ids,
    s.created_at,
    s.updated_at
FROM subjects s
LEFT JOIN centers c ON s.center_id = c.center_id
LEFT JOIN local_subject_ids l ON s.global_subject_id = l.global_subject_id
WHERE s.flagged_for_review = TRUE OR s.withdrawn = TRUE
GROUP BY s.global_subject_id, s.center_id, c.name, s.flagged_for_review, 
         s.review_notes, s.withdrawn, s.created_by, s.created_at, s.updated_at
ORDER BY s.updated_at DESC;

-- View for multi-GSID conflicts
CREATE OR REPLACE VIEW v_multi_gsid_conflicts AS
SELECT 
    ir.resolution_id,
    ir.local_subject_id,
    ir.identifier_type,
    ir.input_center_id,
    ir.matched_gsids,
    jsonb_array_length(ir.matched_gsids) as num_matched_gsids,
    ir.review_reason,
    ir.requires_review,
    ir.reviewed_by,
    ir.reviewed_at,
    ir.created_by,
    ir.created_at
FROM identity_resolutions ir
WHERE ir.match_strategy = 'multiple_gsid_conflict'
  AND ir.matched_gsids IS NOT NULL
  AND jsonb_array_length(ir.matched_gsids) > 1
ORDER BY ir.created_at DESC;

-- NEW: View for center conflicts
CREATE OR REPLACE VIEW v_center_conflicts AS
SELECT 
    ir.resolution_id,
    ir.local_subject_id,
    ir.identifier_type,
    ir.input_center_id,
    ir.gsid,
    ir.review_reason,
    ir.requires_review,
    ir.reviewed_by,
    ir.reviewed_at,
    ir.created_by,
    ir.created_at
FROM identity_resolutions ir
WHERE ir.match_strategy = 'center_mismatch'
  AND ir.requires_review = TRUE
ORDER BY ir.created_at DESC;

-- View for resolution summary by center
CREATE OR REPLACE VIEW v_resolution_summary_by_center AS
SELECT 
    c.center_id,
    c.name as center_name,
    ir.action,
    ir.match_strategy,
    COUNT(*) as resolution_count,
    AVG(ir.confidence) as avg_confidence,
    SUM(CASE WHEN ir.requires_review THEN 1 ELSE 0 END) as review_count
FROM identity_resolutions ir
JOIN centers c ON ir.input_center_id = c.center_id
GROUP BY c.center_id, c.name, ir.action, ir.match_strategy
ORDER BY c.name, resolution_count DESC;

-- View for subjects by source
CREATE OR REPLACE VIEW v_subjects_by_source AS
SELECT 
    s.created_by as source,
    COUNT(DISTINCT s.global_subject_id) as subject_count,
    COUNT(DISTINCT s.center_id) as center_count,
    MIN(s.created_at) as first_created,
    MAX(s.created_at) as last_created
FROM subjects s
GROUP BY s.created_by
ORDER BY subject_count DESC;

-- NEW: View for recent data changes
CREATE OR REPLACE VIEW v_recent_data_changes AS
SELECT 
    a.id,
    a.table_name,
    a.record_key,
    a.changes,
    a.changed_by,
    a.changed_at,
    a.batch_id,
    jsonb_object_keys(a.changes) as changed_fields_count
FROM data_change_audit a
ORDER BY a.changed_at DESC
LIMIT 1000;

-- ============================================================================
-- COMMENTS
-- ============================================================================
COMMENT ON TABLE subjects IS 'Core subject registry with GSID as primary key';
COMMENT ON COLUMN subjects.created_by IS 'Source system that created this subject (e.g., redcap_pipeline, fragment_validator, manual)';

COMMENT ON TABLE local_subject_ids IS 'Maps local subject IDs to global GSIDs with identifier type tracking';
COMMENT ON COLUMN local_subject_ids.identifier_type IS 'Type of identifier: consortium_id, local_id, alias, niddk_no, etc.';
COMMENT ON COLUMN local_subject_ids.created_by IS 'Source system that created this mapping';

COMMENT ON TABLE identity_resolutions IS 'Tracks all identity resolution decisions with center-agnostic matching support';
COMMENT ON COLUMN identity_resolutions.candidate_ids IS 'JSONB array of all candidate IDs submitted for resolution';
COMMENT ON COLUMN identity_resolutions.matched_gsids IS 'JSONB array of GSIDs matched (populated for conflicts)';
COMMENT ON COLUMN identity_resolutions.validation_warnings IS 'JSONB array of ID validation warnings';

COMMENT ON TABLE data_change_audit IS 'Audit trail of all data modifications for reproducibility and tracking';
COMMENT ON COLUMN data_change_audit.record_key IS 'Natural key identifying the record (e.g., {"global_subject_id": "GSID-001", "sample_id": "SMP050"})';
COMMENT ON COLUMN data_change_audit.changes IS 'Fields that changed with old and new values (e.g., {"volume_ml": {"old": 5.0, "new": 5.5}})';
COMMENT ON COLUMN data_change_audit.changed_by IS 'Source system that made the change (fragment_loader, redcap_pipeline, manual, etc.)';
COMMENT ON COLUMN data_change_audit.batch_id IS 'Fragment batch ID that triggered this change';
COMMENT ON COLUMN data_change_audit.source_fragment IS 'S3 path to the source file that contained this change';

COMMENT ON FUNCTION get_local_ids_for_gsid IS 'Returns all local IDs associated with a GSID';
COMMENT ON FUNCTION check_center_conflicts IS 'Checks if a local_subject_id exists with different centers';
COMMENT ON FUNCTION check_multi_gsid_conflicts IS 'Checks if candidate IDs map to multiple different GSIDs';
COMMENT ON FUNCTION get_resolution_stats IS 'Returns resolution statistics for a date range';
COMMENT ON FUNCTION get_subjects_with_multiple_id_types IS 'Returns subjects with multiple identifier types';
COMMENT ON FUNCTION get_subjects_by_source IS 'Returns subjects grouped by source system';
COMMENT ON FUNCTION find_duplicate_subjects IS 'Finds subjects with same local_subject_id linked to different GSIDs';
COMMENT ON FUNCTION get_change_history IS 'Returns change history for a specific record';
COMMENT ON FUNCTION get_change_stats_by_table IS 'Returns change statistics grouped by table';

COMMENT ON VIEW v_subjects_requiring_review IS 'Subjects flagged for review or withdrawn';
COMMENT ON VIEW v_multi_gsid_conflicts IS 'Identity resolutions with multiple GSID conflicts';
COMMENT ON VIEW v_center_conflicts IS 'Identity resolutions with center mismatches';
COMMENT ON VIEW v_resolution_summary_by_center IS 'Resolution statistics grouped by center';
COMMENT ON VIEW v_subjects_by_source IS 'Subject counts grouped by source system';
COMMENT ON VIEW v_recent_data_changes IS 'Most recent data changes across all tables';
COMMENT ON TABLE conflict_resolutions IS 'Tracks data conflicts requiring manual review and resolution';
COMMENT ON COLUMN conflict_resolutions.resolution_action IS 'NULL=pending review | keep_existing/use_incoming/delete_both/merge=resolved but not applied';
COMMENT ON COLUMN conflict_resolutions.resolved IS 'TRUE when resolution has been applied to database';
