-- database/init-scripts/01-schema.sql
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
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE subject_alias (
    alias VARCHAR(14) NOT NULL,
    global_subject_id VARCHAR(21) REFERENCES subjects(global_subject_id),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE local_subject_ids (
    center_id INT,
    local_subject_id VARCHAR,
    identifier_type VARCHAR DEFAULT 'primary',
    global_subject_id VARCHAR(21) REFERENCES subjects(global_subject_id),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (center_id, local_subject_id, identifier_type)
);

CREATE TABLE identity_resolutions (
    resolution_id SERIAL PRIMARY KEY,
    input_center_id INT NOT NULL,
    input_local_id VARCHAR NOT NULL,
    matched_gsid VARCHAR(21) REFERENCES subjects(global_subject_id),
    action VARCHAR NOT NULL,
    match_strategy VARCHAR NOT NULL,
    confidence_score DECIMAL(3,2) NOT NULL,
    requires_review BOOLEAN DEFAULT FALSE,
    review_reason TEXT,
    reviewed_by VARCHAR,
    reviewed_at TIMESTAMP,
    resolution_notes TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    created_by VARCHAR DEFAULT 'system'
);

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
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
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
    CONSTRAINT valid_source_table CHECK (source_table IN ('specimen', 'lcl', 'enteroid', 'sequence', 'genotype', 'olink'))
);

-- Indexes
CREATE INDEX idx_subjects_center ON subjects(center_id);
CREATE INDEX idx_subjects_family ON subjects(family_id);
CREATE INDEX idx_local_ids_gsid ON local_subject_ids(global_subject_id);
CREATE INDEX idx_local_ids_lookup ON local_subject_ids(center_id, local_subject_id);
CREATE INDEX idx_subjects_withdrawn ON subjects(withdrawn) WHERE withdrawn = TRUE;
CREATE INDEX idx_subjects_flagged ON subjects(flagged_for_review) WHERE flagged_for_review = TRUE;
CREATE INDEX idx_resolutions_review ON identity_resolutions(requires_review) WHERE requires_review = TRUE;
CREATE INDEX idx_resolutions_gsid ON identity_resolutions(matched_gsid);
CREATE INDEX idx_resolutions_input ON identity_resolutions(input_center_id, input_local_id);
CREATE INDEX idx_sample_resolutions_review ON sample_resolutions(requires_review) WHERE requires_review = TRUE;
CREATE INDEX idx_sample_resolutions_sample ON sample_resolutions(sample_id, source_table);
CREATE INDEX idx_sample_resolutions_gsid ON sample_resolutions(global_subject_id);
CREATE INDEX idx_sample_resolutions_source ON sample_resolutions(source_table);

-- Triggers
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
