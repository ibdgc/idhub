-- database/init-scripts/01-schema.sql
-- Full schema with ULID-based GSIDs

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
    global_subject_id VARCHAR(12) PRIMARY KEY,
    center_id INT NOT NULL REFERENCES centers(center_id),
    registration_year INTEGER,
    control BOOLEAN DEFAULT FALSE,
    withdrawn BOOLEAN DEFAULT FALSE,
    family_id VARCHAR REFERENCES family(family_id),
    flagged_for_review BOOLEAN DEFAULT FALSE,
    review_notes TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE local_subject_ids (
    center_id INT,
    local_subject_id VARCHAR,
    identifier_type VARCHAR DEFAULT 'primary',
    global_subject_id VARCHAR(12) REFERENCES subjects(global_subject_id),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (center_id, local_subject_id, identifier_type)
);

CREATE TABLE subject_alias (
    alias VARCHAR(14) NOT NULL,
    global_subject_id VARCHAR(12) REFERENCES subjects(global_subject_id),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE lcl (
    knumber VARCHAR(7) PRIMARY KEY,
    global_subject_id VARCHAR(12) REFERENCES subjects(global_subject_id),
    niddk_no INT NOT NULL,
    date_collected DATE,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE blood (
    sample_id VARCHAR PRIMARY KEY,
    global_subject_id VARCHAR(12) REFERENCES subjects(global_subject_id),
    sample_type VARCHAR(6),
    date_collected DATE,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE dna (
    sample_id VARCHAR PRIMARY KEY,
    global_subject_id VARCHAR(12) REFERENCES subjects(global_subject_id),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE wgs (
    sample_id VARCHAR PRIMARY KEY,
    global_subject_id VARCHAR(12) REFERENCES subjects(global_subject_id),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE immunochip (
    sample_id VARCHAR PRIMARY KEY,
    global_subject_id VARCHAR(12) REFERENCES subjects(global_subject_id),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE bge (
    sample_id VARCHAR PRIMARY KEY,
    global_subject_id VARCHAR(12) REFERENCES subjects(global_subject_id),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE exomechip (
    sample_id VARCHAR PRIMARY KEY,
    global_subject_id VARCHAR(12) REFERENCES subjects(global_subject_id),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE gwas2 (
    sample_id VARCHAR PRIMARY KEY,
    global_subject_id VARCHAR(12) REFERENCES subjects(global_subject_id),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE plasma (
    sample_id VARCHAR PRIMARY KEY,
    global_subject_id VARCHAR(12) REFERENCES subjects(global_subject_id),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE enteroid (
    sample_id VARCHAR PRIMARY KEY,
    global_subject_id VARCHAR(12) REFERENCES subjects(global_subject_id),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE olink (
    sample_id VARCHAR PRIMARY KEY,
    global_subject_id VARCHAR(12) REFERENCES subjects(global_subject_id),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE rnaseq (
    sample_id VARCHAR PRIMARY KEY,
    global_subject_id VARCHAR(12) REFERENCES subjects(global_subject_id),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE wes (
    seq_id TEXT PRIMARY KEY,
    global_subject_id VARCHAR(12) REFERENCES subjects(global_subject_id),
    batch TEXT,
    vcf_sample_id VARCHAR,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE genotyping (
    genotype_id TEXT PRIMARY KEY,
    global_subject_id VARCHAR(12) REFERENCES subjects(global_subject_id),
    genotyping_project TEXT,
    genotyping_barcode TEXT,
    batch TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE identity_resolutions (
    resolution_id SERIAL PRIMARY KEY,
    input_center_id INT NOT NULL,
    input_local_id VARCHAR NOT NULL,
    matched_gsid VARCHAR(12) REFERENCES subjects(global_subject_id),
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
