-- database/schema.sql
-- idHub Core Schema

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
    global_subject_id SERIAL PRIMARY KEY,
    center_id INT NOT NULL REFERENCES centers(center_id),
    registration_year DATE,
    control BOOLEAN DEFAULT FALSE,
    withdrawn BOOLEAN DEFAULT FALSE,
    family_id VARCHAR REFERENCES family(family_id),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE local_subject_ids (
    center_id INT,
    local_subject_id VARCHAR,
    identifier_type VARCHAR,
    global_subject_id INT REFERENCES subjects(global_subject_id),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (center_id, local_subject_id)
);

CREATE TABLE subject_alias (
    alias VARCHAR(14) NOT NULL,
    global_subject_id INT REFERENCES subjects(global_subject_id),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE lcl (
    knumber VARCHAR(7) PRIMARY KEY,
    global_subject_id INT REFERENCES subjects(global_subject_id),
    niddk_no INT NOT NULL,
    date_collected DATE,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE blood (
    sample_id VARCHAR PRIMARY KEY,
    global_subject_id INT REFERENCES subjects(global_subject_id),
    sample_type VARCHAR(6),
    date_collected DATE,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE dna (
    sample_id VARCHAR PRIMARY KEY,
    global_subject_id INT REFERENCES subjects(global_subject_id),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE wgs (
    sample_id VARCHAR PRIMARY KEY,
    global_subject_id INT REFERENCES subjects(global_subject_id),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE immunochip (
    sample_id VARCHAR PRIMARY KEY,
    global_subject_id INT REFERENCES subjects(global_subject_id),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE bge (
    sample_id VARCHAR PRIMARY KEY,
    global_subject_id INT REFERENCES subjects(global_subject_id),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE exomechip (
    sample_id VARCHAR PRIMARY KEY,
    global_subject_id INT REFERENCES subjects(global_subject_id),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE gwas2 (
    sample_id VARCHAR PRIMARY KEY,
    global_subject_id INT REFERENCES subjects(global_subject_id),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE plasma (
    sample_id VARCHAR PRIMARY KEY,
    global_subject_id INT REFERENCES subjects(global_subject_id),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE enteroid (
    sample_id VARCHAR PRIMARY KEY,
    global_subject_id INT REFERENCES subjects(global_subject_id),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE olink (
    sample_id VARCHAR PRIMARY KEY,
    global_subject_id INT REFERENCES subjects(global_subject_id),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE rnaseq (
    sample_id VARCHAR PRIMARY KEY,
    global_subject_id INT REFERENCES subjects(global_subject_id),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE wes (
    seq_id TEXT PRIMARY KEY,
    global_subject_id INT REFERENCES subjects(global_subject_id),
    batch TEXT,
    vcf_sample_id VARCHAR,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE genotyping (
    genotype_id TEXT PRIMARY KEY,
    global_subject_id INT REFERENCES subjects(global_subject_id),
    genotyping_project TEXT,
    genotyping_barcode TEXT,
    batch TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Indexes for performance
CREATE INDEX idx_subjects_center ON subjects(center_id);
CREATE INDEX idx_subjects_family ON subjects(family_id);
CREATE INDEX idx_local_ids_gsid ON local_subject_ids(global_subject_id);
CREATE INDEX idx_subjects_withdrawn ON subjects(withdrawn) WHERE withdrawn = TRUE;

-- Update timestamp trigger
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
