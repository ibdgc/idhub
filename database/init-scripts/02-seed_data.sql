-- database/seed_data.sql
-- Initial reference data

INSERT INTO centers (center_id, name, investigator, country, consortium) VALUES
(1, 'Mount Sinai', 'Dr. Smith', 'USA', 'IBD Consortium'),
(2, 'Cedars-Sinai', 'Dr. Johnson', 'USA', 'IBD Consortium'),
(3, 'University of Cambridge', 'Dr. Williams', 'UK', 'European IBD Network'),
(4, 'University of Pittsburgh', 'Dr. Davis', 'USA', 'IBD Consortium'),
(5, 'Yale University', 'Dr. Brown', 'USA', 'IBD Consortium');

-- Sample families for testing
INSERT INTO family (family_id, public_id, public_family_id, local_pedigree) VALUES
('FAM001', 1001, 5001, 'PED001'),
('FAM002', 1002, 5002, 'PED002'),
('FAM003', 1003, 5003, 'PED003');
