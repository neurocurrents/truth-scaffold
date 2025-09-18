--
CREATE TABLE IF NOT EXISTS records (
    pmid BIGINT PRIMARY KEY,
    title TEXT,
    abstract TEXT,
    journal_x TEXT,
    year_x INTEGER,
    doi TEXT,
    replication_outcome TEXT,
    decade INTEGER,
    year_y INTEGER,
    journal_y TEXT,
    pm_title TEXT,
    pm_abstract TEXT,
    reports_sex BOOLEAN,
    reports_race BOOLEAN,
    sex_any_hint BOOLEAN,
    race_any_hint BOOLEAN,
    sex_text_mention BOOLEAN,
    sex_text_counts BOOLEAN,
    sex_mesh_hit BOOLEAN,
    race_text_mention BOOLEAN,
    race_mesh_hit BOOLEAN,
    is_compliant BOOLEAN,
    compliance_category TEXT,
    fts tsvector
);

-- Trigger to update the fts column automatically
CREATE OR REPLACE FUNCTION records_fts_trigger() RETURNS trigger AS $$
BEGIN
  NEW.fts := to_tsvector('english', coalesce(NEW.title,'') || ' ' || coalesce(NEW.abstract,''));
  RETURN NEW;
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS tsvectorupdate ON records;
CREATE TRIGGER tsvectorupdate BEFORE INSERT OR UPDATE
    ON records FOR EACH ROW EXECUTE FUNCTION records_fts_trigger();
