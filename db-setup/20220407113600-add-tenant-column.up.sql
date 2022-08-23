ALTER TABLE jobs ADD COLUMN IF NOT EXISTS tenantid text;

CREATE INDEX IF NOT EXISTS jobs_tenantid_jobdef_idx ON jobs (tenantid, jobdef);