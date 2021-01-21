CREATE TABLE jobs
(
  jobid uuid NOT NULL,
  jobdef text,
  state text,
  start timestamp with time zone,
  ended timestamp with time zone,
  stepstate text,
  stepid text,
  system text,
  job bytea,
  revision text,
  steptype text,
  parentjobid uuid,
  isparent boolean,
  threadstack text,
  CONSTRAINT jobs_pkey PRIMARY KEY (jobid)
)
WITH (
  OIDS=FALSE
);

CREATE INDEX jobs_parentjobid_idx
  ON jobs
  USING btree
  (parentjobid);

CREATE TABLE users
(
  name character varying(40) NOT NULL,
  email character varying(70) NOT NULL,
  password_digest character varying(162),
  created_at timestamp without time zone NOT NULL DEFAULT now(),
  updated_at timestamp without time zone NOT NULL DEFAULT now(),
  level character varying(12) NOT NULL DEFAULT 'user'::character varying,
  CONSTRAINT users_pkey PRIMARY KEY (name),
  CONSTRAINT users_email_key UNIQUE (email),
  CONSTRAINT users_name_check CHECK (name::text <> ''::text)
)
WITH (
  OIDS=FALSE
);