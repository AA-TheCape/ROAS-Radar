BEGIN;

CREATE TABLE IF NOT EXISTS app_settings (
  singleton boolean PRIMARY KEY DEFAULT true CHECK (singleton),
  reporting_timezone text NOT NULL DEFAULT 'America/Los_Angeles',
  created_at timestamptz NOT NULL DEFAULT now(),
  updated_at timestamptz NOT NULL DEFAULT now()
);

INSERT INTO app_settings (singleton, reporting_timezone)
VALUES (true, 'America/Los_Angeles')
ON CONFLICT (singleton) DO NOTHING;

COMMIT;
