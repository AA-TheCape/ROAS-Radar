BEGIN;

ALTER TABLE tracking_sessions
  ADD COLUMN IF NOT EXISTS initial_gbraid text,
  ADD COLUMN IF NOT EXISTS initial_wbraid text;

ALTER TABLE tracking_events
  ADD COLUMN IF NOT EXISTS gbraid text,
  ADD COLUMN IF NOT EXISTS wbraid text;

DO $$
BEGIN
  IF NOT EXISTS (
    SELECT 1
    FROM pg_constraint
    WHERE conname = 'tracking_sessions_landing_page_len_chk'
  ) THEN
    ALTER TABLE tracking_sessions
      ADD CONSTRAINT tracking_sessions_landing_page_len_chk
      CHECK (landing_page IS NULL OR char_length(landing_page) <= 2048);
  END IF;

  IF NOT EXISTS (
    SELECT 1
    FROM pg_constraint
    WHERE conname = 'tracking_sessions_referrer_url_len_chk'
  ) THEN
    ALTER TABLE tracking_sessions
      ADD CONSTRAINT tracking_sessions_referrer_url_len_chk
      CHECK (referrer_url IS NULL OR char_length(referrer_url) <= 2048);
  END IF;

  IF NOT EXISTS (
    SELECT 1
    FROM pg_constraint
    WHERE conname = 'tracking_sessions_initial_utm_source_len_chk'
  ) THEN
    ALTER TABLE tracking_sessions
      ADD CONSTRAINT tracking_sessions_initial_utm_source_len_chk
      CHECK (initial_utm_source IS NULL OR char_length(initial_utm_source) <= 255);
  END IF;

  IF NOT EXISTS (
    SELECT 1
    FROM pg_constraint
    WHERE conname = 'tracking_sessions_initial_utm_medium_len_chk'
  ) THEN
    ALTER TABLE tracking_sessions
      ADD CONSTRAINT tracking_sessions_initial_utm_medium_len_chk
      CHECK (initial_utm_medium IS NULL OR char_length(initial_utm_medium) <= 255);
  END IF;

  IF NOT EXISTS (
    SELECT 1
    FROM pg_constraint
    WHERE conname = 'tracking_sessions_initial_utm_campaign_len_chk'
  ) THEN
    ALTER TABLE tracking_sessions
      ADD CONSTRAINT tracking_sessions_initial_utm_campaign_len_chk
      CHECK (initial_utm_campaign IS NULL OR char_length(initial_utm_campaign) <= 255);
  END IF;

  IF NOT EXISTS (
    SELECT 1
    FROM pg_constraint
    WHERE conname = 'tracking_sessions_initial_utm_content_len_chk'
  ) THEN
    ALTER TABLE tracking_sessions
      ADD CONSTRAINT tracking_sessions_initial_utm_content_len_chk
      CHECK (initial_utm_content IS NULL OR char_length(initial_utm_content) <= 255);
  END IF;

  IF NOT EXISTS (
    SELECT 1
    FROM pg_constraint
    WHERE conname = 'tracking_sessions_initial_utm_term_len_chk'
  ) THEN
    ALTER TABLE tracking_sessions
      ADD CONSTRAINT tracking_sessions_initial_utm_term_len_chk
      CHECK (initial_utm_term IS NULL OR char_length(initial_utm_term) <= 255);
  END IF;

  IF NOT EXISTS (
    SELECT 1
    FROM pg_constraint
    WHERE conname = 'tracking_sessions_initial_gclid_len_chk'
  ) THEN
    ALTER TABLE tracking_sessions
      ADD CONSTRAINT tracking_sessions_initial_gclid_len_chk
      CHECK (initial_gclid IS NULL OR char_length(initial_gclid) <= 255);
  END IF;

  IF NOT EXISTS (
    SELECT 1
    FROM pg_constraint
    WHERE conname = 'tracking_sessions_initial_gbraid_len_chk'
  ) THEN
    ALTER TABLE tracking_sessions
      ADD CONSTRAINT tracking_sessions_initial_gbraid_len_chk
      CHECK (initial_gbraid IS NULL OR char_length(initial_gbraid) <= 255);
  END IF;

  IF NOT EXISTS (
    SELECT 1
    FROM pg_constraint
    WHERE conname = 'tracking_sessions_initial_wbraid_len_chk'
  ) THEN
    ALTER TABLE tracking_sessions
      ADD CONSTRAINT tracking_sessions_initial_wbraid_len_chk
      CHECK (initial_wbraid IS NULL OR char_length(initial_wbraid) <= 255);
  END IF;

  IF NOT EXISTS (
    SELECT 1
    FROM pg_constraint
    WHERE conname = 'tracking_sessions_initial_fbclid_len_chk'
  ) THEN
    ALTER TABLE tracking_sessions
      ADD CONSTRAINT tracking_sessions_initial_fbclid_len_chk
      CHECK (initial_fbclid IS NULL OR char_length(initial_fbclid) <= 255);
  END IF;

  IF NOT EXISTS (
    SELECT 1
    FROM pg_constraint
    WHERE conname = 'tracking_sessions_initial_ttclid_len_chk'
  ) THEN
    ALTER TABLE tracking_sessions
      ADD CONSTRAINT tracking_sessions_initial_ttclid_len_chk
      CHECK (initial_ttclid IS NULL OR char_length(initial_ttclid) <= 255);
  END IF;

  IF NOT EXISTS (
    SELECT 1
    FROM pg_constraint
    WHERE conname = 'tracking_sessions_initial_msclkid_len_chk'
  ) THEN
    ALTER TABLE tracking_sessions
      ADD CONSTRAINT tracking_sessions_initial_msclkid_len_chk
      CHECK (initial_msclkid IS NULL OR char_length(initial_msclkid) <= 255);
  END IF;

  IF NOT EXISTS (
    SELECT 1
    FROM pg_constraint
    WHERE conname = 'tracking_events_page_url_len_chk'
  ) THEN
    ALTER TABLE tracking_events
      ADD CONSTRAINT tracking_events_page_url_len_chk
      CHECK (page_url IS NULL OR char_length(page_url) <= 2048);
  END IF;

  IF NOT EXISTS (
    SELECT 1
    FROM pg_constraint
    WHERE conname = 'tracking_events_referrer_url_len_chk'
  ) THEN
    ALTER TABLE tracking_events
      ADD CONSTRAINT tracking_events_referrer_url_len_chk
      CHECK (referrer_url IS NULL OR char_length(referrer_url) <= 2048);
  END IF;

  IF NOT EXISTS (
    SELECT 1
    FROM pg_constraint
    WHERE conname = 'tracking_events_utm_source_len_chk'
  ) THEN
    ALTER TABLE tracking_events
      ADD CONSTRAINT tracking_events_utm_source_len_chk
      CHECK (utm_source IS NULL OR char_length(utm_source) <= 255);
  END IF;

  IF NOT EXISTS (
    SELECT 1
    FROM pg_constraint
    WHERE conname = 'tracking_events_utm_medium_len_chk'
  ) THEN
    ALTER TABLE tracking_events
      ADD CONSTRAINT tracking_events_utm_medium_len_chk
      CHECK (utm_medium IS NULL OR char_length(utm_medium) <= 255);
  END IF;

  IF NOT EXISTS (
    SELECT 1
    FROM pg_constraint
    WHERE conname = 'tracking_events_utm_campaign_len_chk'
  ) THEN
    ALTER TABLE tracking_events
      ADD CONSTRAINT tracking_events_utm_campaign_len_chk
      CHECK (utm_campaign IS NULL OR char_length(utm_campaign) <= 255);
  END IF;

  IF NOT EXISTS (
    SELECT 1
    FROM pg_constraint
    WHERE conname = 'tracking_events_utm_content_len_chk'
  ) THEN
    ALTER TABLE tracking_events
      ADD CONSTRAINT tracking_events_utm_content_len_chk
      CHECK (utm_content IS NULL OR char_length(utm_content) <= 255);
  END IF;

  IF NOT EXISTS (
    SELECT 1
    FROM pg_constraint
    WHERE conname = 'tracking_events_utm_term_len_chk'
  ) THEN
    ALTER TABLE tracking_events
      ADD CONSTRAINT tracking_events_utm_term_len_chk
      CHECK (utm_term IS NULL OR char_length(utm_term) <= 255);
  END IF;

  IF NOT EXISTS (
    SELECT 1
    FROM pg_constraint
    WHERE conname = 'tracking_events_gclid_len_chk'
  ) THEN
    ALTER TABLE tracking_events
      ADD CONSTRAINT tracking_events_gclid_len_chk
      CHECK (gclid IS NULL OR char_length(gclid) <= 255);
  END IF;

  IF NOT EXISTS (
    SELECT 1
    FROM pg_constraint
    WHERE conname = 'tracking_events_gbraid_len_chk'
  ) THEN
    ALTER TABLE tracking_events
      ADD CONSTRAINT tracking_events_gbraid_len_chk
      CHECK (gbraid IS NULL OR char_length(gbraid) <= 255);
  END IF;

  IF NOT EXISTS (
    SELECT 1
    FROM pg_constraint
    WHERE conname = 'tracking_events_wbraid_len_chk'
  ) THEN
    ALTER TABLE tracking_events
      ADD CONSTRAINT tracking_events_wbraid_len_chk
      CHECK (wbraid IS NULL OR char_length(wbraid) <= 255);
  END IF;

  IF NOT EXISTS (
    SELECT 1
    FROM pg_constraint
    WHERE conname = 'tracking_events_fbclid_len_chk'
  ) THEN
    ALTER TABLE tracking_events
      ADD CONSTRAINT tracking_events_fbclid_len_chk
      CHECK (fbclid IS NULL OR char_length(fbclid) <= 255);
  END IF;

  IF NOT EXISTS (
    SELECT 1
    FROM pg_constraint
    WHERE conname = 'tracking_events_ttclid_len_chk'
  ) THEN
    ALTER TABLE tracking_events
      ADD CONSTRAINT tracking_events_ttclid_len_chk
      CHECK (ttclid IS NULL OR char_length(ttclid) <= 255);
  END IF;

  IF NOT EXISTS (
    SELECT 1
    FROM pg_constraint
    WHERE conname = 'tracking_events_msclkid_len_chk'
  ) THEN
    ALTER TABLE tracking_events
      ADD CONSTRAINT tracking_events_msclkid_len_chk
      CHECK (msclkid IS NULL OR char_length(msclkid) <= 255);
  END IF;
END $$;

COMMIT;
