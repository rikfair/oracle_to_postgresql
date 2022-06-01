DO $$
BEGIN
	GRANT USAGE ON SCHEMA %%schema%% TO %%schema%%_user, %%schema%%_admin;
	GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA %%schema%% TO %%schema%%_user, %%schema%%_admin;
	ALTER DEFAULT PRIVILEGES IN SCHEMA %%schema%% GRANT SELECT, INSERT, UPDATE, DELETE ON TABLES TO %%schema%%_user, %%schema%%_admin;
	GRANT USAGE, SELECT ON ALL SEQUENCES IN SCHEMA %%schema%% TO %%schema%%_user, %%schema%%_admin;
	ALTER DEFAULT PRIVILEGES IN SCHEMA %%schema%% GRANT USAGE, SELECT ON SEQUENCES TO %%schema%%_user, %%schema%%_admin;
END $$;
