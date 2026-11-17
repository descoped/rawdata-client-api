-- The user owns the database, so it owns its public schema: since PostgreSQL 15, public is no longer writable by
-- every user, and GRANT ALL ON DATABASE does not cover creating tables in it.
CREATE USER rdc WITH PASSWORD 'rdc';
CREATE DATABASE rdc OWNER rdc;
