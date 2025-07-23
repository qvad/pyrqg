-- init-postgres.sql
-- This file is executed by PostgreSQL on startup to initialize the database.

-- Create the testdb database if it doesn't exist
SELECT 'CREATE DATABASE testdb'
WHERE NOT EXISTS (SELECT FROM pg_database WHERE datname = 'testdb')\gexec

-- Connect to the testdb database
\c testdb;

-- Create schema 'public' if it does not exist (default in postgres)
CREATE SCHEMA IF NOT EXISTS public;

-- Grant privileges to testuser (if not already granted by default for superuser)
-- For a standard setup, testuser (created via POSTGRES_USER env var) will have privileges on testdb
-- No need to explicitly create user/db as docker-entrypoint-initdb.d handles POSTGRES_DB/USER/PASSWORD
