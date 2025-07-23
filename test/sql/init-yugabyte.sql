-- init-yugabyte.sql
-- Drop if exists (clean slate)
DROP DATABASE IF EXISTS testdb;
CREATE DATABASE testdb;

\c testdb;
CREATE SCHEMA IF NOT EXISTS public;