-- Create tenant-specific schemas within financial_shared database
CREATE SCHEMA IF NOT EXISTS bank001;
CREATE SCHEMA IF NOT EXISTS bank002;
CREATE SCHEMA IF NOT EXISTS bank003;

-- Airflow metadata database
CREATE DATABASE airflow_db;
