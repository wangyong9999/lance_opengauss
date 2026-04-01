/* lance_fdw--1.0.sql */

-- Complain if script is sourced in psql, rather than via CREATE EXTENSION
\echo Use "CREATE EXTENSION lance_fdw" to load this file. \quit

/*
 * FDW handler function - returns the FdwRoutine with callback pointers.
 */
CREATE FUNCTION lance_fdw_handler()
    RETURNS fdw_handler
    AS 'MODULE_PATHNAME'
    LANGUAGE C STRICT;

/*
 * FDW validator function - validates options for CREATE SERVER and
 * CREATE FOREIGN TABLE.
 */
CREATE FUNCTION lance_fdw_validator(text[], oid)
    RETURNS void
    AS 'MODULE_PATHNAME'
    LANGUAGE C STRICT;

/*
 * Create the Foreign Data Wrapper.
 */
CREATE FOREIGN DATA WRAPPER lance_fdw
    HANDLER lance_fdw_handler
    VALIDATOR lance_fdw_validator;

/*
 * Schema auto-discovery: reads a Lance dataset and returns
 * the CREATE FOREIGN TABLE DDL.
 *
 * Usage: SELECT lance_import('lance_srv', 'my_table', '/path/to/dataset');
 */
CREATE FUNCTION lance_import(server_name text, table_name text, uri text)
    RETURNS text
    AS 'MODULE_PATHNAME', 'lance_import'
    LANGUAGE C STRICT;

/*
 * Usage example (run manually after CREATE EXTENSION):
 *
 *   CREATE SERVER lance_srv FOREIGN DATA WRAPPER lance_fdw;
 *
 *   CREATE FOREIGN TABLE my_lance_data (
 *       id        bigint,
 *       name      text,
 *       embedding float4[]
 *   ) SERVER lance_srv
 *     OPTIONS (uri '/path/to/lance/dataset');
 *
 *   SELECT * FROM my_lance_data LIMIT 10;
 */
