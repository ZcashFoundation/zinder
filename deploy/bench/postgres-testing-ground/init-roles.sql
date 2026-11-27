-- Least-privilege role and schema initialization for the Zinder
-- Stateless-Topology Testing Ground (local/diagnostic only, ADR-0004).

CREATE ROLE zinder_schema_owner LOGIN PASSWORD 'zinder_schema_owner_local_only';
CREATE ROLE zinder_writer LOGIN PASSWORD 'zinder_writer_local_only';
CREATE ROLE zinder_reader LOGIN PASSWORD 'zinder_reader_local_only';
CREATE ROLE zinder_admin LOGIN PASSWORD 'zinder_admin_local_only';

REVOKE CREATE ON SCHEMA public FROM PUBLIC;

CREATE SCHEMA IF NOT EXISTS zinder AUTHORIZATION zinder_schema_owner;
GRANT USAGE ON SCHEMA zinder TO zinder_writer, zinder_reader;

ALTER DEFAULT PRIVILEGES FOR ROLE zinder_schema_owner IN SCHEMA zinder
    GRANT SELECT, INSERT, UPDATE, DELETE ON TABLES TO zinder_writer;
ALTER DEFAULT PRIVILEGES FOR ROLE zinder_schema_owner IN SCHEMA zinder
    GRANT USAGE, SELECT ON SEQUENCES TO zinder_writer;
ALTER DEFAULT PRIVILEGES FOR ROLE zinder_schema_owner IN SCHEMA zinder
    GRANT SELECT ON TABLES TO zinder_reader;

CREATE TABLE IF NOT EXISTS zinder.topology_probe (
    id INTEGER PRIMARY KEY,
    epoch_label TEXT NOT NULL,
    recorded_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp()
);

ALTER TABLE zinder.topology_probe OWNER TO zinder_schema_owner;

GRANT SELECT, INSERT, UPDATE, DELETE ON TABLE zinder.topology_probe TO zinder_writer;
GRANT SELECT ON TABLE zinder.topology_probe TO zinder_reader;

INSERT INTO zinder.topology_probe (id, epoch_label)
VALUES (1, 'pgbouncer-transaction-pool-ready')
ON CONFLICT (id) DO NOTHING;
