ALTER SYSTEM SET wal_level = 'logical';
CREATE PUBLICATION northwind_pub FOR ALL TABLES;