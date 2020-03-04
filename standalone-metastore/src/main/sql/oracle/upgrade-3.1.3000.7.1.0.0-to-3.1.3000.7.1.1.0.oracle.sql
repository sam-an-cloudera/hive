-- HIVE-22872
ALTER TABLE SCHEDULED_QUERIES ADD ACTIVE_EXECUTION_ID number(19);

-- These lines need to be last.  Insert any changes above.
UPDATE CDH_VERSION SET SCHEMA_VERSION='3.1.3000.7.1.0.0', VERSION_COMMENT='Hive release version 3.1.3000 for CDH 7.1.0.0' where VER_ID=1;
SELECT 'Finished upgrading MetaStore schema from 3.1.3000.7.1.0.0 to 3.1.3000.7.1.1.0' AS Status from dual;

