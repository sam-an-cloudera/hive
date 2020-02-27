CREATE TABLE SCHEDULED_QUERIES (
	SCHEDULED_QUERY_ID BIGINT NOT NULL,
	CLUSTER_NAMESPACE VARCHAR(256),
	ENABLED BOOLEAN NOT NULL,
	NEXT_EXECUTION INTEGER,
	QUERY VARCHAR(4000),
	SCHEDULE VARCHAR(256),
	SCHEDULE_NAME VARCHAR(256),
	`USER` VARCHAR(256),
	CONSTRAINT SCHEDULED_QUERIES_PK PRIMARY KEY (SCHEDULED_QUERY_ID)
);

CREATE TABLE SCHEDULED_EXECUTIONS (
	SCHEDULED_EXECUTION_ID BIGINT NOT NULL,
	END_TIME INTEGER,
	ERROR_MESSAGE VARCHAR(2000),
	EXECUTOR_QUERY_ID VARCHAR(256),
	LAST_UPDATE_TIME INTEGER,
	SCHEDULED_QUERY_ID BIGINT,
	START_TIME INTEGER,
	STATE VARCHAR(256),
	CONSTRAINT SCHEDULED_EXECUTIONS_PK PRIMARY KEY (SCHEDULED_EXECUTION_ID),
	CONSTRAINT SCHEDULED_EXECUTIONS_SCHQ_FK FOREIGN KEY (SCHEDULED_QUERY_ID) REFERENCES SCHEDULED_QUERIES(SCHEDULED_QUERY_ID) ON DELETE CASCADE
);

CREATE INDEX IDX_SCHEDULED_EXECUTIONS_LAST_UPDATE_TIME ON SCHEDULED_EXECUTIONS (LAST_UPDATE_TIME);
CREATE INDEX IDX_SCHEDULED_EXECUTIONS_SCHEDULED_QUERY_ID ON SCHEDULED_EXECUTIONS (SCHEDULED_QUERY_ID);
CREATE UNIQUE INDEX UNIQUE_SCHEDULED_EXECUTIONS_ID ON SCHEDULED_EXECUTIONS (SCHEDULED_EXECUTION_ID);

-- HIVE-22728
ALTER TABLE `KEY_CONSTRAINTS` DROP PRIMARY KEY;
ALTER TABLE `KEY_CONSTRAINTS` ADD CONSTRAINT `CONSTRAINTS_PK` PRIMARY KEY (`PARENT_TBL_ID`, `CONSTRAINT_NAME`, `POSITION`);

-- HIVE-21487
CREATE INDEX COMPLETED_COMPACTIONS_RES ON COMPLETED_COMPACTIONS (CC_DATABASE,CC_TABLE,CC_PARTITION);

-- These lines need to be last.  Insert any changes above.
UPDATE VERSION SET SCHEMA_VERSION='3.1.3000', VERSION_COMMENT='Hive release version 3.1.3000' where VER_ID=1;
SELECT 'Finished upgrading MetaStore schema from 3.1.2000 to 3.1.3000';
