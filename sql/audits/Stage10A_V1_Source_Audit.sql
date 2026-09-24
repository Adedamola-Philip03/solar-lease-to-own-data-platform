/* STAGE 10A V1 -- SOURCE DISCOVERY FOR THE MEDALLION PIPELINE
Run the complete file in a fresh SSMS window in NewSolarEnergyDB.
Read-only against project objects: only a local temporary inventory is created.
No source records, procedures, permissions or tracking settings are changed.
No customer values, passwords or connection strings are selected.

WORK PLAN (Stages 01-09 remain the source simulator and SQL validation baseline)
10A Source audit and data contracts: identify source tables, keys, relationships,
    mutation behaviour and change-capture capability. This script starts 10A.
10B Platform and access: confirm existing ADF/ADLS/Databricks resources, identities,
    networking, secret storage and costs before creating additional resources.
10C Ingestion foundation: approved table allowlist, batch ledger, consistent full
    load, incremental extraction, deletes, schema drift and retry checkpoints.
10D Bronze: retain raw extracted batches with source/batch/operation/version/time;
    publish a batch manifest only after all required objects land successfully.
10E Silver: typed records, deterministic deduplication, updates/deletes, referential
    checks, quarantines and traceable reconciliation back to Bronze.
10F Gold: contract/customer facts, dimensions, monthly risk and department metrics;
    reconcile the initial load to saved SQL Run 2 using the same reporting date.
10G Orchestration: scheduled dependencies, retry limits, monitoring, failed-run
    handling, retention-expiry recovery and publication only after validation.
10H Department reports: exports and dashboards consume one validated Gold version.
10I Email: approved recipient mapping, dispatch log, retry/duplicate controls and
    explicit handling of uncertain delivery outcomes. No recipients guessed.
10J Portfolio handover: Git versioning, deployment instructions, lineage, test
    evidence, operating guide and project defence.

This audit is not an ingestion job. Candidate timestamp/rowversion fields do
not establish a safe watermark. Creation timestamps and identity values alone
can miss later updates, deletes or transactions that commit out of order.
Change Tracking indicates changed keys/current state; it is not every historical
row image. CDC is a separate option. The extraction choice follows this audit.
Metadata rows are approximate and are not a full-load reconciliation baseline.
Metadata is inspected sequentially; avoid schema deployments during the audit.
An empty metadata result may reflect limited permissions, not missing features.
*/
SET NOCOUNT ON;
IF DB_NAME()<>N'NewSolarEnergyDB'
 THROW 51500,'Select NewSolarEnergyDB before running this audit.',1;
IF @@TRANCOUNT<>0
 THROW 51501,'Use a fresh connection with no open transaction.',1;
IF ISNULL(HAS_PERMS_BY_NAME(DB_NAME(),'DATABASE','VIEW DEFINITION'),0)<>1
 THROW 51502,'Run with database VIEW DEFINITION permission so the inventory is complete.',1;

DROP TABLE IF EXISTS #SourceInventory;
CREATE TABLE #SourceInventory
(
 Table_Name sysname COLLATE DATABASE_DEFAULT NOT NULL PRIMARY KEY,
 Domain_Name varchar(30) COLLATE DATABASE_DEFAULT NOT NULL,
 Expected_Change_Pattern varchar(100) COLLATE DATABASE_DEFAULT NOT NULL
);
INSERT #SourceInventory VALUES
 ('tbl_Project_Settings','POLICY','Mutable configuration; reporting date is a business cutoff'),
 ('tbl_Purchase_Types','POLICY','Reference data; changes must be captured'),
 ('tbl_Tenure_Policies','POLICY','Versioned policy; do not assume old rows cannot be edited'),
 ('tbl_Risk_Bands','POLICY','Versioned policy; capture changes and policy identity'),
 ('tbl_Loss_Warning_Policies','POLICY','Versioned policy; capture changes and policy identity'),
 ('tbl_Regions','MASTER','Mutable labels and reference data'),
 ('tbl_States','MASTER','Mutable labels and reference relationships'),
 ('tbl_Dealers','MASTER','Mutable names, status and assignments'),
 ('tbl_Agents','MASTER','Mutable names, status and current dealer'),
 ('tbl_Customers','MASTER','Mutable profile, registration status and geography'),
 ('tbl_Products','MASTER','Mutable catalogue, enabled flag and current cash price'),
 ('tbl_Product_Tenure_Pricing','POLICY','Versioned pricing; availability can change'),
 ('tbl_Contracts','CONTRACT','Origination terms; capture changes even if application intends fixed terms'),
 ('tbl_Contract_Events','EVENT','Intended append-only events; database enforcement must be verified'),
 ('tbl_Payment_Obligations','PAYMENT','Contractual obligations; do not rely on identity-only ingestion'),
 ('tbl_Payment_Receipts','PAYMENT','Intended append-only cash receipts; reversal is a separate record'),
 ('tbl_Receipt_Reversals','PAYMENT','Intended append-only cash reversal records'),
 ('tbl_Payment_Allocations','PAYMENT','Intended append-only allocations; reversals separate'),
 ('tbl_Allocation_Reversals','PAYMENT','Intended append-only allocation reversal records'),
 ('tbl_Assets','ASSET','Asset reference records; corrections may occur'),
 ('tbl_Contract_Assets','ASSET','Assignment records may be updated when unassigned'),
 ('tbl_Installations','FULFILMENT','Mutable installation, cancellation and accepted handover dates'),
 ('tbl_Warranty_Policies','POLICY','Versioned warranty terms; availability can change'),
 ('tbl_Contract_Warranties','WARRANTY','Warranty start/end updated after accepted handover'),
 ('tbl_Warranty_Events','EVENT','Intended append-only void/reinstate events'),
 ('tbl_Ownership_Events','EVENT','Intended append-only ownership transfers'),
 ('tbl_Service_Cases','SERVICE','Service case metadata; verify update behaviour'),
 ('tbl_Service_Case_Events','EVENT','Intended append-only service state transitions');

-- RESULT 01: source database and available change-tracking configuration.
SELECT '01_DATABASE_CAPABILITIES' AS Result_Set,d.name AS Database_Name,
 CONVERT(int,SERVERPROPERTY('EngineEdition')) AS Engine_Edition,
 d.collation_name AS Database_Collation,d.compatibility_level,
 d.snapshot_isolation_state_desc,d.is_read_committed_snapshot_on,d.is_cdc_enabled,
 CONVERT(bit,CASE WHEN ct.database_id IS NULL THEN 0 ELSE 1 END) AS Change_Tracking_Enabled,
 ct.retention_period,ct.retention_period_units_desc,ct.is_auto_cleanup_on,
 CHANGE_TRACKING_CURRENT_VERSION() AS Current_Change_Tracking_Version,
 HAS_PERMS_BY_NAME(DB_NAME(),'DATABASE','ALTER') AS Can_Alter_Database,
 SYSUTCDATETIME() AS Audited_At_UTC
FROM sys.databases d
LEFT JOIN sys.change_tracking_databases ct ON ct.database_id=d.database_id
WHERE d.database_id=DB_ID();

-- RESULT 02: classify every user table. Unknown objects require review.
SELECT '02_TABLE_INVENTORY' AS Result_Set,s.name AS Schema_Name,t.name AS Table_Name,
 CASE WHEN s.name='dbo' AND src.Table_Name IS NOT NULL THEN 'SOURCE_CANDIDATE'
      WHEN s.name='dbo' AND t.name LIKE 'tbl[_]Report[_]%' THEN 'SQL_REPORT_BASELINE_NOT_RAW_SOURCE'
      WHEN s.name='dbo' AND (t.name LIKE 'tbl[_]Synthetic[_]%' OR t.name='tbl_Contract_Generation_Plan') THEN 'SYNTHETIC_GENERATOR_CONTROL'
      WHEN s.name='dbo' AND t.name='tbl_Lifecycle_Requests' THEN 'IDEMPOTENCY_AUDIT_REVIEW_SEPARATELY'
      ELSE 'UNCLASSIFIED_REVIEW_REQUIRED' END AS Ingestion_Classification,
 src.Domain_Name,src.Expected_Change_Pattern,
 n.Approximate_Rows,pk.Primary_Key_Name,t.temporal_type_desc,t.is_tracked_by_cdc,
 CONVERT(bit,CASE WHEN ct.object_id IS NULL THEN 0 ELSE 1 END) AS Change_Tracking_Enabled,
 ct.is_track_columns_updated_on,ct.begin_version,ct.min_valid_version,
 HAS_PERMS_BY_NAME(QUOTENAME(s.name)+'.'+QUOTENAME(t.name),'OBJECT','SELECT') AS Can_Select,
 HAS_PERMS_BY_NAME(QUOTENAME(s.name)+'.'+QUOTENAME(t.name),'OBJECT','VIEW CHANGE TRACKING') AS Can_View_Change_Tracking
FROM sys.tables t
JOIN sys.schemas s ON s.schema_id=t.schema_id
LEFT JOIN #SourceInventory src ON src.Table_Name=t.name COLLATE DATABASE_DEFAULT AND s.name='dbo'
LEFT JOIN sys.change_tracking_tables ct ON ct.object_id=t.object_id
OUTER APPLY(SELECT SUM(CONVERT(bigint,p.rows)) Approximate_Rows FROM sys.partitions p WHERE p.object_id=t.object_id AND p.index_id IN (0,1)) n
OUTER APPLY(SELECT i.name Primary_Key_Name FROM sys.indexes i WHERE i.object_id=t.object_id AND i.is_primary_key=1) pk
WHERE t.is_ms_shipped=0 ORDER BY Ingestion_Classification,s.name,t.name;

-- RESULT 03: expected source coverage and primary-key blockers.
SELECT '03_SOURCE_READINESS' AS Result_Set,src.Table_Name,src.Domain_Name,
 CASE WHEN t.object_id IS NULL THEN 'MISSING_TABLE'
      WHEN pk.index_id IS NULL THEN 'MISSING_PRIMARY_KEY'
      WHEN ISNULL(HAS_PERMS_BY_NAME(N'dbo.'+QUOTENAME(src.Table_Name),'OBJECT','SELECT'),0)<>1 THEN 'SELECT_PERMISSION_REQUIRED'
      ELSE 'PRESENT_WITH_PRIMARY_KEY' END AS Structural_Status,
 CASE WHEN ct.object_id IS NOT NULL THEN 'CHANGE_TRACKING_ENABLED'
      WHEN t.is_tracked_by_cdc=1 THEN 'CDC_ENABLED_REVIEW_CAPTURE_CONFIGURATION'
      ELSE 'CHANGE_CAPTURE_NOT_ENABLED' END AS Change_Capture_Status,
 src.Expected_Change_Pattern
FROM #SourceInventory src
LEFT JOIN sys.tables t ON t.name COLLATE DATABASE_DEFAULT=src.Table_Name AND t.schema_id=SCHEMA_ID(N'dbo')
LEFT JOIN sys.indexes pk ON pk.object_id=t.object_id AND pk.is_primary_key=1
LEFT JOIN sys.change_tracking_tables ct ON ct.object_id=t.object_id
ORDER BY src.Domain_Name,src.Table_Name;

-- RESULT 04: complete source column contract; lengths are bytes, not characters.
SELECT '04_SOURCE_COLUMNS' AS Result_Set,t.name AS Table_Name,c.column_id AS Column_Position,
 c.name AS Column_Name,ty.name AS Data_Type,c.max_length AS Max_Length_Bytes,
 c.[precision] AS Numeric_Precision,c.[scale] AS Numeric_Scale,c.is_nullable,c.is_identity,
 c.is_computed,c.collation_name,dc.[definition] AS Default_Definition,
 CASE WHEN c.system_type_id=189 THEN 'ROWVERSION_CANDIDATE_NOT_A_DELETE_FEED'
      WHEN c.name LIKE '%Modified%' OR c.name LIKE '%Updated%' THEN 'POSSIBLE_UPDATE_MARKER_VERIFY_ALL_WRITE_PATHS'
      WHEN c.name LIKE '%Created%' OR c.name LIKE '%Recorded%' THEN 'CREATION_OR_RECORDING_TIME_NOT_UPDATE_WATERMARK'
      ELSE NULL END AS Ingestion_Review_Note
FROM #SourceInventory src
JOIN sys.tables t ON t.name COLLATE DATABASE_DEFAULT=src.Table_Name AND t.schema_id=SCHEMA_ID(N'dbo')
JOIN sys.columns c ON c.object_id=t.object_id
JOIN sys.types ty ON ty.user_type_id=c.user_type_id
LEFT JOIN sys.default_constraints dc ON dc.object_id=c.default_object_id
ORDER BY t.name,c.column_id;

-- RESULT 05: ordered primary keys, including composite keys.
SELECT '05_PRIMARY_KEYS' AS Result_Set,t.name AS Table_Name,i.name AS Primary_Key_Name,
 ic.key_ordinal AS Key_Position,c.name AS Key_Column
FROM #SourceInventory src
JOIN sys.tables t ON t.name COLLATE DATABASE_DEFAULT=src.Table_Name AND t.schema_id=SCHEMA_ID(N'dbo')
JOIN sys.indexes i ON i.object_id=t.object_id AND i.is_primary_key=1
JOIN sys.index_columns ic ON ic.object_id=i.object_id AND ic.index_id=i.index_id AND ic.key_ordinal>0
JOIN sys.columns c ON c.object_id=ic.object_id AND c.column_id=ic.column_id
ORDER BY t.name,ic.key_ordinal;

-- RESULT 06: dependency edges, used to design Silver validation/load ordering.
SELECT '06_RELATIONSHIPS' AS Result_Set,ct.name AS Child_Table,fk.name AS Foreign_Key_Name,
 fkc.constraint_column_id AS Key_Position,cc.name AS Child_Column,
 ps.name AS Parent_Schema,pt.name AS Parent_Table,pc.name AS Parent_Column,
 fk.is_disabled,fk.is_not_trusted,fk.delete_referential_action_desc
FROM sys.foreign_keys fk
JOIN sys.foreign_key_columns fkc ON fkc.constraint_object_id=fk.object_id
JOIN sys.tables ct ON ct.object_id=fk.parent_object_id
JOIN #SourceInventory src ON src.Table_Name=ct.name COLLATE DATABASE_DEFAULT AND ct.schema_id=SCHEMA_ID(N'dbo')
JOIN sys.columns cc ON cc.object_id=ct.object_id AND cc.column_id=fkc.parent_column_id
JOIN sys.tables pt ON pt.object_id=fk.referenced_object_id
JOIN sys.schemas ps ON ps.schema_id=pt.schema_id
JOIN sys.columns pc ON pc.object_id=pt.object_id AND pc.column_id=fkc.referenced_column_id
ORDER BY ct.name,fk.name,fkc.constraint_column_id;

-- RESULT 07: DML triggers, if any. Names alone do not prove append-only enforcement.
SELECT '07_SOURCE_TRIGGERS' AS Result_Set,t.name AS Table_Name,tr.name AS Trigger_Name,
 tr.is_disabled,tr.is_instead_of_trigger,te.type_desc AS Trigger_Event
FROM sys.triggers tr JOIN sys.tables t ON t.object_id=tr.parent_id
JOIN #SourceInventory src ON src.Table_Name=t.name COLLATE DATABASE_DEFAULT AND t.schema_id=SCHEMA_ID(N'dbo')
LEFT JOIN sys.trigger_events te ON te.object_id=tr.object_id
ORDER BY t.name,tr.name,te.type_desc;

-- RESULT 08: modules referencing source tables (dependency metadata, not a proof
-- of every writer: dynamic SQL, applications and manual DML may not appear).
SELECT DISTINCT '08_SOURCE_MODULE_DEPENDENCIES' AS Result_Set,
 OBJECT_SCHEMA_NAME(o.object_id) AS Module_Schema,o.name AS Module_Name,o.type_desc,
 src.Table_Name AS Referenced_Source_Table
FROM sys.sql_expression_dependencies dep
JOIN sys.objects o ON o.object_id=dep.referencing_id
JOIN sys.tables t ON t.object_id=dep.referenced_id
JOIN #SourceInventory src ON src.Table_Name=t.name COLLATE DATABASE_DEFAULT AND t.schema_id=SCHEMA_ID(N'dbo')
WHERE o.type IN ('P','V','FN','IF','TF','TR')
ORDER BY Module_Schema,Module_Name,Referenced_Source_Table;

-- RESULT 09: evidence baseline, not the proposed raw ingestion source.
IF OBJECT_ID(N'dbo.tbl_Report_Runs',N'U') IS NOT NULL
 EXEC sys.sp_executesql N'SELECT ''09_SQL_VALIDATION_BASELINE'' AS Result_Set,
 Report_Run_ID,Report_Version,Reporting_As_Of_Date,Run_Status,Contract_Rows,Customer_Rows,Monthly_Rows,
 Net_Cash,Net_Allocated,Unallocated_Credit
 FROM dbo.tbl_Report_Runs WHERE Run_Status=''SUCCEEDED'' ORDER BY Report_Run_ID;';

-- RESULT 10: audit completion is not a declaration that ingestion is ready.
SELECT '10_AUDIT_COMPLETION' AS Result_Set,
 'Stage 10A source audit completed; no project objects or data changed' AS Result,
 (SELECT COUNT(*) FROM #SourceInventory) AS Expected_Source_Tables,
 (SELECT COUNT(*) FROM #SourceInventory x JOIN sys.tables t
   ON t.name COLLATE DATABASE_DEFAULT=x.Table_Name AND t.schema_id=SCHEMA_ID(N'dbo')) AS Present_Source_Tables,
 'Review structural status, permissions and change-capture settings before implementation' AS Next_Action;
