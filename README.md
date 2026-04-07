

```
python src/test_connection.py
python src/auto_register_connectors.py

// excute cdc scan
EXEC sys.sp_cdc_scan;


docker exec -it cdc_service bash
ls /opt/spark/jars | grep mssql

poetry run ipython

docker compose down cdc && docker compose up -d --build cdc

MERGE INTO {table} WITH (TABLOCK) AS t
Khóa toàn bảng (TABLOCK).
MERGE INTO {table} WITH (ROWLOCK, UPDLOCK) AS t
ROWLOCK: chỉ lock dòng đang update.
UPDLOCK: ngăn conflict đọc/ghi mà vẫn cho phép song song.
``` 

## Note
```
SQL Server thường xuyên truncate log
SELECT MIN([Current LSN]) FROM sys.fn_dblog(NULL, NULL) để lấy lsn tối thiểu
```

- backup db test
```
Win + R
dtswizard
```

- enable cdc
```
USE IPM_new;
GO

EXEC sys.sp_cdc_enable_db;
GO

DECLARE @schema sysname,
        @table sysname,
        @sql NVARCHAR(MAX);

DECLARE cur CURSOR FOR
SELECT 
    TABLE_SCHEMA, 
    TABLE_NAME
FROM INFORMATION_SCHEMA.TABLES
WHERE TABLE_TYPE='BASE TABLE'
  AND TABLE_SCHEMA = 'dbo'
  AND TABLE_NAME NOT LIKE 'sys%'
  AND TABLE_NAME NOT LIKE '%tracking%'
  AND TABLE_NAME NOT LIKE '%_tracking%'
  AND TABLE_NAME NOT LIKE 'sym%';

OPEN cur;
FETCH NEXT FROM cur INTO @schema, @table;

WHILE @@FETCH_STATUS = 0
BEGIN
    SET @sql = '
        BEGIN TRY
            EXEC sys.sp_cdc_enable_table 
                @source_schema = N''' + @schema + ''',
                @source_name   = N''' + @table + ''',
                @role_name     = NULL,
                @supports_net_changes = 0;  -- FIXED: NO NEED FOR PK
            PRINT ''CDC ENABLED: ' + @schema + '.' + @table + ''';
        END TRY
        BEGIN CATCH
            PRINT ''FAILED: ' + @schema + '.' + @table + ' → '' + ERROR_MESSAGE();
        END CATCH;
    ';

    EXEC(@sql);

    FETCH NEXT FROM cur INTO @schema, @table;
END

CLOSE cur;
DEALLOCATE cur;
```

- check CDC
```
SELECT 
    s.name AS schema_name,
    t.name AS table_name,
    ct.capture_instance,
    t.object_id
FROM cdc.change_tables ct
JOIN sys.tables t 
    ON t.object_id = ct.source_object_id
JOIN sys.schemas s
    ON s.schema_id = t.schema_id
ORDER BY s.name, t.name;
```# Tool_Sync_Database_SQL
