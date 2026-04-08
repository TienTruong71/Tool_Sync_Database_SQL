## nếu có sửa lại code thì chạy lệnh sau để build lại exe
```
python build_exe.py
```

## Nên để file .env cùng cấp với thư mục CDC_Replicator.exe

```
# ip db nguồn
KINGDOM_SQLSERVER_HOST=
KINGDOM_SQLSERVER_USER=
KINGDOM_SQLSERVER_PASS=
KINGDOM_SQLSERVER_DB=	
KINGDOM_SQLSERVER_PORT=


# ip db đích
KINGDOM_DST_SQLSERVER_HOST=
KINGDOM_DST_SQLSERVER_USER=
KINGDOM_DST_SQLSERVER_PASS=
KINGDOM_DST_SQLSERVER_DB=
KINGDOM_DST_SQLSERVER_PORT=


# KINGDOM_SYNC_TABLES=      # Table cụ thể muốn đồng bộ , default: all
KINGDOM_BATCH_SIZE=                          # Tốc độ đồng bộ, default: 500
KINGDOM_SCHEMA_CHECK_INTERVAL=             # Tốc độ check column trong table thay đổi, default: 5s
KINGDOM_TABLE_SCAN_INTERVAL=             # Tốc độ check xem db có table nào thay đổi , default: 5m
```