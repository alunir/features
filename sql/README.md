## Import data to Cloud SQL

See [here](https://cloud.google.com/sql/docs/postgres/import-export?hl=ja)


## How to use COPY
- COPY is a command that allows you to copy data between a file and a table.

### Preliminary
Grant the `pg_read_server_files` Role:
```sql
GRANT pg_read_server_files TO your_username;
```

Then verify the role
```sql
\du your_username
```

### Syntax
```sql
COPY table_name [ ( column_name [, ...] ) ]
    FROM { 'filename' | PROGRAM 'command' | STDIN }
    [ [ WITH ] ( option [, ...] ) ]
```

### Example
```sql
COPY ohlcv FROM 'data.csv' DELIMITER ',' CSV HEADER;
```

