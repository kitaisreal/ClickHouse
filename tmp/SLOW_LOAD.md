```
ubuntu@ip-172-31-39-69:~/ClickHouse/tmp$ go run populate_cache.go
Populating 100000 tables with 64 workers against http://localhost:8123/
progress: 1000 tables
...
progress: 100000 tables
Done: 100000 tables created, 0 errors, elapsed 56m56.683502554s
```

Check cache size:
```
ip-172-31-39-69.ec2.internal :) SELECT COUNT(*) FROM system.filesystem_cache;

   ┌─COUNT()─┐
1. │ 1100000 │ -- 1.10 million
   └─────────┘

1 row in set. Elapsed: 12.592 sec. Processed 1.10 million rows, 453.20 MB (87.36 thousand rows/s., 35.99 MB/s.)
Peak memory usage: 203.86 KiB.
```

Restart ClickHouse:
```
2026.03.25 14:35:54.117848 [ 1746368 ] {} <Information> FileCacheRocksDBIndex: Opened RocksDB metadata index at /home/ubuntu/ClickHouse/build_release/programs/filesystem_caches/s3_cache/.rocksdb_metadata_index
2026.03.25 14:35:54.397730 [ 1746368 ] {} <Information> FileCacheRocksDBIndex: Loaded 1100000 entries from RocksDB metadata index
2026.03.25 14:35:54.397764 [ 1746368 ] {} <Information> FileCache(s3_cache): Loading filesystem cache from RocksDB index (1100000 entries)
2026.03.25 14:35:56.280800 [ 1746368 ] {} <Information> FileCache(s3_cache): Loaded filesystem cache from RocksDB index (1100000 entries) in 1.883063194 seconds
```

