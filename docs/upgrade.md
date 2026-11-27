# Upgrade and rollback

The database (`<home>/data/dioxide.db`, home from `DIOXIDE_HOME`) carries a schema version;
newer code migrates an older database on start and older code refuses a newer one ("upgrade
code-dioxide"). Schema v6 only adds the column `tasks.model_fallback_applied` (default 0), so
v5 to v6 drops nothing and takes no backup. To roll back to a v5 release, stop the service
(`dioxide service uninstall`, or stop `dioxide up`), copy the database, then run
`sqlite3 <home>/data/dioxide.db "PRAGMA user_version=5"` and start the old version. v5 code
reads tasks by column name and inserts with named columns, so it ignores the extra column; a
task that already fell back to the mapped model runs on the original models again.
