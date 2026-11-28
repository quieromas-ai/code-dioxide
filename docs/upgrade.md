# Upgrade and rollback

Releases and their breaking changes are listed in [CHANGELOG.md](../CHANGELOG.md).

The database (`<home>/data/dioxide.db`, home from `DIOXIDE_HOME`) carries a schema version
(9 in 0.1.0, 11 in 0.2.0). Newer code migrates an older database when it starts; older code refuses a newer
database with "upgrade code-dioxide".

To upgrade: stop the service (`dioxide service uninstall`, or stop `dioxide up`), copy
`<home>/data/dioxide.db`, install the new version and start it again.

To roll back: stop the service, put back the copy you made before upgrading, and start the old
version. Without a copy there is no supported way back: tasks recorded by the newer version may
use tables and columns the old one does not know.

0.1.0 to 0.2.0: the migration adds columns and a table (`tasks.resume_cause`, `steps` billed
usage and `files_changed_json`, `step_models`); existing rows keep their data and the new
figures start with the stages measured after the upgrade. A lifecycle task paused across the
upgrade keeps running the workflow copied at its first launch.
