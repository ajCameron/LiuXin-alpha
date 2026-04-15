PRAGMA `foreign_keys` = ON;

CREATE TRIGGER IF NOT EXISTS `trg_backup_presence_links_prevent_delete_when_protected`
BEFORE DELETE ON `backup_presence_links`
WHEN OLD.`backup_presence_link_is_protected` = 1
BEGIN
  SELECT RAISE(ABORT, 'backup_presence_links row is protected and may not be deleted');
END;

CREATE TRIGGER IF NOT EXISTS `trg_backup_presence_links_prevent_update_when_immutable`
BEFORE UPDATE ON `backup_presence_links`
WHEN OLD.`backup_presence_link_is_immutable` = 1
BEGIN
  SELECT RAISE(ABORT, 'backup_presence_links row is immutable and may not be updated');
END;
