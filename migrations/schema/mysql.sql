-- Generated from the complete mysql migration chain through 0041.
-- Do not edit directly. The initializer owns deployment metadata and migration history.

SET @mokyu_schema_foreign_keys=@@foreign_key_checks;
SET foreign_key_checks=0;

CREATE TABLE `access_state` (
  `id` tinyint unsigned NOT NULL,
  `coverage_since` datetime(6) DEFAULT NULL,
  `flushed_at` datetime(6) DEFAULT NULL,
  PRIMARY KEY (`id`),
  CONSTRAINT `access_state_singleton` CHECK ((`id` = 1))
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin;

CREATE TABLE `api_tokens` (
  `id` binary(16) NOT NULL,
  `user_id` binary(16) NOT NULL,
  `label` varchar(128) /*!80000 COLLATE utf8mb4_0900_bin */ /*M!100603 COLLATE utf8mb4_nopad_bin */ NOT NULL,
  `token_hash` binary(32) NOT NULL,
  `prefix` varchar(32) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
  `system_access` tinyint(1) NOT NULL DEFAULT '0',
  `auth_revision` bigint NOT NULL,
  `authorization_revision` bigint NOT NULL DEFAULT '0',
  `created_at` datetime(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
  `expires_at` datetime(6) DEFAULT NULL,
  `last_used_at` datetime(6) DEFAULT NULL,
  `revoked_at` datetime(6) DEFAULT NULL,
  PRIMARY KEY (`id`),
  UNIQUE KEY `token_hash` (`token_hash`),
  KEY `api_tokens_user` (`user_id`),
  CONSTRAINT `api_tokens_user` FOREIGN KEY (`user_id`) REFERENCES `web_users` (`id`) ON DELETE CASCADE,
  CONSTRAINT `api_tokens_label` CHECK ((length(`label`) between 1 and 128))
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 /*!80000 COLLATE=utf8mb4_0900_bin */ /*M!100603 COLLATE=utf8mb4_nopad_bin */;

CREATE TABLE `audit_events` (
  `id` bigint NOT NULL AUTO_INCREMENT,
  `created_at` datetime(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
  `finished_at` datetime(6) DEFAULT NULL,
  `actor_id` binary(16) DEFAULT NULL,
  `actor_label` varchar(64) /*!80000 COLLATE utf8mb4_0900_bin */ /*M!100603 COLLATE utf8mb4_nopad_bin */ NOT NULL,
  `token_id` binary(16) DEFAULT NULL,
  `source` varchar(5) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
  `action` varchar(128) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
  `target` text /*!80000 COLLATE utf8mb4_0900_bin */ /*M!100603 COLLATE utf8mb4_nopad_bin */ NOT NULL DEFAULT (_utf8mb4''),
  `project_id` binary(16) DEFAULT NULL,
  `bucket_id` binary(16) DEFAULT NULL,
  `outcome` varchar(9) CHARACTER SET ascii COLLATE ascii_bin NOT NULL DEFAULT 'unknown',
  `request_id` varchar(128) CHARACTER SET ascii COLLATE ascii_bin DEFAULT NULL,
  `http_status` int DEFAULT NULL,
  `detail` json NOT NULL DEFAULT (json_object()),
  PRIMARY KEY (`id`),
  KEY `audit_actor` (`actor_id`,`id`),
  KEY `audit_security` (`actor_id`,`action`,`outcome`,`created_at`),
  KEY `audit_created` (`created_at`,`id`),
  CONSTRAINT `audit_outcome` CHECK ((`outcome` in (_ascii'unknown',_ascii'succeeded',_ascii'failed',_ascii'partial'))),
  CONSTRAINT `audit_source` CHECK ((`source` in (_utf8mb4'web',_utf8mb4'token',_utf8mb4'cli',_utf8mb4's3')))
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 /*!80000 COLLATE=utf8mb4_0900_bin */ /*M!100603 COLLATE=utf8mb4_nopad_bin */;

CREATE TABLE `bucket_domains` (
  `host` varchar(253) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
  `bucket_id` binary(16) NOT NULL,
  PRIMARY KEY (`host`),
  KEY `bucket_domains_bucket` (`bucket_id`),
  CONSTRAINT `bucket_domains_bucket` FOREIGN KEY (`bucket_id`) REFERENCES `buckets` (`id`) ON DELETE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin;

CREATE TABLE `buckets` (
  `id` binary(16) NOT NULL,
  `name` varchar(63) /*!80000 COLLATE utf8mb4_0900_bin */ /*M!100603 COLLATE utf8mb4_nopad_bin */ NOT NULL,
  `state` varchar(7) CHARACTER SET ascii COLLATE ascii_bin NOT NULL DEFAULT 'active',
  `cors` json NOT NULL DEFAULT (json_array()),
  `website_enabled` tinyint(1) NOT NULL DEFAULT '0',
  `index_document` text /*!80000 COLLATE utf8mb4_0900_bin */ /*M!100603 COLLATE utf8mb4_nopad_bin */ NOT NULL DEFAULT (_utf8mb4'index.html'),
  `error_document` text /*!80000 COLLATE utf8mb4_0900_bin */ /*M!100603 COLLATE utf8mb4_nopad_bin */ NOT NULL DEFAULT (_utf8mb4'404.html'),
  `created_at` datetime(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
  `project_id` binary(16) NOT NULL,
  `settings_revision` bigint NOT NULL DEFAULT '0',
  `uploads_paused` tinyint(1) NOT NULL DEFAULT '0',
  `public_base_url` text /*!80000 COLLATE utf8mb4_0900_bin */ /*M!100603 COLLATE utf8mb4_nopad_bin */ NOT NULL DEFAULT (_utf8mb4''),
  `storage_id` binary(16) NOT NULL,
  PRIMARY KEY (`id`),
  UNIQUE KEY `name` (`name`),
  UNIQUE KEY `buckets_id_project` (`id`,`project_id`),
  KEY `buckets_project` (`project_id`),
  KEY `buckets_storage` (`storage_id`),
  CONSTRAINT `buckets_project` FOREIGN KEY (`project_id`) REFERENCES `projects` (`id`),
  CONSTRAINT `buckets_storage` FOREIGN KEY (`storage_id`) REFERENCES `storages` (`id`),
  CONSTRAINT `buckets_state` CHECK ((`state` in (_utf8mb4'active',_utf8mb4'purging')))
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 /*!80000 COLLATE=utf8mb4_0900_bin */ /*M!100603 COLLATE=utf8mb4_nopad_bin */;

CREATE TABLE `cache_pins` (
  `chunk_id` bigint NOT NULL,
  `pin_type` varchar(6) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
  `owner_id` bigint NOT NULL,
  `created_at` datetime(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
  PRIMARY KEY (`chunk_id`,`pin_type`,`owner_id`),
  KEY `cache_pins_owner` (`owner_id`,`chunk_id`),
  CONSTRAINT `cache_pins_chunk` FOREIGN KEY (`chunk_id`) REFERENCES `chunks` (`id`),
  CONSTRAINT `cache_pins_type` CHECK ((`pin_type` in (_ascii'upload',_ascii'pack')))
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin;

CREATE TABLE `chunk_access_stats` (
  `chunk_id` bigint NOT NULL,
  `read_count` bigint NOT NULL DEFAULT '0',
  `range_read_count` bigint NOT NULL DEFAULT '0',
  `read_bytes` bigint NOT NULL DEFAULT '0',
  `last_read_at` datetime(6) NOT NULL,
  PRIMARY KEY (`chunk_id`),
  CONSTRAINT `chunk_access_stats_chunk` FOREIGN KEY (`chunk_id`) REFERENCES `chunks` (`id`) ON DELETE CASCADE,
  CONSTRAINT `chunk_access_stats_counts` CHECK (((`read_count` >= 0) and (`range_read_count` >= 0) and (`read_bytes` >= 0)))
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin;

CREATE TABLE `chunk_access_windows` (
  `chunk_id` bigint NOT NULL,
  `window_start` datetime(6) NOT NULL,
  `read_count` bigint NOT NULL DEFAULT '0',
  `range_read_count` bigint NOT NULL DEFAULT '0',
  `read_bytes` bigint NOT NULL DEFAULT '0',
  `range_origin_reads` bigint NOT NULL DEFAULT '0',
  PRIMARY KEY (`chunk_id`,`window_start`),
  KEY `chunk_access_expiry` (`window_start`),
  CONSTRAINT `chunk_access_source` FOREIGN KEY (`chunk_id`) REFERENCES `chunks` (`id`) ON DELETE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin;

CREATE TABLE `chunk_encodings` (
  `encoding_id` bigint NOT NULL AUTO_INCREMENT,
  `chunk_id` bigint NOT NULL,
  `location_id` binary(16) NOT NULL,
  `storage_id` binary(16) NOT NULL,
  `standalone_size` int DEFAULT NULL,
  `standalone_compressed` tinyint(1) NOT NULL DEFAULT '0',
  `nonce` varbinary(12) DEFAULT NULL,
  `state` varchar(9) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
  `owner_task` binary(16) DEFAULT NULL,
  `encryption_algorithm` varchar(17) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
  `block_key_id` binary(16) DEFAULT NULL,
  `encoding_fingerprint` binary(32) NOT NULL,
  `created_at` datetime(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
  `unreferenced_at` datetime(6) DEFAULT NULL,
  `deleted_at` datetime(6) DEFAULT NULL,
  `stored_at` datetime(6) DEFAULT NULL,
  `ready_chunk` bigint GENERATED ALWAYS AS (if((`state` = _ascii'ready'),`chunk_id`,NULL)) VIRTUAL,
  `quarantined_at` datetime(6) DEFAULT NULL,
  `quarantine_code` varchar(32) CHARACTER SET ascii COLLATE ascii_bin DEFAULT NULL,
  `quarantine_revision` bigint NOT NULL DEFAULT '0',
  `owner_stream` bigint DEFAULT NULL,
  `recovery_pending` tinyint(1) NOT NULL DEFAULT '0',
  `nonce_id` bigint DEFAULT NULL,
  `sync_pending` tinyint(1) NOT NULL DEFAULT '0',
  `boot_id` binary(16) DEFAULT NULL,
  PRIMARY KEY (`encoding_id`),
  UNIQUE KEY `location_id` (`location_id`),
  UNIQUE KEY `encodings_ready` (`ready_chunk`),
  KEY `encodings_chunk` (`storage_id`,`chunk_id`),
  KEY `encodings_owner` (`owner_task`),
  KEY `encodings_key` (`block_key_id`),
  KEY `encodings_chunk_id` (`chunk_id`),
  KEY `encodings_gc` (`state`,`unreferenced_at`,`encoding_id`),
  KEY `encodings_quarantined` (`storage_id`,`quarantined_at`,`encoding_id`),
  KEY `encodings_recovery` (`recovery_pending`,`storage_id`,`unreferenced_at`,`encoding_id`),
  KEY `encodings_owner_stream` (`owner_stream`),
  KEY `encodings_sync` (`sync_pending`,`state`,`quarantined_at`,`chunk_id`),
  CONSTRAINT `encodings_chunk` FOREIGN KEY (`storage_id`, `chunk_id`) REFERENCES `chunks` (`storage_id`, `id`) ON DELETE CASCADE,
  CONSTRAINT `encodings_key` FOREIGN KEY (`block_key_id`) REFERENCES `managed_keys` (`id`),
  CONSTRAINT `encodings_owner` FOREIGN KEY (`owner_task`) REFERENCES `tasks` (`id`) ON DELETE SET NULL,
  CONSTRAINT `encodings_owner_stream` FOREIGN KEY (`owner_stream`) REFERENCES `streams` (`id`) ON DELETE RESTRICT,
  CONSTRAINT `chunk_encodings_nonce_id` CHECK ((((`encryption_algorithm` = _ascii'none') and (`nonce_id` is null) and (`nonce` is null) and (`block_key_id` is null)) or ((`encryption_algorithm` <> _ascii'none') and (`block_key_id` is not null) and (((`nonce` is null) and (`nonce_id` is null) and (`state` <> _ascii'ready')) or ((`nonce` is not null) and (`nonce_id` is not null) and (`nonce_id` > 0)))))),
  CONSTRAINT `encoding_memory_sync` CHECK (((`boot_id` is null) or (0 <> `sync_pending`))),
  CONSTRAINT `encodings_algorithm` CHECK ((`encryption_algorithm` in (_ascii'none',_ascii'aes-256-gcm',_ascii'chacha20-poly1305'))),
  CONSTRAINT `encodings_encryption` CHECK ((((`encryption_algorithm` = _ascii'none') and (`block_key_id` is null)) or ((`encryption_algorithm` <> _ascii'none') and (`block_key_id` is not null)))),
  CONSTRAINT `encodings_nonce` CHECK (((`nonce` is null) or (length(`nonce`) = 12))),
  CONSTRAINT `encodings_quarantine` CHECK (((`quarantined_at` is null) = (`quarantine_code` is null))),
  CONSTRAINT `encodings_quarantine_revision` CHECK ((`quarantine_revision` >= 0)),
  CONSTRAINT `encodings_size` CHECK (((`standalone_size` is null) or (`standalone_size` between 1 and 4194320))),
  CONSTRAINT `encodings_state` CHECK ((`state` in (_ascii'uploading',_ascii'ready',_ascii'retired',_ascii'deleting',_ascii'deleted')))
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin;

CREATE TABLE `chunks` (
  `id` bigint NOT NULL AUTO_INCREMENT,
  `storage_id` binary(16) NOT NULL,
  `owner_stream` bigint DEFAULT NULL,
  `hash` binary(32) NOT NULL,
  `raw_size` int NOT NULL,
  `standalone_size` int DEFAULT NULL,
  `encryption_algorithm` varchar(17) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
  `block_key_id` binary(16) DEFAULT NULL,
  `standalone_compressed` tinyint(1) NOT NULL DEFAULT '0',
  `nonce` varbinary(12) DEFAULT NULL,
  `format` int NOT NULL DEFAULT '2',
  `state` varchar(9) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
  `current_encoding_id` bigint NOT NULL DEFAULT '0',
  `pack_id` bigint DEFAULT NULL,
  `created_at` datetime(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
  `unreferenced_at` datetime(6) DEFAULT NULL,
  `deleted_at` datetime(6) DEFAULT NULL,
  `reference_changed_at` datetime(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
  `split_at` datetime(6) DEFAULT NULL,
  `range_split_at` datetime(6) DEFAULT NULL,
  `repack_after` datetime(6) DEFAULT NULL,
  `active_hash` binary(32) GENERATED ALWAYS AS (if((`state` in (_ascii'preparing',_ascii'uploading',_ascii'ready')),`hash`,NULL)) VIRTUAL,
  `uuid` binary(16) NOT NULL,
  PRIMARY KEY (`id`),
  UNIQUE KEY `chunks_storage_id` (`storage_id`,`id`),
  UNIQUE KEY `chunks_dedup` (`storage_id`,`active_hash`,`raw_size`),
  KEY `chunks_key` (`block_key_id`),
  KEY `chunks_pack` (`storage_id`,`pack_id`),
  KEY `chunks_gc` (`storage_id`,`state`,`unreferenced_at`,`id`),
  KEY `chunks_deleted` (`state`,`deleted_at`,`id`),
  KEY `chunks_pack_id` (`pack_id`),
  KEY `chunks_repack` (`repack_after`,`id`),
  KEY `chunks_owner` (`owner_stream`),
  CONSTRAINT `chunks_key` FOREIGN KEY (`block_key_id`) REFERENCES `managed_keys` (`id`),
  CONSTRAINT `chunks_owner` FOREIGN KEY (`owner_stream`) REFERENCES `streams` (`id`) ON DELETE SET NULL,
  CONSTRAINT `chunks_pack` FOREIGN KEY (`storage_id`, `pack_id`) REFERENCES `packs` (`storage_id`, `id`),
  CONSTRAINT `chunks_storage` FOREIGN KEY (`storage_id`) REFERENCES `storages` (`id`),
  CONSTRAINT `chunks_algorithm` CHECK ((`encryption_algorithm` in (_ascii'none',_ascii'aes-256-gcm',_ascii'chacha20-poly1305'))),
  CONSTRAINT `chunks_encoding` CHECK (((`current_encoding_id` > 0) or (`state` = _ascii'preparing'))),
  CONSTRAINT `chunks_encryption` CHECK ((((`encryption_algorithm` = _ascii'none') and (`block_key_id` is null)) or ((`encryption_algorithm` <> _ascii'none') and (`block_key_id` is not null)))),
  CONSTRAINT `chunks_format_v2` CHECK ((`format` = 2)),
  CONSTRAINT `chunks_raw_size` CHECK ((`raw_size` between 1 and 4194304)),
  CONSTRAINT `chunks_state` CHECK ((`state` in (_ascii'preparing',_ascii'uploading',_ascii'ready',_ascii'failed',_ascii'deleting',_ascii'deleted'))),
  CONSTRAINT `chunks_stored_size` CHECK (((`standalone_size` is null) or (`standalone_size` between 1 and 4194320)))
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin;

CREATE TABLE `extents` (
  `stream_id` bigint NOT NULL,
  `offset_bytes` bigint NOT NULL,
  `length` int NOT NULL,
  `chunk_id` bigint DEFAULT NULL,
  `fragment_id` bigint DEFAULT NULL,
  `source_offset` int NOT NULL DEFAULT '0',
  PRIMARY KEY (`stream_id`,`offset_bytes`),
  KEY `extents_chunk` (`chunk_id`),
  KEY `extents_fragment` (`fragment_id`),
  CONSTRAINT `extents_chunk` FOREIGN KEY (`chunk_id`) REFERENCES `chunks` (`id`),
  CONSTRAINT `extents_fragment` FOREIGN KEY (`fragment_id`) REFERENCES `fragments` (`id`),
  CONSTRAINT `extents_stream` FOREIGN KEY (`stream_id`) REFERENCES `streams` (`id`) ON DELETE CASCADE,
  CONSTRAINT `extents_length` CHECK ((`length` between 1 and 4194304)),
  CONSTRAINT `extents_offset` CHECK ((`offset_bytes` >= 0)),
  CONSTRAINT `extents_source` CHECK (((`chunk_id` is null) <> (`fragment_id` is null))),
  CONSTRAINT `extents_source_offset` CHECK (((`source_offset` >= 0) and ((`source_offset` + `length`) <= 4194304)))
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin;

CREATE TABLE `fragments` (
  `id` bigint NOT NULL AUTO_INCREMENT,
  `location_id` binary(16) NOT NULL,
  `owner_stream` bigint DEFAULT NULL,
  `sealed` tinyint(1) NOT NULL DEFAULT '0',
  `size` int NOT NULL,
  `hash` binary(32) NOT NULL,
  `created_at` datetime(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
  `file_id` binary(16) DEFAULT NULL,
  `file_offset` bigint DEFAULT NULL,
  PRIMARY KEY (`id`),
  UNIQUE KEY `fragments_location` (`location_id`),
  KEY `fragments_cleanup` (`created_at`,`id`),
  KEY `fragments_file` (`file_id`,`file_offset`),
  KEY `fragments_stream` (`owner_stream`),
  CONSTRAINT `fragments_ibfk_1` FOREIGN KEY (`file_id`) REFERENCES `multipart_files` (`id`),
  CONSTRAINT `fragments_stream` FOREIGN KEY (`owner_stream`) REFERENCES `streams` (`id`) ON DELETE SET NULL,
  CONSTRAINT `fragments_chk_1` CHECK ((((`file_id` is null) = (`file_offset` is null)) and ((`file_offset` is null) or (`file_offset` >= 0)))),
  CONSTRAINT `fragments_size` CHECK ((`size` between 1 and 4194304))
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin;

CREATE TABLE `integrity_issues` (
  `id` bigint NOT NULL AUTO_INCREMENT,
  `task_id` binary(16) NOT NULL,
  `subject` varchar(64) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
  `code` varchar(64) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
  `chunk_id` bigint DEFAULT NULL,
  `location_id` binary(16) DEFAULT NULL,
  `stream_id` bigint DEFAULT NULL,
  `bucket_id` binary(16) DEFAULT NULL,
  `object_key` varbinary(1024) DEFAULT NULL,
  `detail` json NOT NULL DEFAULT (json_object()),
  `created_at` datetime(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
  PRIMARY KEY (`id`),
  UNIQUE KEY `integrity_issues_subject` (`task_id`,`subject`,`code`),
  KEY `integrity_issues_page` (`task_id`,`id`),
  KEY `integrity_issues_code` (`task_id`,`code`,`id`),
  CONSTRAINT `integrity_issues_task` FOREIGN KEY (`task_id`) REFERENCES `tasks` (`id`) ON DELETE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin;

CREATE TABLE `integrity_packs` (
  `task_id` binary(16) NOT NULL,
  `pack_id` bigint NOT NULL,
  `error_code` varchar(64) COLLATE utf8mb4_bin DEFAULT NULL,
  PRIMARY KEY (`task_id`,`pack_id`),
  KEY `integrity_packs_pack` (`pack_id`),
  CONSTRAINT `integrity_packs_pack` FOREIGN KEY (`pack_id`) REFERENCES `packs` (`id`) ON DELETE CASCADE,
  CONSTRAINT `integrity_packs_task` FOREIGN KEY (`task_id`) REFERENCES `tasks` (`id`) ON DELETE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin;

CREATE TABLE `key_replacements` (
  `id` binary(16) NOT NULL,
  `actor` varchar(160) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
  `plan` json NOT NULL,
  `result` json DEFAULT NULL,
  `expires_at` timestamp(6) NOT NULL,
  PRIMARY KEY (`id`),
  KEY `key_replacements_expiry` (`expires_at`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin;

CREATE TABLE `maintenance_controls` (
  `policy` varchar(7) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
  `paused` tinyint(1) NOT NULL DEFAULT '0',
  `last_scheduled_at` datetime(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
  `updated_at` datetime(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
  PRIMARY KEY (`policy`),
  CONSTRAINT `maintenance_controls_policy` CHECK ((`policy` in (_utf8mb4'pack',_utf8mb4'reuse',_utf8mb4'reclaim',_utf8mb4'repack',_utf8mb4'range',_utf8mb4'gc',_utf8mb4'cleanup')))
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin;

CREATE TABLE `maintenance_previews` (
  `id` binary(16) NOT NULL,
  `actor_id` binary(16) NOT NULL,
  `token_id` binary(16) DEFAULT NULL,
  `action` varchar(10) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
  `parameters` json NOT NULL,
  `fingerprint` char(64) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
  `expires_at` datetime(6) NOT NULL DEFAULT ((now(6) + interval 10 minute)),
  `task_id` binary(16) DEFAULT NULL,
  PRIMARY KEY (`id`),
  KEY `maintenance_previews_token` (`token_id`),
  KEY `maintenance_previews_task` (`task_id`),
  KEY `maintenance_previews_owner` (`actor_id`,`token_id`,`action`,`task_id`),
  KEY `maintenance_previews_expiry` (`expires_at`),
  CONSTRAINT `maintenance_previews_actor` FOREIGN KEY (`actor_id`) REFERENCES `web_users` (`id`) ON DELETE CASCADE,
  CONSTRAINT `maintenance_previews_task` FOREIGN KEY (`task_id`) REFERENCES `tasks` (`id`) ON DELETE SET NULL,
  CONSTRAINT `maintenance_previews_token` FOREIGN KEY (`token_id`) REFERENCES `api_tokens` (`id`) ON DELETE CASCADE,
  CONSTRAINT `maintenance_previews_action` CHECK ((`action` in (_utf8mb4'unpack',_utf8mb4'sweep',_utf8mb4'quarantine',_utf8mb4'recheck')))
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin;

CREATE TABLE `managed_keys` (
  `id` binary(16) NOT NULL,
  `name` varchar(128) /*!80000 COLLATE utf8mb4_0900_bin */ /*M!100603 COLLATE utf8mb4_nopad_bin */ NOT NULL,
  `purpose` varchar(7) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
  `key_algorithm` varchar(17) CHARACTER SET ascii COLLATE ascii_bin NOT NULL DEFAULT 'raw',
  `secret_encrypted` blob,
  `fingerprint` binary(32) NOT NULL,
  `created_at` datetime(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
  `revoked_at` datetime(6) DEFAULT NULL,
  PRIMARY KEY (`id`),
  UNIQUE KEY `name` (`name`),
  UNIQUE KEY `managed_keys_fingerprint` (`fingerprint`),
  CONSTRAINT `managed_keys_algorithm` CHECK ((`key_algorithm` in (_ascii'raw',_ascii'aes-256-gcm',_ascii'chacha20-poly1305'))),
  CONSTRAINT `managed_keys_name` CHECK ((length(`name`) between 1 and 128)),
  CONSTRAINT `managed_keys_purpose` CHECK ((`purpose` in (_ascii'access',_ascii'signing',_ascii'block')))
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 /*!80000 COLLATE=utf8mb4_0900_bin */ /*M!100603 COLLATE=utf8mb4_nopad_bin */;

CREATE TABLE `media_operations` (
  `id` binary(16) NOT NULL,
  `user_id` binary(16) NOT NULL,
  `token_id` binary(16) DEFAULT NULL,
  `token_scope` binary(17) GENERATED ALWAYS AS (concat(if((`token_id` is null),0x00,0x01),ifnull(`token_id`,0x00000000000000000000000000000000))) STORED,
  `client_id` binary(16) NOT NULL,
  `request_hash` binary(32) NOT NULL,
  `result` json DEFAULT NULL,
  `created_at` datetime(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
  PRIMARY KEY (`id`),
  UNIQUE KEY `media_operations_client` (`user_id`,`token_scope`,`client_id`),
  KEY `media_operations_cleanup` (`created_at`,`id`),
  CONSTRAINT `media_operations_user` FOREIGN KEY (`user_id`) REFERENCES `web_users` (`id`) ON DELETE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin;

CREATE TABLE `member_bucket_grants` (
  `user_id` binary(16) NOT NULL,
  `project_id` binary(16) NOT NULL,
  `bucket_id` binary(16) NOT NULL,
  `actions` smallint unsigned NOT NULL,
  PRIMARY KEY (`user_id`,`bucket_id`),
  KEY `member_grants_member` (`user_id`,`project_id`),
  KEY `member_grants_bucket` (`bucket_id`,`project_id`),
  CONSTRAINT `member_grants_bucket` FOREIGN KEY (`bucket_id`, `project_id`) REFERENCES `buckets` (`id`, `project_id`) ON DELETE CASCADE,
  CONSTRAINT `member_grants_member` FOREIGN KEY (`user_id`, `project_id`) REFERENCES `project_members` (`user_id`, `project_id`) ON DELETE CASCADE,
  CONSTRAINT `member_grants_actions` CHECK ((`actions` <= 127))
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin;

CREATE TABLE `mokyu_meta` (
  `id` tinyint unsigned NOT NULL,
  `schema_version` int NOT NULL,
  `deployment_id` binary(16) NOT NULL,
  `deployment_key_fingerprint` binary(32) DEFAULT NULL,
  `gc_paused` tinyint(1) NOT NULL DEFAULT '0',
  `maintenance` tinyint(1) NOT NULL DEFAULT '0',
  `project_management` tinyint(1) NOT NULL DEFAULT '0',
  `pack_creation_paused` tinyint(1) NOT NULL DEFAULT '0',
  PRIMARY KEY (`id`),
  CONSTRAINT `mokyu_meta_singleton` CHECK ((`id` = 1))
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin;

CREATE TABLE `multipart_files` (
  `id` binary(16) NOT NULL,
  `upload_id` binary(16) DEFAULT NULL,
  `storage_id` binary(16) NOT NULL,
  `source_id` bigint DEFAULT NULL,
  `reclaim_pending` tinyint(1) NOT NULL DEFAULT '0',
  `state` varchar(8) CHARACTER SET ascii COLLATE ascii_bin NOT NULL DEFAULT 'open',
  PRIMARY KEY (`id`),
  UNIQUE KEY `source_id` (`source_id`),
  KEY `storage_id` (`storage_id`),
  KEY `multipart_files_upload` (`upload_id`,`state`),
  KEY `multipart_files_reclaim` (`reclaim_pending`,`storage_id`,`id`),
  CONSTRAINT `multipart_files_ibfk_1` FOREIGN KEY (`upload_id`) REFERENCES `uploads` (`id`) ON DELETE SET NULL,
  CONSTRAINT `multipart_files_ibfk_2` FOREIGN KEY (`storage_id`) REFERENCES `storages` (`id`),
  CONSTRAINT `multipart_files_source` FOREIGN KEY (`source_id`) REFERENCES `raw_sources` (`id`),
  CONSTRAINT `multipart_files_chk_1` CHECK ((`state` in (_ascii'open',_ascii'sealed',_ascii'deleting')))
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin;

CREATE TABLE `nonce_counters` (
  `block_key_id` binary(16) NOT NULL,
  `type_tag` bigint NOT NULL,
  `reserved_id` bigint NOT NULL DEFAULT '0',
  PRIMARY KEY (`block_key_id`,`type_tag`),
  CONSTRAINT `nonce_counters_key` FOREIGN KEY (`block_key_id`) REFERENCES `managed_keys` (`id`),
  CONSTRAINT `nonce_counters_chk_1` CHECK ((`type_tag` between 0 and 31)),
  CONSTRAINT `nonce_counters_chk_2` CHECK ((`reserved_id` >= 0))
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin;

CREATE TABLE `nonce_observations` (
  `block_key_id` binary(16) NOT NULL,
  `data` json NOT NULL,
  `revision` bigint NOT NULL,
  PRIMARY KEY (`block_key_id`),
  CONSTRAINT `nonce_observations_key` FOREIGN KEY (`block_key_id`) REFERENCES `managed_keys` (`id`),
  CONSTRAINT `nonce_observations_chk_1` CHECK ((`revision` > 0))
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin;

CREATE TABLE `objects` (
  `bucket_id` binary(16) NOT NULL,
  `object_key` varbinary(1024) NOT NULL,
  `stream_id` bigint DEFAULT NULL,
  `write_token` bigint NOT NULL,
  `catalog_size` bigint DEFAULT NULL,
  `catalog_modified` datetime(6) DEFAULT NULL,
  `catalog_type` text /*!80000 COLLATE utf8mb4_0900_bin */ /*M!100603 COLLATE utf8mb4_nopad_bin */,
  `catalog_category` varchar(16) /*!80000 COLLATE utf8mb4_0900_bin */ /*M!100603 COLLATE utf8mb4_nopad_bin */ DEFAULT NULL,
  `catalog_public` tinyint(1) DEFAULT NULL,
  PRIMARY KEY (`bucket_id`,`object_key`),
  KEY `objects_catalog_size` (`bucket_id`,`catalog_size`,`object_key`),
  KEY `objects_catalog_modified` (`bucket_id`,`catalog_modified`,`object_key`),
  KEY `objects_stream` (`stream_id`),
  CONSTRAINT `objects_bucket` FOREIGN KEY (`bucket_id`) REFERENCES `buckets` (`id`),
  CONSTRAINT `objects_stream` FOREIGN KEY (`stream_id`) REFERENCES `streams` (`id`),
  CONSTRAINT `objects_key` CHECK ((length(`object_key`) between 1 and 1024))
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 /*!80000 COLLATE=utf8mb4_0900_bin */ /*M!100603 COLLATE=utf8mb4_nopad_bin */;

CREATE TABLE `pack_access_windows` (
  `pack_id` bigint NOT NULL,
  `window_start` datetime(6) NOT NULL,
  `downloads` bigint NOT NULL DEFAULT '0',
  `downloaded_bytes` bigint NOT NULL DEFAULT '0',
  `partial_downloads` bigint NOT NULL DEFAULT '0',
  `useful_bytes` bigint NOT NULL DEFAULT '0',
  `partial_bytes` bigint NOT NULL DEFAULT '0',
  `updated_at` datetime(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
  PRIMARY KEY (`pack_id`,`window_start`),
  KEY `pack_access_expiry` (`window_start`),
  CONSTRAINT `pack_access_source` FOREIGN KEY (`pack_id`) REFERENCES `packs` (`id`) ON DELETE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin;

CREATE TABLE `pack_changes` (
  `pack_id` bigint NOT NULL,
  `generation` bigint NOT NULL DEFAULT '1',
  `next_check_at` datetime(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
  `reason` varchar(7) CHARACTER SET ascii COLLATE ascii_bin NOT NULL DEFAULT 'reuse',
  PRIMARY KEY (`pack_id`),
  KEY `pack_changes_due` (`reason`,`next_check_at`,`pack_id`),
  CONSTRAINT `pack_changes_pack` FOREIGN KEY (`pack_id`) REFERENCES `packs` (`id`) ON DELETE CASCADE,
  CONSTRAINT `pack_changes_generation` CHECK ((`generation` > 0)),
  CONSTRAINT `pack_changes_reason` CHECK ((`reason` in (_utf8mb4'reuse',_utf8mb4'reclaim')))
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin;

CREATE TABLE `pack_inputs` (
  `task_id` binary(16) NOT NULL,
  `chunk_id` bigint NOT NULL,
  `created_at` datetime(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
  PRIMARY KEY (`task_id`,`chunk_id`),
  UNIQUE KEY `pack_inputs_chunk` (`chunk_id`),
  CONSTRAINT `pack_inputs_source` FOREIGN KEY (`chunk_id`) REFERENCES `chunks` (`id`),
  CONSTRAINT `pack_inputs_task` FOREIGN KEY (`task_id`) REFERENCES `tasks` (`id`) ON DELETE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin;

CREATE TABLE `pack_maintenance` (
  `stream_id` bigint NOT NULL,
  `progress_cursor` bigint NOT NULL DEFAULT '0',
  `generation` bigint NOT NULL DEFAULT '1',
  `next_check_at` datetime(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
  `updated_at` datetime(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
  `reason` varchar(6) CHARACTER SET ascii COLLATE ascii_bin NOT NULL DEFAULT 'pack',
  PRIMARY KEY (`stream_id`),
  KEY `pack_maintenance_due` (`reason`,`next_check_at`,`stream_id`),
  CONSTRAINT `pack_maintenance_stream` FOREIGN KEY (`stream_id`) REFERENCES `streams` (`id`) ON DELETE CASCADE,
  CONSTRAINT `pack_maintenance_progress` CHECK (((`progress_cursor` >= 0) and (`generation` > 0))),
  CONSTRAINT `pack_maintenance_reason` CHECK ((`reason` in (_ascii'pack',_ascii'repack')))
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin;

CREATE TABLE `pack_member_access_windows` (
  `pack_id` bigint NOT NULL,
  `window_start` datetime(6) NOT NULL,
  `chunk_id` bigint NOT NULL,
  `downloads` bigint NOT NULL DEFAULT '0',
  PRIMARY KEY (`pack_id`,`window_start`,`chunk_id`),
  KEY `pack_member_access_expiry` (`window_start`),
  KEY `pack_member_access_chunk` (`chunk_id`),
  CONSTRAINT `pack_member_access_chunk` FOREIGN KEY (`chunk_id`) REFERENCES `chunks` (`id`) ON DELETE CASCADE,
  CONSTRAINT `pack_member_access_pack` FOREIGN KEY (`pack_id`) REFERENCES `packs` (`id`) ON DELETE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin;

CREATE TABLE `pack_members` (
  `pack_id` bigint NOT NULL,
  `ordinal` int NOT NULL,
  `chunk_id` bigint NOT NULL,
  `offset_bytes` bigint NOT NULL,
  PRIMARY KEY (`pack_id`,`ordinal`),
  UNIQUE KEY `pack_members_chunk` (`pack_id`,`chunk_id`),
  KEY `members_chunk` (`chunk_id`),
  CONSTRAINT `members_chunk` FOREIGN KEY (`chunk_id`) REFERENCES `chunks` (`id`),
  CONSTRAINT `members_pack` FOREIGN KEY (`pack_id`) REFERENCES `packs` (`id`) ON DELETE CASCADE,
  CONSTRAINT `members_offset` CHECK ((`offset_bytes` >= 0)),
  CONSTRAINT `members_ordinal` CHECK ((`ordinal` between 0 and 4095))
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin;

CREATE TABLE `packs` (
  `id` bigint NOT NULL AUTO_INCREMENT,
  `location_id` binary(16) NOT NULL,
  `storage_id` binary(16) NOT NULL,
  `encryption_algorithm` varchar(17) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
  `block_key_id` binary(16) DEFAULT NULL,
  `raw_size` bigint NOT NULL,
  `stored_size` bigint DEFAULT NULL,
  `pack_compressed` tinyint(1) NOT NULL DEFAULT '0',
  `nonce` varbinary(12) DEFAULT NULL,
  `digest` binary(32) DEFAULT NULL,
  `member_count` int NOT NULL,
  `state` varchar(9) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
  `owner_task` binary(16) DEFAULT NULL,
  `pack_fingerprint` binary(32) NOT NULL,
  `created_at` datetime(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
  `unreferenced_at` datetime(6) DEFAULT NULL,
  `deleted_at` datetime(6) DEFAULT NULL,
  `range_checked_at` datetime(6) DEFAULT NULL,
  `stored_at` datetime(6) DEFAULT NULL,
  `quarantined_at` datetime(6) DEFAULT NULL,
  `quarantine_code` varchar(32) CHARACTER SET ascii COLLATE ascii_bin DEFAULT NULL,
  `quarantine_revision` bigint NOT NULL DEFAULT '0',
  `nonce_id` bigint DEFAULT NULL,
  PRIMARY KEY (`id`),
  UNIQUE KEY `location_id` (`location_id`),
  UNIQUE KEY `packs_storage_id` (`storage_id`,`id`),
  KEY `packs_key` (`block_key_id`),
  KEY `packs_owner` (`owner_task`),
  KEY `packs_gc` (`state`,`unreferenced_at`,`id`),
  KEY `packs_quarantined` (`storage_id`,`quarantined_at`,`id`),
  CONSTRAINT `packs_key` FOREIGN KEY (`block_key_id`) REFERENCES `managed_keys` (`id`),
  CONSTRAINT `packs_owner` FOREIGN KEY (`owner_task`) REFERENCES `tasks` (`id`) ON DELETE SET NULL,
  CONSTRAINT `packs_storage` FOREIGN KEY (`storage_id`) REFERENCES `storages` (`id`),
  CONSTRAINT `packs_algorithm` CHECK ((`encryption_algorithm` in (_ascii'none',_ascii'aes-256-gcm',_ascii'chacha20-poly1305'))),
  CONSTRAINT `packs_encryption` CHECK ((((`encryption_algorithm` = _ascii'none') and (`block_key_id` is null)) or ((`encryption_algorithm` <> _ascii'none') and (`block_key_id` is not null)))),
  CONSTRAINT `packs_members` CHECK ((`member_count` between 2 and 4096)),
  CONSTRAINT `packs_nonce` CHECK (((`nonce` is null) or (length(`nonce`) = 12))),
  CONSTRAINT `packs_nonce_id` CHECK ((((`encryption_algorithm` = _utf8mb4'none') and (`nonce_id` is null) and (`nonce` is null) and (`block_key_id` is null)) or ((`encryption_algorithm` <> _utf8mb4'none') and (`block_key_id` is not null) and (((`nonce` is null) and (`nonce_id` is null) and (`state` <> _utf8mb4'ready')) or ((`nonce` is not null) and (`nonce_id` is not null) and (`nonce_id` > 0)))))),
  CONSTRAINT `packs_quarantine` CHECK (((`quarantined_at` is null) = (`quarantine_code` is null))),
  CONSTRAINT `packs_quarantine_revision` CHECK ((`quarantine_revision` >= 0)),
  CONSTRAINT `packs_size` CHECK (((`raw_size` > 0) and ((`stored_size` is null) or (`stored_size` > 0)))),
  CONSTRAINT `packs_state` CHECK ((`state` in (_ascii'preparing',_ascii'ready',_ascii'retired',_ascii'deleting',_ascii'deleted')))
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin;

CREATE TABLE `parts` (
  `upload_id` binary(16) NOT NULL,
  `part_number` int NOT NULL,
  `stream_id` bigint DEFAULT NULL,
  `write_token` bigint NOT NULL,
  `quota_size` bigint NOT NULL DEFAULT '0',
  PRIMARY KEY (`upload_id`,`part_number`),
  KEY `parts_stream` (`stream_id`),
  CONSTRAINT `parts_stream` FOREIGN KEY (`stream_id`) REFERENCES `streams` (`id`),
  CONSTRAINT `parts_upload` FOREIGN KEY (`upload_id`) REFERENCES `uploads` (`id`) ON DELETE CASCADE,
  CONSTRAINT `parts_number` CHECK ((`part_number` between 1 and 10000)),
  CONSTRAINT `parts_quota_size` CHECK ((`quota_size` >= 0))
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin;

CREATE TABLE `pending_uploads` (
  `chunk_id` bigint NOT NULL,
  `stream_id` bigint DEFAULT NULL,
  `offset_bytes` bigint NOT NULL,
  `staged_size` bigint NOT NULL,
  `cache_key` varchar(40) CHARACTER SET ascii COLLATE ascii_bin DEFAULT NULL,
  `source_pack_id` bigint DEFAULT NULL,
  `owner_task` binary(16) DEFAULT NULL,
  `created_at` datetime(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
  `next_retry_at` datetime(6) NOT NULL,
  `attempts` int NOT NULL DEFAULT '0',
  `last_error` text /*!80000 COLLATE utf8mb4_0900_bin */ /*M!100603 COLLATE utf8mb4_nopad_bin */,
  `encoding_policy` json DEFAULT NULL,
  PRIMARY KEY (`chunk_id`),
  KEY `pending_pack` (`source_pack_id`),
  KEY `pending_due` (`owner_task`,`next_retry_at`,`chunk_id`),
  KEY `pending_stream_offset` (`stream_id`,`offset_bytes`),
  KEY `pending_cache_key` (`cache_key`),
  CONSTRAINT `pending_chunk` FOREIGN KEY (`chunk_id`) REFERENCES `chunks` (`id`),
  CONSTRAINT `pending_owner` FOREIGN KEY (`owner_task`) REFERENCES `tasks` (`id`) ON DELETE SET NULL,
  CONSTRAINT `pending_pack` FOREIGN KEY (`source_pack_id`) REFERENCES `packs` (`id`),
  CONSTRAINT `pending_stream` FOREIGN KEY (`stream_id`) REFERENCES `streams` (`id`) ON DELETE SET NULL,
  CONSTRAINT `pending_attempts` CHECK ((`attempts` >= 0)),
  CONSTRAINT `pending_offset` CHECK ((`offset_bytes` >= 0)),
  CONSTRAINT `pending_size` CHECK ((`staged_size` > 0))
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 /*!80000 COLLATE=utf8mb4_0900_bin */ /*M!100603 COLLATE=utf8mb4_nopad_bin */;

CREATE TABLE `project_members` (
  `user_id` binary(16) NOT NULL,
  `project_id` binary(16) NOT NULL,
  `role` varchar(10) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
  `scope` varchar(8) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
  PRIMARY KEY (`user_id`,`project_id`),
  KEY `project_members_project` (`project_id`),
  CONSTRAINT `project_members_project` FOREIGN KEY (`project_id`) REFERENCES `projects` (`id`) ON DELETE CASCADE,
  CONSTRAINT `project_members_user` FOREIGN KEY (`user_id`) REFERENCES `web_users` (`id`) ON DELETE CASCADE,
  CONSTRAINT `project_members_role` CHECK ((`role` in (_utf8mb4'reader',_utf8mb4'writer',_utf8mb4'maintainer'))),
  CONSTRAINT `project_members_scope` CHECK ((`scope` in (_utf8mb4'all',_utf8mb4'selected')))
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin;

CREATE TABLE `projects` (
  `id` binary(16) NOT NULL,
  `name` varchar(128) /*!80000 COLLATE utf8mb4_0900_bin */ /*M!100603 COLLATE utf8mb4_nopad_bin */ NOT NULL,
  `description` varchar(2000) /*!80000 COLLATE utf8mb4_0900_bin */ /*M!100603 COLLATE utf8mb4_nopad_bin */ NOT NULL DEFAULT '',
  `builtin` tinyint(1) NOT NULL DEFAULT '0',
  `allow_bucket_create` tinyint(1) NOT NULL DEFAULT '0',
  `created_at` datetime(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
  PRIMARY KEY (`id`),
  UNIQUE KEY `name` (`name`),
  CONSTRAINT `projects_description` CHECK ((length(`description`) <= 2000)),
  CONSTRAINT `projects_name` CHECK ((length(`name`) between 1 and 128))
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 /*!80000 COLLATE=utf8mb4_0900_bin */ /*M!100603 COLLATE=utf8mb4_nopad_bin */;

CREATE TABLE `quota_reservations` (
  `id` bigint NOT NULL AUTO_INCREMENT,
  `bucket_id` binary(16) NOT NULL,
  `object_key` varbinary(1024) NOT NULL,
  `stream_id` bigint DEFAULT NULL,
  `upload_id` binary(16) DEFAULT NULL,
  `result_stream_id` bigint DEFAULT NULL,
  `closed` tinyint(1) NOT NULL DEFAULT '0',
  `replaced_stream_id` bigint DEFAULT NULL,
  `credit_bytes` bigint NOT NULL DEFAULT '0',
  `logical_bytes` bigint NOT NULL DEFAULT '0',
  `inflight_bytes` bigint NOT NULL DEFAULT '0',
  PRIMARY KEY (`id`),
  UNIQUE KEY `stream_id` (`stream_id`),
  UNIQUE KEY `upload_id` (`upload_id`),
  UNIQUE KEY `result_stream_id` (`result_stream_id`),
  KEY `reservations_replaced` (`bucket_id`,`object_key`,`replaced_stream_id`),
  CONSTRAINT `reservations_bucket` FOREIGN KEY (`bucket_id`) REFERENCES `buckets` (`id`),
  CONSTRAINT `reservations_result` FOREIGN KEY (`result_stream_id`) REFERENCES `streams` (`id`) ON DELETE SET NULL,
  CONSTRAINT `reservations_stream` FOREIGN KEY (`stream_id`) REFERENCES `streams` (`id`),
  CONSTRAINT `reservations_upload` FOREIGN KEY (`upload_id`) REFERENCES `uploads` (`id`),
  CONSTRAINT `reservations_bytes` CHECK (((`credit_bytes` >= 0) and (`logical_bytes` >= 0) and (`inflight_bytes` >= 0))),
  CONSTRAINT `reservations_owner` CHECK (((`stream_id` is null) <> (`upload_id` is null)))
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin;

CREATE TABLE `quota_scopes` (
  `scope` varchar(7) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
  `scope_id` binary(16) NOT NULL,
  `used_bytes` bigint NOT NULL DEFAULT '0',
  `reserved_bytes` bigint NOT NULL DEFAULT '0',
  `inflight_bytes` bigint NOT NULL DEFAULT '0',
  `object_count` bigint NOT NULL DEFAULT '0',
  `bucket_count` bigint NOT NULL DEFAULT '0',
  `logical_bytes_limit` bigint DEFAULT NULL,
  `inflight_bytes_limit` bigint DEFAULT NULL,
  `bucket_count_limit` bigint DEFAULT NULL,
  PRIMARY KEY (`scope`,`scope_id`),
  CONSTRAINT `quota_bytes` CHECK (((`used_bytes` >= 0) and (`reserved_bytes` >= 0) and (`inflight_bytes` >= 0))),
  CONSTRAINT `quota_counts` CHECK (((`object_count` >= 0) and (`bucket_count` >= 0))),
  CONSTRAINT `quota_limits` CHECK ((((`logical_bytes_limit` is null) or (`logical_bytes_limit` >= 0)) and ((`inflight_bytes_limit` is null) or (`inflight_bytes_limit` >= 0)) and ((`bucket_count_limit` is null) or (`bucket_count_limit` >= 0)))),
  CONSTRAINT `quota_scope` CHECK ((`scope` in (_utf8mb4'project',_utf8mb4'bucket')))
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin;

CREATE TABLE `quota_writes` (
  `stream_id` bigint NOT NULL,
  `reservation_id` bigint NOT NULL,
  `allocated_bytes` bigint NOT NULL DEFAULT '0',
  `contribution_bytes` bigint NOT NULL DEFAULT '0',
  PRIMARY KEY (`stream_id`),
  KEY `quota_writes_reservation` (`reservation_id`),
  CONSTRAINT `quota_writes_reservation` FOREIGN KEY (`reservation_id`) REFERENCES `quota_reservations` (`id`) ON DELETE CASCADE,
  CONSTRAINT `quota_writes_stream` FOREIGN KEY (`stream_id`) REFERENCES `streams` (`id`),
  CONSTRAINT `quota_writes_bytes` CHECK (((`allocated_bytes` >= 0) and (`contribution_bytes` >= 0)))
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin;

CREATE TABLE `raw_processing` (
  `stream_id` bigint NOT NULL,
  `source_id` bigint NOT NULL,
  `payload_revision` bigint NOT NULL,
  `content_hash` binary(32) DEFAULT NULL,
  `source_revision` bigint DEFAULT NULL,
  `token` binary(16) DEFAULT NULL,
  `lease_expires_at` datetime(6) DEFAULT NULL,
  `next_attempt_at` datetime(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
  `attempts` int NOT NULL DEFAULT '0',
  `last_error` text COLLATE utf8mb4_bin,
  PRIMARY KEY (`stream_id`),
  KEY `raw_processing_due` (`next_attempt_at`,`stream_id`),
  KEY `raw_processing_source_fk` (`source_id`),
  CONSTRAINT `raw_processing_source_fk` FOREIGN KEY (`source_id`) REFERENCES `raw_sources` (`id`) ON DELETE CASCADE,
  CONSTRAINT `raw_processing_stream_fk` FOREIGN KEY (`stream_id`) REFERENCES `streams` (`id`) ON DELETE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin;

CREATE TABLE `raw_ranges` (
  `stream_id` bigint NOT NULL,
  `offset_bytes` bigint NOT NULL,
  `length` bigint NOT NULL,
  `source_id` bigint NOT NULL,
  `source_offset` bigint NOT NULL DEFAULT '0',
  PRIMARY KEY (`stream_id`,`offset_bytes`),
  KEY `raw_ranges_source` (`source_id`),
  CONSTRAINT `raw_ranges_source` FOREIGN KEY (`source_id`) REFERENCES `raw_sources` (`id`),
  CONSTRAINT `raw_ranges_stream` FOREIGN KEY (`stream_id`) REFERENCES `streams` (`id`) ON DELETE CASCADE,
  CONSTRAINT `raw_ranges_chk_1` CHECK ((`offset_bytes` >= 0)),
  CONSTRAINT `raw_ranges_chk_2` CHECK ((`length` >= 0)),
  CONSTRAINT `raw_ranges_chk_3` CHECK ((`source_offset` >= 0))
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin;

CREATE TABLE `raw_sources` (
  `id` bigint NOT NULL AUTO_INCREMENT,
  `location_id` binary(16) NOT NULL,
  `storage_id` binary(16) NOT NULL,
  `owner_stream` bigint DEFAULT NULL,
  `medium` varchar(10) CHARACTER SET ascii COLLATE ascii_bin NOT NULL DEFAULT 'local',
  `durability` varchar(8) CHARACTER SET ascii COLLATE ascii_bin NOT NULL DEFAULT 'durable',
  `boot_id` binary(16) DEFAULT NULL,
  `state` varchar(11) CHARACTER SET ascii COLLATE ascii_bin NOT NULL DEFAULT 'writing',
  `size` bigint NOT NULL DEFAULT '0',
  `digest` binary(16) DEFAULT NULL,
  `content_hash` binary(32) DEFAULT NULL,
  `upload_id` text COLLATE utf8mb4_bin,
  `revision` bigint NOT NULL DEFAULT '1',
  `profile_revision` bigint NOT NULL,
  `quarantined_at` datetime(6) DEFAULT NULL,
  `quarantine_code` varchar(32) CHARACTER SET ascii COLLATE ascii_bin DEFAULT NULL,
  `created_at` datetime(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
  `unreferenced_at` datetime(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
  `pending_upload` tinyint(1) NOT NULL DEFAULT '0',
  `processing_task` binary(16) DEFAULT NULL,
  PRIMARY KEY (`id`),
  UNIQUE KEY `raw_sources_location` (`location_id`),
  UNIQUE KEY `raw_sources_content` (`storage_id`,`content_hash`,`size`),
  KEY `raw_sources_cleanup` (`storage_id`,`state`,`unreferenced_at`,`id`),
  KEY `raw_sources_owner` (`owner_stream`),
  KEY `raw_sources_processing` (`processing_task`),
  KEY `raw_sources_sync` (`durability`,`state`,`quarantine_code`,`id`),
  CONSTRAINT `raw_sources_owner` FOREIGN KEY (`owner_stream`) REFERENCES `streams` (`id`) ON DELETE SET NULL,
  CONSTRAINT `raw_sources_processing_fk` FOREIGN KEY (`processing_task`) REFERENCES `tasks` (`id`) ON DELETE SET NULL,
  CONSTRAINT `raw_sources_storage` FOREIGN KEY (`storage_id`) REFERENCES `storages` (`id`),
  CONSTRAINT `raw_sources_chk_1` CHECK ((`medium` in (_ascii'local',_ascii'memory',_ascii'shared_s3'))),
  CONSTRAINT `raw_sources_chk_2` CHECK ((`durability` in (_ascii'durable',_ascii'buffered',_ascii'volatile'))),
  CONSTRAINT `raw_sources_chk_3` CHECK ((`state` in (_ascii'writing',_ascii'ready',_ascii'unavailable',_ascii'deleting'))),
  CONSTRAINT `raw_sources_chk_4` CHECK ((`size` >= 0)),
  CONSTRAINT `raw_sources_chk_5` CHECK ((`revision` > 0)),
  CONSTRAINT `raw_sources_chk_6` CHECK ((`profile_revision` > 0)),
  CONSTRAINT `raw_sources_chk_7` CHECK (((`state` <> _ascii'ready') or (`digest` is not null))),
  CONSTRAINT `raw_sources_chk_8` CHECK (((`medium` = _ascii'memory') = (`boot_id` is not null)))
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin;

CREATE TABLE `raw_uploads` (
  `source_id` bigint NOT NULL,
  `token` binary(16) DEFAULT NULL,
  `deduplicate` tinyint(1) NOT NULL,
  `background` tinyint(1) NOT NULL,
  `content_hash` binary(32) DEFAULT NULL,
  `next_attempt_at` timestamp(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
  `attempts` int NOT NULL DEFAULT '0',
  `last_error` text COLLATE utf8mb4_bin,
  PRIMARY KEY (`source_id`),
  KEY `raw_uploads_due` (`next_attempt_at`,`source_id`),
  CONSTRAINT `raw_uploads_source` FOREIGN KEY (`source_id`) REFERENCES `raw_sources` (`id`) ON DELETE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin;

CREATE TABLE `runtime_samples` (
  `sampled_at` datetime(6) NOT NULL,
  `data` json NOT NULL,
  PRIMARY KEY (`sampled_at`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin;

CREATE TABLE `runtime_settings` (
  `section` varchar(32) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
  `value` json NOT NULL,
  `revision` bigint NOT NULL DEFAULT '1',
  `updated_at` datetime(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
  `is_default` tinyint(1) NOT NULL DEFAULT '1',
  PRIMARY KEY (`section`),
  CONSTRAINT `runtime_settings_revision` CHECK ((`revision` > 0))
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin;

CREATE TABLE `s3_credential_grants` (
  `access_key` varchar(128) CHARACTER SET utf8mb4 /*!80000 COLLATE utf8mb4_0900_bin */ /*M!100603 COLLATE utf8mb4_nopad_bin */ NOT NULL,
  `bucket_id` binary(16) NOT NULL,
  `actions` smallint unsigned NOT NULL,
  PRIMARY KEY (`access_key`,`bucket_id`),
  KEY `credential_grants_bucket` (`bucket_id`),
  CONSTRAINT `credential_grants_bucket` FOREIGN KEY (`bucket_id`) REFERENCES `buckets` (`id`) ON DELETE CASCADE,
  CONSTRAINT `credential_grants_credential` FOREIGN KEY (`access_key`) REFERENCES `s3_credentials` (`access_key_id`) ON DELETE CASCADE,
  CONSTRAINT `credential_grants_actions` CHECK ((`actions` <= 127))
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin;

CREATE TABLE `s3_credentials` (
  `access_key_id` varchar(128) /*!80000 COLLATE utf8mb4_0900_bin */ /*M!100603 COLLATE utf8mb4_nopad_bin */ NOT NULL,
  `secret_encrypted` blob NOT NULL,
  `enabled` tinyint(1) NOT NULL DEFAULT '1',
  `created_at` datetime(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
  `project_id` binary(16) NOT NULL,
  `authorization_revision` bigint NOT NULL DEFAULT '0',
  `label` varchar(128) /*!80000 COLLATE utf8mb4_0900_bin */ /*M!100603 COLLATE utf8mb4_nopad_bin */ NOT NULL DEFAULT '',
  `expires_at` datetime(6) DEFAULT NULL,
  `last_used_at` datetime(6) DEFAULT NULL,
  `created_by` binary(16) DEFAULT NULL,
  PRIMARY KEY (`access_key_id`),
  KEY `s3_credentials_project` (`project_id`),
  KEY `s3_credentials_creator` (`created_by`),
  CONSTRAINT `s3_credentials_creator` FOREIGN KEY (`created_by`) REFERENCES `web_users` (`id`) ON DELETE SET NULL,
  CONSTRAINT `s3_credentials_project` FOREIGN KEY (`project_id`) REFERENCES `projects` (`id`),
  CONSTRAINT `s3_credentials_label` CHECK ((length(`label`) <= 128))
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 /*!80000 COLLATE=utf8mb4_0900_bin */ /*M!100603 COLLATE=utf8mb4_nopad_bin */;

CREATE TABLE `session_refresh_tokens` (
  `id` binary(16) NOT NULL,
  `session_id` binary(16) NOT NULL,
  `family_id` binary(16) NOT NULL,
  `token_hash` binary(32) NOT NULL,
  `created_at` datetime(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
  `last_used_at` datetime(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
  `expires_at` datetime(6) NOT NULL,
  `revoked_at` datetime(6) DEFAULT NULL,
  `replaced_by` binary(16) DEFAULT NULL,
  `active_session` binary(16) GENERATED ALWAYS AS (if((`revoked_at` is null),`session_id`,NULL)) VIRTUAL,
  PRIMARY KEY (`id`),
  UNIQUE KEY `token_hash` (`token_hash`),
  UNIQUE KEY `refresh_active` (`active_session`),
  KEY `refresh_session` (`session_id`),
  KEY `refresh_successor` (`replaced_by`),
  KEY `refresh_expiry` (`revoked_at`,`expires_at`),
  CONSTRAINT `refresh_session` FOREIGN KEY (`session_id`) REFERENCES `sessions` (`id`) ON DELETE CASCADE,
  CONSTRAINT `refresh_successor` FOREIGN KEY (`replaced_by`) REFERENCES `session_refresh_tokens` (`id`) ON DELETE SET NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin;

CREATE TABLE `sessions` (
  `id` binary(16) NOT NULL,
  `token_hash` binary(32) NOT NULL,
  `user_id` binary(16) NOT NULL,
  `csrf_hash` binary(32) NOT NULL,
  `expires_at` datetime(6) NOT NULL,
  `created_at` datetime(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
  `last_seen_at` datetime(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
  `reauthenticated_at` datetime(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
  `auth_revision` bigint NOT NULL DEFAULT '0',
  `user_agent` varchar(512) /*!80000 COLLATE utf8mb4_0900_bin */ /*M!100603 COLLATE utf8mb4_nopad_bin */ NOT NULL DEFAULT '',
  PRIMARY KEY (`id`),
  UNIQUE KEY `token_hash` (`token_hash`),
  KEY `sessions_expiry` (`expires_at`),
  KEY `sessions_user_created` (`user_id`,`created_at` DESC,`id` DESC),
  CONSTRAINT `sessions_user` FOREIGN KEY (`user_id`) REFERENCES `web_users` (`id`) ON DELETE CASCADE,
  CONSTRAINT `sessions_user_agent` CHECK ((length(`user_agent`) <= 512))
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 /*!80000 COLLATE=utf8mb4_0900_bin */ /*M!100603 COLLATE=utf8mb4_nopad_bin */;

CREATE TABLE `setup_tokens` (
  `id` tinyint unsigned NOT NULL,
  `token_hash` binary(32) NOT NULL,
  `created_at` datetime(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
  `expires_at` datetime(6) NOT NULL,
  PRIMARY KEY (`id`),
  CONSTRAINT `setup_singleton` CHECK ((`id` = 1))
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin;

CREATE TABLE `storage_maintenance` (
  `storage_id` binary(16) NOT NULL,
  `policy` varchar(16) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
  `last_scheduled_at` datetime(6) NOT NULL,
  PRIMARY KEY (`storage_id`,`policy`),
  CONSTRAINT `storage_maintenance_storage_fk` FOREIGN KEY (`storage_id`) REFERENCES `storages` (`id`) ON DELETE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin;

CREATE TABLE `storage_profiles` (
  `storage_id` binary(16) NOT NULL,
  `revision` bigint NOT NULL DEFAULT '1',
  `connect_timeout` varchar(32) COLLATE ascii_bin NOT NULL DEFAULT '5s',
  `request_timeout` varchar(32) COLLATE ascii_bin NOT NULL DEFAULT '30s',
  `max_retries` int NOT NULL DEFAULT '3',
  `retry_timeout` varchar(32) COLLATE ascii_bin NOT NULL DEFAULT '120s',
  `priority_aging` varchar(32) COLLATE ascii_bin NOT NULL DEFAULT '30s',
  `min_storage_duration` varchar(32) COLLATE ascii_bin NOT NULL DEFAULT '0s',
  `unreferenced_grace` varchar(32) COLLATE ascii_bin NOT NULL DEFAULT '48h',
  `deleted_chunk_retention` varchar(32) COLLATE ascii_bin NOT NULL DEFAULT '7d',
  `upload_retention` varchar(32) COLLATE ascii_bin NOT NULL DEFAULT '24h',
  `cache_enabled` tinyint(1) NOT NULL DEFAULT '0',
  `cache_soft_limit` varchar(32) COLLATE ascii_bin DEFAULT NULL,
  `cache_entries` bigint DEFAULT NULL,
  `upload_cache` tinyint(1) NOT NULL DEFAULT '0',
  `upload_cache_size` varchar(32) COLLATE ascii_bin DEFAULT NULL,
  `compression_enabled` tinyint(1) NOT NULL DEFAULT '0',
  `compression_strategy` varchar(8) COLLATE ascii_bin NOT NULL DEFAULT 'always',
  `compression_level` int NOT NULL DEFAULT '6',
  `compression_sample_level` int NOT NULL DEFAULT '3',
  `compression_min_savings_percent` int NOT NULL DEFAULT '2',
  `compression_min_savings_bytes` bigint NOT NULL DEFAULT '256',
  `pack_enabled` tinyint(1) NOT NULL DEFAULT '0',
  `pack_max_size` varchar(32) COLLATE ascii_bin NOT NULL DEFAULT '32MiB',
  `pack_upload_flush_timeout` varchar(32) COLLATE ascii_bin NOT NULL DEFAULT '60s',
  `pack_interval` varchar(32) COLLATE ascii_bin NOT NULL DEFAULT '1h',
  `pack_reuse_interval` varchar(32) COLLATE ascii_bin NOT NULL DEFAULT '5m',
  `pack_reclaim_interval` varchar(32) COLLATE ascii_bin NOT NULL DEFAULT '30m',
  `pack_repack_interval` varchar(32) COLLATE ascii_bin NOT NULL DEFAULT '24h',
  `pack_repack_cooldown` varchar(32) COLLATE ascii_bin NOT NULL DEFAULT '1h',
  `pack_reclaim_min_savings_bytes` varchar(32) COLLATE ascii_bin NOT NULL DEFAULT '4MiB',
  `pack_range_enabled` tinyint(1) NOT NULL DEFAULT '0',
  `pack_range_interval` varchar(32) COLLATE ascii_bin NOT NULL DEFAULT '15m',
  `pack_range_window` varchar(32) COLLATE ascii_bin NOT NULL DEFAULT '24h',
  `pack_range_min_downloads` int NOT NULL DEFAULT '8',
  `pack_range_min_savings_percent` int NOT NULL DEFAULT '50',
  `pack_range_min_savings_bytes` varchar(32) COLLATE ascii_bin NOT NULL DEFAULT '64MiB',
  `pack_range_repack_after` varchar(32) COLLATE ascii_bin NOT NULL DEFAULT '30d',
  `pack_range_repack_window` varchar(32) COLLATE ascii_bin NOT NULL DEFAULT '7d',
  `pack_range_repack_retry_interval` varchar(32) COLLATE ascii_bin NOT NULL DEFAULT '7d',
  `pack_concurrency` int NOT NULL DEFAULT '1',
  `integrity_concurrency` int NOT NULL DEFAULT '1',
  `integrity_rps` bigint DEFAULT NULL,
  `integrity_bandwidth` varchar(32) COLLATE ascii_bin DEFAULT NULL,
  `integrity_batch_timeout` varchar(32) COLLATE ascii_bin NOT NULL DEFAULT '30s',
  `read_concurrency` int DEFAULT NULL,
  `upload_concurrency` int DEFAULT NULL,
  `control_concurrency` int DEFAULT NULL,
  `pack_byte_budget` varchar(32) COLLATE ascii_bin DEFAULT NULL,
  `integrity_byte_budget` varchar(32) COLLATE ascii_bin DEFAULT NULL,
  `maintenance_concurrency` int DEFAULT NULL,
  `maintenance_byte_budget` varchar(32) COLLATE ascii_bin DEFAULT NULL,
  `migration_concurrency` int DEFAULT NULL,
  `migration_byte_budget` varchar(32) COLLATE ascii_bin DEFAULT NULL,
  `updated_at` datetime(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
  `cdc_enabled` tinyint(1) NOT NULL DEFAULT '1',
  `deduplication_enabled` tinyint(1) NOT NULL DEFAULT '0',
  `multipart_idle_timeout` varchar(32) CHARACTER SET ascii COLLATE ascii_bin NOT NULL DEFAULT '24h',
  `min_free_space` varchar(32) CHARACTER SET ascii COLLATE ascii_bin DEFAULT NULL,
  `write_processing` varchar(16) CHARACTER SET ascii COLLATE ascii_bin NOT NULL DEFAULT 'inline',
  `upload_cache_overflow` varchar(16) COLLATE ascii_bin NOT NULL DEFAULT 'fallback',
  `write_durability` varchar(16) COLLATE ascii_bin NOT NULL DEFAULT 'durable',
  `write_flush_interval` bigint NOT NULL DEFAULT '0',
  `write_backlog_max_age` bigint NOT NULL DEFAULT '300',
  `background_scheduling` varchar(32) COLLATE ascii_bin NOT NULL DEFAULT 'balanced',
  `public_cache_validation` varchar(32) COLLATE ascii_bin NOT NULL DEFAULT 'strict',
  `public_cache_refresh_interval` varchar(32) COLLATE ascii_bin NOT NULL DEFAULT '1s',
  `public_cache_max_age` varchar(32) COLLATE ascii_bin NOT NULL DEFAULT '5s',
  `read_source_validation` varchar(32) COLLATE ascii_bin NOT NULL DEFAULT 'strict',
  `source_validation_max_age` varchar(32) COLLATE ascii_bin NOT NULL DEFAULT '5s',
  `read_integrity` varchar(32) COLLATE ascii_bin NOT NULL DEFAULT 'inline',
  `file_delivery` varchar(32) COLLATE ascii_bin NOT NULL DEFAULT 'application',
  `file_delivery_prefix` varchar(512) COLLATE ascii_bin DEFAULT NULL,
  `cors_cache_enabled` tinyint(1) NOT NULL DEFAULT '0',
  PRIMARY KEY (`storage_id`),
  CONSTRAINT `storage_profiles_storage` FOREIGN KEY (`storage_id`) REFERENCES `storages` (`id`) ON DELETE CASCADE,
  CONSTRAINT `storage_background_scheduling` CHECK ((`background_scheduling` in (_ascii'balanced',_ascii'foreground'))),
  CONSTRAINT `storage_delivery_prefix` CHECK ((((`file_delivery` = _ascii'application') and (`file_delivery_prefix` is null)) or ((`file_delivery` <> _ascii'application') and (`file_delivery_prefix` is not null)))),
  CONSTRAINT `storage_file_delivery` CHECK ((`file_delivery` in (_ascii'application',_ascii'x_accel_redirect',_ascii'x_sendfile'))),
  CONSTRAINT `storage_profiles_cache_entries` CHECK (((`cache_entries` is null) or (`cache_entries` > 0))),
  CONSTRAINT `storage_profiles_compression_bytes` CHECK ((`compression_min_savings_bytes` >= 0)),
  CONSTRAINT `storage_profiles_compression_percent` CHECK ((`compression_min_savings_percent` between 0 and 100)),
  CONSTRAINT `storage_profiles_compression_strategy` CHECK ((`compression_strategy` in (_ascii'never',_ascii'always',_ascii'sample'))),
  CONSTRAINT `storage_profiles_control` CHECK (((`control_concurrency` is null) or (`control_concurrency` > 0))),
  CONSTRAINT `storage_profiles_integrity_concurrency` CHECK ((`integrity_concurrency` > 0)),
  CONSTRAINT `storage_profiles_integrity_rps` CHECK (((`integrity_rps` is null) or (`integrity_rps` > 0))),
  CONSTRAINT `storage_profiles_maintenance` CHECK (((`maintenance_concurrency` is null) or (`maintenance_concurrency` > 0))),
  CONSTRAINT `storage_profiles_max_retries` CHECK ((`max_retries` between 0 and 100)),
  CONSTRAINT `storage_profiles_migration` CHECK (((`migration_concurrency` is null) or (`migration_concurrency` > 0))),
  CONSTRAINT `storage_profiles_pack_concurrency` CHECK ((`pack_concurrency` > 0)),
  CONSTRAINT `storage_profiles_pack_downloads` CHECK ((`pack_range_min_downloads` > 0)),
  CONSTRAINT `storage_profiles_pack_percent` CHECK ((`pack_range_min_savings_percent` between 1 and 100)),
  CONSTRAINT `storage_profiles_read` CHECK (((`read_concurrency` is null) or (`read_concurrency` > 0))),
  CONSTRAINT `storage_profiles_revision` CHECK ((`revision` > 0)),
  CONSTRAINT `storage_profiles_upload` CHECK (((`upload_concurrency` is null) or (`upload_concurrency` > 0))),
  CONSTRAINT `storage_public_cache_validation` CHECK ((`public_cache_validation` in (_ascii'strict',_ascii'background'))),
  CONSTRAINT `storage_read_integrity` CHECK ((`read_integrity` in (_ascii'inline',_ascii'background',_ascii'off'))),
  CONSTRAINT `storage_source_validation` CHECK ((`read_source_validation` in (_ascii'strict',_ascii'background'))),
  CONSTRAINT `storage_upload_overflow_check` CHECK ((`upload_cache_overflow` in (_ascii'fallback',_ascii'reject'))),
  CONSTRAINT `storage_write_durability` CHECK ((`write_durability` in (_ascii'durable',_ascii'buffered',_ascii'volatile')))
) ENGINE=InnoDB DEFAULT CHARSET=ascii COLLATE=ascii_bin;

CREATE TABLE `storages` (
  `id` binary(16) NOT NULL,
  `name` varchar(128) /*!80000 COLLATE utf8mb4_0900_bin */ /*M!100603 COLLATE utf8mb4_nopad_bin */ NOT NULL,
  `type` varchar(5) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
  `path` text /*!80000 COLLATE utf8mb4_0900_bin */ /*M!100603 COLLATE utf8mb4_nopad_bin */,
  `endpoint_url` text /*!80000 COLLATE utf8mb4_0900_bin */ /*M!100603 COLLATE utf8mb4_nopad_bin */,
  `remote_bucket_name` text /*!80000 COLLATE utf8mb4_0900_bin */ /*M!100603 COLLATE utf8mb4_nopad_bin */,
  `region` text /*!80000 COLLATE utf8mb4_0900_bin */ /*M!100603 COLLATE utf8mb4_nopad_bin */,
  `prefix` text /*!80000 COLLATE utf8mb4_0900_bin */ /*M!100603 COLLATE utf8mb4_nopad_bin */ NOT NULL DEFAULT (_utf8mb4''),
  `access_key_encrypted` blob,
  `secret_key_encrypted` blob,
  `identity_marker` json NOT NULL,
  `enabled` tinyint(1) NOT NULL DEFAULT '1',
  `block_key_id` binary(16) DEFAULT NULL,
  `created_at` datetime(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
  `updated_at` datetime(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
  PRIMARY KEY (`id`),
  UNIQUE KEY `name` (`name`),
  KEY `storages_key` (`block_key_id`),
  CONSTRAINT `storages_key` FOREIGN KEY (`block_key_id`) REFERENCES `managed_keys` (`id`),
  CONSTRAINT `storages_connection` CHECK ((((`type` = _utf8mb4'local') and (`path` is not null) and (`endpoint_url` is null) and (`remote_bucket_name` is null) and (`region` is null) and (`access_key_encrypted` is null) and (`secret_key_encrypted` is null)) or ((`type` = _utf8mb4's3') and (`path` is null) and (`endpoint_url` is not null) and (`remote_bucket_name` is not null) and (`region` is not null) and (`access_key_encrypted` is not null) and (`secret_key_encrypted` is not null)))),
  CONSTRAINT `storages_name` CHECK ((length(`name`) between 1 and 128)),
  CONSTRAINT `storages_type` CHECK ((`type` in (_utf8mb4'local',_utf8mb4's3')))
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 /*!80000 COLLATE=utf8mb4_0900_bin */ /*M!100603 COLLATE=utf8mb4_nopad_bin */;

CREATE TABLE `streams` (
  `id` bigint NOT NULL AUTO_INCREMENT,
  `bucket_id` binary(16) NOT NULL,
  `object_key` varbinary(1024) NOT NULL,
  `stream_type` varchar(6) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
  `state` varchar(9) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
  `size` bigint NOT NULL DEFAULT '0',
  `etag` varchar(128) CHARACTER SET ascii COLLATE ascii_bin NOT NULL DEFAULT '',
  `metadata` json NOT NULL DEFAULT (json_object()),
  `public_read` tinyint(1) NOT NULL DEFAULT '0',
  `checksums` json NOT NULL DEFAULT (json_object()),
  `upload_cache_bypass` tinyint(1) NOT NULL DEFAULT '0',
  `write_authorization` json NOT NULL,
  `owner_task_id` binary(16) DEFAULT NULL,
  `owner_execution_token` binary(16) DEFAULT NULL,
  `created_at` datetime(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
  `touched_at` datetime(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
  `http_compression` json NOT NULL DEFAULT (json_object()),
  `payload_type` varchar(6) CHARACTER SET ascii COLLATE ascii_bin NOT NULL DEFAULT 'chunks',
  `payload_revision` bigint NOT NULL DEFAULT '1',
  PRIMARY KEY (`id`),
  KEY `streams_owner` (`owner_task_id`),
  KEY `streams_cleanup` (`state`,`touched_at`,`id`),
  KEY `streams_writing` (`bucket_id`,`object_key`,`state`),
  CONSTRAINT `streams_bucket` FOREIGN KEY (`bucket_id`) REFERENCES `buckets` (`id`),
  CONSTRAINT `streams_owner` FOREIGN KEY (`owner_task_id`) REFERENCES `tasks` (`id`) ON DELETE SET NULL,
  CONSTRAINT `streams_chk_1` CHECK ((`payload_type` in (_ascii'chunks',_ascii'raw'))),
  CONSTRAINT `streams_chk_2` CHECK ((`payload_revision` > 0)),
  CONSTRAINT `streams_key` CHECK ((length(`object_key`) between 1 and 1024)),
  CONSTRAINT `streams_size` CHECK ((`size` >= 0)),
  CONSTRAINT `streams_state` CHECK ((`state` in (_ascii'writing',_ascii'ready',_ascii'retired',_ascii'abandoned'))),
  CONSTRAINT `streams_type` CHECK ((`stream_type` in (_ascii'object',_ascii'part')))
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin;

CREATE TABLE `tasks` (
  `id` binary(16) NOT NULL,
  `task_type` varchar(17) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
  `bucket_id` binary(16) DEFAULT NULL,
  `state` varchar(9) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
  `progress_cursor` text /*!80000 COLLATE utf8mb4_0900_bin */ /*M!100603 COLLATE utf8mb4_nopad_bin */,
  `processed` bigint NOT NULL DEFAULT '0',
  `detail` json NOT NULL DEFAULT (json_object()),
  `policy` varchar(17) CHARACTER SET ascii COLLATE ascii_bin GENERATED ALWAYS AS (coalesce(nullif((cast(json_unquote(json_extract(`detail`,_utf8mb4'$.policy')) as char charset ascii) collate ascii_bin),_ascii'null'),`task_type`)) VIRTUAL,
  `error` text /*!80000 COLLATE utf8mb4_0900_bin */ /*M!100603 COLLATE utf8mb4_nopad_bin */,
  `created_by_label` varchar(64) /*!80000 COLLATE utf8mb4_0900_bin */ /*M!100603 COLLATE utf8mb4_nopad_bin */ NOT NULL DEFAULT 'Unknown',
  `source` varchar(16) /*!80000 COLLATE utf8mb4_0900_bin */ /*M!100603 COLLATE utf8mb4_nopad_bin */ NOT NULL DEFAULT 'unknown',
  `execution_token` binary(16) DEFAULT NULL,
  `created_at` datetime(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
  `updated_at` datetime(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
  `started_at` datetime(6) DEFAULT NULL,
  PRIMARY KEY (`id`),
  KEY `tasks_bucket` (`bucket_id`),
  KEY `tasks_work` (`state`,`updated_at`,`id`),
  KEY `tasks_list` (`created_at` DESC,`id` DESC),
  KEY `tasks_state_list` (`state`,`created_at` DESC,`id` DESC),
  KEY `tasks_policy_recent` (`policy`,`created_at` DESC,`id` DESC),
  CONSTRAINT `tasks_bucket` FOREIGN KEY (`bucket_id`) REFERENCES `buckets` (`id`) ON DELETE SET NULL,
  CONSTRAINT `tasks_processed` CHECK ((`processed` >= 0)),
  CONSTRAINT `tasks_state` CHECK ((`state` in (_utf8mb4'queued',_utf8mb4'running',_utf8mb4'paused',_utf8mb4'completed',_utf8mb4'failed'))),
  CONSTRAINT `tasks_type` CHECK ((`task_type` in (_utf8mb4'purge',_utf8mb4'sweep',_utf8mb4'integrity',_utf8mb4'pack',_utf8mb4'unpack',_utf8mb4'upload',_utf8mb4'cache_flush',_utf8mb4'gc',_utf8mb4'cleanup',_utf8mb4'storage_migration',_utf8mb4'rewrite',_utf8mb4'usage')))
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 /*!80000 COLLATE=utf8mb4_0900_bin */ /*M!100603 COLLATE=utf8mb4_nopad_bin */;

CREATE TABLE `token_bucket_grants` (
  `token_id` binary(16) NOT NULL,
  `bucket_id` binary(16) NOT NULL,
  `actions` smallint unsigned NOT NULL,
  PRIMARY KEY (`token_id`,`bucket_id`),
  KEY `token_grants_bucket` (`bucket_id`),
  CONSTRAINT `token_grants_bucket` FOREIGN KEY (`bucket_id`) REFERENCES `buckets` (`id`) ON DELETE CASCADE,
  CONSTRAINT `token_grants_token` FOREIGN KEY (`token_id`) REFERENCES `api_tokens` (`id`) ON DELETE CASCADE,
  CONSTRAINT `token_grants_actions` CHECK ((`actions` <= 127))
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin;

CREATE TABLE `uploads` (
  `id` binary(16) NOT NULL,
  `bucket_id` binary(16) NOT NULL,
  `object_key` varbinary(1024) NOT NULL,
  `access_key` varchar(128) /*!80000 COLLATE utf8mb4_0900_bin */ /*M!100603 COLLATE utf8mb4_nopad_bin */ NOT NULL,
  `state` varchar(10) CHARACTER SET ascii COLLATE ascii_bin NOT NULL DEFAULT 'active',
  `metadata` json NOT NULL DEFAULT (json_object()),
  `public_read` tinyint(1) NOT NULL DEFAULT '0',
  `checksum_algorithm` varchar(16) CHARACTER SET ascii COLLATE ascii_bin DEFAULT NULL,
  `checksum_type` varchar(16) CHARACTER SET ascii COLLATE ascii_bin DEFAULT NULL,
  `manifest_hash` varchar(128) CHARACTER SET ascii COLLATE ascii_bin DEFAULT NULL,
  `result` json DEFAULT NULL,
  `output_stream` bigint DEFAULT NULL,
  `created_at` datetime(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
  `touched_at` datetime(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
  `durable_at` datetime(6) DEFAULT NULL,
  `completion_token` binary(16) DEFAULT NULL,
  `completion_expires_at` datetime(6) DEFAULT NULL,
  PRIMARY KEY (`id`),
  KEY `uploads_listing` (`bucket_id`,`object_key`,`id`),
  KEY `uploads_cleanup` (`state`,`touched_at`,`id`),
  KEY `uploads_output` (`output_stream`),
  CONSTRAINT `uploads_bucket` FOREIGN KEY (`bucket_id`) REFERENCES `buckets` (`id`),
  CONSTRAINT `uploads_output` FOREIGN KEY (`output_stream`) REFERENCES `streams` (`id`),
  CONSTRAINT `uploads_key` CHECK ((length(`object_key`) between 1 and 1024)),
  CONSTRAINT `uploads_state` CHECK ((`state` in (_ascii'active',_ascii'completing',_ascii'completed',_ascii'aborted')))
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 /*!80000 COLLATE=utf8mb4_0900_bin */ /*M!100603 COLLATE=utf8mb4_nopad_bin */;

CREATE TABLE `usage_buckets` (
  `scope_id` varchar(64) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
  `bucket_id` binary(16) NOT NULL,
  PRIMARY KEY (`scope_id`,`bucket_id`),
  CONSTRAINT `usage_buckets_scope` FOREIGN KEY (`scope_id`) REFERENCES `usage_summary` (`id`) ON DELETE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin;

CREATE TABLE `usage_history` (
  `scope_id` varchar(64) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
  `snapshot_at` datetime(6) NOT NULL,
  `data` json NOT NULL,
  PRIMARY KEY (`scope_id`,`snapshot_at`),
  KEY `usage_history_expiry` (`snapshot_at`),
  CONSTRAINT `usage_history_scope` FOREIGN KEY (`scope_id`) REFERENCES `usage_summary` (`id`) ON DELETE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin;

CREATE TABLE `usage_summary` (
  `id` varchar(64) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
  `requested_at` datetime(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
  `as_of` datetime(6) DEFAULT NULL,
  `data` json DEFAULT NULL,
  PRIMARY KEY (`id`),
  KEY `usage_requested` (`requested_at`,`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin;

CREATE TABLE `web_uploads` (
  `upload_id` binary(16) NOT NULL,
  `user_id` binary(16) DEFAULT NULL,
  `client_id` binary(16) NOT NULL,
  `request_hash` binary(32) NOT NULL,
  `file_name` varchar(1024) /*!80000 COLLATE utf8mb4_0900_bin */ /*M!100603 COLLATE utf8mb4_nopad_bin */ NOT NULL,
  `expected_size` bigint NOT NULL,
  `part_size` bigint NOT NULL,
  `modified_at` bigint DEFAULT NULL,
  `expected_stream_id` bigint DEFAULT NULL,
  PRIMARY KEY (`upload_id`),
  UNIQUE KEY `web_uploads_client` (`user_id`,`client_id`),
  CONSTRAINT `web_uploads_upload` FOREIGN KEY (`upload_id`) REFERENCES `uploads` (`id`) ON DELETE CASCADE,
  CONSTRAINT `web_uploads_user` FOREIGN KEY (`user_id`) REFERENCES `web_users` (`id`) ON DELETE SET NULL,
  CONSTRAINT `web_uploads_modified` CHECK (((`modified_at` is null) or (`modified_at` >= 0))),
  CONSTRAINT `web_uploads_name` CHECK ((length(`file_name`) between 1 and 1024)),
  CONSTRAINT `web_uploads_part_size` CHECK ((`part_size` between 5242880 and 5368709120)),
  CONSTRAINT `web_uploads_size` CHECK ((`expected_size` between 0 and 53687091200000))
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 /*!80000 COLLATE=utf8mb4_0900_bin */ /*M!100603 COLLATE=utf8mb4_nopad_bin */;

CREATE TABLE `web_users` (
  `id` binary(16) NOT NULL,
  `username` varchar(64) /*!80000 COLLATE utf8mb4_0900_bin */ /*M!100603 COLLATE utf8mb4_nopad_bin */ NOT NULL,
  `password_hash` varchar(255) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
  `enabled` tinyint(1) NOT NULL DEFAULT '1',
  `created_at` datetime(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
  `display_name` varchar(240) /*!80000 COLLATE utf8mb4_0900_bin */ /*M!100603 COLLATE utf8mb4_nopad_bin */ NOT NULL DEFAULT '',
  `locale` varchar(5) CHARACTER SET ascii COLLATE ascii_bin DEFAULT NULL,
  `theme` varchar(5) CHARACTER SET ascii COLLATE ascii_bin DEFAULT NULL,
  `avatar_email` varchar(320) /*!80000 COLLATE utf8mb4_0900_bin */ /*M!100603 COLLATE utf8mb4_nopad_bin */ NOT NULL DEFAULT '',
  `avatar_enabled` tinyint(1) NOT NULL DEFAULT '0',
  `auth_revision` bigint NOT NULL DEFAULT '0',
  `role` varchar(6) CHARACTER SET ascii COLLATE ascii_bin NOT NULL DEFAULT 'member',
  `authorization_revision` bigint NOT NULL DEFAULT '0',
  `must_change_password` tinyint(1) NOT NULL DEFAULT '0',
  PRIMARY KEY (`id`),
  UNIQUE KEY `username` (`username`),
  CONSTRAINT `web_users_avatar_email` CHECK ((length(`avatar_email`) <= 320)),
  CONSTRAINT `web_users_display_name` CHECK ((length(`display_name`) <= 240)),
  CONSTRAINT `web_users_locale` CHECK ((`locale` in (_utf8mb4'en',_utf8mb4'zh-CN',_utf8mb4'ja'))),
  CONSTRAINT `web_users_role` CHECK ((`role` in (_utf8mb4'admin',_utf8mb4'member'))),
  CONSTRAINT `web_users_theme` CHECK ((`theme` in (_utf8mb4'auto',_utf8mb4'light',_utf8mb4'dark'))),
  CONSTRAINT `web_users_username` CHECK ((length(`username`) between 1 and 64))
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 /*!80000 COLLATE=utf8mb4_0900_bin */ /*M!100603 COLLATE=utf8mb4_nopad_bin */;

INSERT INTO `access_state` (`id`, `coverage_since`, `flushed_at`) VALUES ('1', CURRENT_TIMESTAMP(6), CURRENT_TIMESTAMP(6));
INSERT INTO `maintenance_controls` (`policy`, `paused`, `last_scheduled_at`, `updated_at`) VALUES ('cleanup', '0', CURRENT_TIMESTAMP(6), CURRENT_TIMESTAMP(6));
INSERT INTO `maintenance_controls` (`policy`, `paused`, `last_scheduled_at`, `updated_at`) VALUES ('gc', '0', CURRENT_TIMESTAMP(6), CURRENT_TIMESTAMP(6));
INSERT INTO `maintenance_controls` (`policy`, `paused`, `last_scheduled_at`, `updated_at`) VALUES ('pack', '0', CURRENT_TIMESTAMP(6), CURRENT_TIMESTAMP(6));
INSERT INTO `maintenance_controls` (`policy`, `paused`, `last_scheduled_at`, `updated_at`) VALUES ('range', '0', CURRENT_TIMESTAMP(6), CURRENT_TIMESTAMP(6));
INSERT INTO `maintenance_controls` (`policy`, `paused`, `last_scheduled_at`, `updated_at`) VALUES ('reclaim', '0', CURRENT_TIMESTAMP(6), CURRENT_TIMESTAMP(6));
INSERT INTO `maintenance_controls` (`policy`, `paused`, `last_scheduled_at`, `updated_at`) VALUES ('repack', '0', CURRENT_TIMESTAMP(6), CURRENT_TIMESTAMP(6));
INSERT INTO `maintenance_controls` (`policy`, `paused`, `last_scheduled_at`, `updated_at`) VALUES ('reuse', '0', CURRENT_TIMESTAMP(6), CURRENT_TIMESTAMP(6));
INSERT INTO `projects` (`id`, `name`, `description`, `builtin`, `allow_bucket_create`, `created_at`) VALUES (X'00000000000000000000000000000001', 'Default', '', '1', '0', CURRENT_TIMESTAMP(6));
INSERT INTO `quota_scopes` (`scope`, `scope_id`, `used_bytes`, `reserved_bytes`, `inflight_bytes`, `object_count`, `bucket_count`, `logical_bytes_limit`, `inflight_bytes_limit`, `bucket_count_limit`) VALUES ('project', X'00000000000000000000000000000001', '0', '0', '0', '0', '0', NULL, NULL, NULL);
INSERT INTO `usage_summary` (`id`, `requested_at`, `as_of`, `data`) VALUES ('global', CURRENT_TIMESTAMP(6), NULL, NULL);

SET foreign_key_checks=@mokyu_schema_foreign_keys;

CREATE SQL SECURITY DEFINER VIEW `user_bucket_access` AS select `m`.`user_id` AS `user_id`,`b`.`id` AS `bucket_id`,((case `m`.`role` when 'reader' then 67 when 'writer' then 95 when 'maintainer' then 127 else 0 end) & if((`m`.`scope` = 'all'),127,coalesce(`g`.`actions`,0))) AS `actions` from ((`project_members` `m` join `buckets` `b` on((`b`.`project_id` = `m`.`project_id`))) left join `member_bucket_grants` `g` on(((`g`.`user_id` = `m`.`user_id`) and (`g`.`bucket_id` = `b`.`id`))));

CREATE SQL SECURITY DEFINER VIEW `token_bucket_access` AS select `t`.`id` AS `token_id`,`g`.`bucket_id` AS `bucket_id`,(`g`.`actions` & if((`u`.`role` = 'admin'),127,coalesce(`a`.`actions`,0))) AS `actions` from (((`api_tokens` `t` join `web_users` `u` on((`u`.`id` = `t`.`user_id`))) join `token_bucket_grants` `g` on((`g`.`token_id` = `t`.`id`))) left join `user_bucket_access` `a` on(((`a`.`user_id` = `t`.`user_id`) and (`a`.`bucket_id` = `g`.`bucket_id`))));

CREATE TRIGGER `encodings_owner_insert` BEFORE INSERT ON `chunk_encodings` FOR EACH ROW BEGIN
    IF NEW.owner_task IS NOT NULL AND NEW.owner_stream IS NOT NULL THEN
        SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT='encoding owners are exclusive';
    END IF;
END;

CREATE TRIGGER `encodings_owner_update` BEFORE UPDATE ON `chunk_encodings` FOR EACH ROW BEGIN
    IF NEW.owner_task IS NOT NULL AND NEW.owner_stream IS NOT NULL THEN
        SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT='encoding owners are exclusive';
    END IF;
END;
