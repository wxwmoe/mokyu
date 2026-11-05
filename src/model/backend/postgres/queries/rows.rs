use super::*;

impl<'r> sqlx::FromRow<'r, sqlx::postgres::PgRow> for crate::app::Bucket {
    fn from_row(row: &'r sqlx::postgres::PgRow) -> sqlx::Result<Self> {
        use sqlx::Row;
        Ok(Self {
            id: row.try_get("id")?,
            project_id: row.try_get("project_id")?,
            name: row.try_get("name")?,
            storage_id: row.try_get("storage_id")?,
            state: row.try_get("state")?,
            settings_revision: row.try_get("settings_revision")?,
            image_revision: row.try_get("image_revision")?,
            uploads_paused: row.try_get("uploads_paused")?,
            public_base_url: row.try_get("public_base_url")?,
            cors: row.try_get::<serde_json::Value, _>("cors")?.into(),
            website_enabled: row.try_get("website_enabled")?,
            index_document: row.try_get("index_document")?,
            error_document: row.try_get("error_document")?,
            created_at: row.try_get("created_at")?,
        })
    }
}

impl<'r> sqlx::FromRow<'r, sqlx::postgres::PgRow> for crate::app::StoredStream {
    fn from_row(row: &'r sqlx::postgres::PgRow) -> sqlx::Result<Self> {
        use sqlx::Row;
        Ok(Self {
            id: row.try_get("id")?,
            bucket_id: row.try_get("bucket_id")?,
            object_key: row.try_get("object_key")?,
            stream_type: row.try_get("stream_type")?,
            payload_type: row.try_get("payload_type")?,
            payload_revision: row.try_get("payload_revision")?,
            state: row.try_get("state")?,
            size: row.try_get("size")?,
            etag: row.try_get("etag")?,
            metadata: row.try_get("metadata")?,
            public_read: row.try_get("public_read")?,
            checksums: row.try_get("checksums")?,
            http_compression: row.try_get("http_compression")?,
            created_at: row.try_get("created_at")?,
            touched_at: row.try_get("touched_at")?,
        })
    }
}

impl<'r> sqlx::FromRow<'r, sqlx::postgres::PgRow> for crate::app::Extent {
    fn from_row(row: &'r sqlx::postgres::PgRow) -> sqlx::Result<Self> {
        use sqlx::Row;
        Ok(Self {
            stream_id: row.try_get("stream_id")?,
            offset_bytes: row.try_get("offset_bytes")?,
            length: row.try_get("length")?,
            chunk_id: row.try_get("chunk_id")?,
            fragment_id: row.try_get("fragment_id")?,
            source_offset: row.try_get("source_offset")?,
        })
    }
}

impl<'r> sqlx::FromRow<'r, sqlx::postgres::PgRow> for crate::content::codec::Chunk {
    fn from_row(row: &'r sqlx::postgres::PgRow) -> sqlx::Result<Self> {
        use sqlx::Row;
        crate::model::physical::read_chunk(
            Self {
                id: row.try_get("id")?,
                uuid: row.try_get("id")?,
                encoding_id: row.try_get("current_encoding_id")?,
                home_storage_id: None,
                address: None,
                location_id: match row.try_get("location_id") {
                    Ok(value) => value,
                    Err(sqlx::Error::ColumnNotFound(_)) => Default::default(),
                    Err(error) => return Err(error),
                },
                storage_id: row.try_get("storage_id")?,
                hash: row.try_get("hash")?,
                raw_size: row.try_get("raw_size")?,
                stored_size: row.try_get("standalone_size")?,
                algorithm: row.try_get("encryption_algorithm")?,
                block_key_id: row.try_get("block_key_id")?,
                compressed: row.try_get("standalone_compressed")?,
                nonce: row.try_get("nonce")?,
                format: row.try_get("format")?,
                state: row.try_get("state")?,
                created_at: row.try_get("created_at")?,
            },
            row.try_get("tier_material"),
        )
    }
}

impl<'r> sqlx::FromRow<'r, sqlx::postgres::PgRow> for crate::content::pack::Pack {
    fn from_row(row: &'r sqlx::postgres::PgRow) -> sqlx::Result<Self> {
        use sqlx::Row;
        crate::model::physical::read_pack(
            Self {
                id: row.try_get("id")?,
                home_storage_id: None,
                address: None,
                location_id: row.try_get("id")?,
                storage_id: row.try_get("storage_id")?,
                algorithm: row.try_get("encryption_algorithm")?,
                block_key_id: row.try_get("block_key_id")?,
                raw_size: row.try_get("raw_size")?,
                stored_size: row.try_get("stored_size")?,
                compressed: row.try_get("pack_compressed")?,
                nonce: row.try_get("nonce")?,
                digest: row.try_get("digest")?,
                member_count: row.try_get("member_count")?,
                state: row.try_get("state")?,
                created_at: row.try_get("created_at")?,
            },
            row.try_get("tier_material"),
        )
    }
}

impl<'r> sqlx::FromRow<'r, sqlx::postgres::PgRow> for crate::model::account::Profile {
    fn from_row(row: &'r sqlx::postgres::PgRow) -> sqlx::Result<Self> {
        use sqlx::Row;
        Ok(Self {
            id: row.try_get("id")?,
            username: row.try_get("username")?,
            role: row.try_get("role")?,
            must_change_password: row.try_get("must_change_password")?,
            project_management: row.try_get("project_management")?,
            display_name: row.try_get("display_name")?,
            locale: row.try_get("locale")?,
            theme: row.try_get("theme")?,
            avatar_email: row.try_get("avatar_email")?,
            avatar_enabled: row.try_get("avatar_enabled")?,
            avatar_url: Default::default(),
        })
    }
}

impl<'r> sqlx::FromRow<'r, sqlx::postgres::PgRow> for crate::model::account::Session {
    fn from_row(row: &'r sqlx::postgres::PgRow) -> sqlx::Result<Self> {
        use sqlx::Row;
        Ok(Self {
            id: row.try_get("id")?,
            created_at: row.try_get("created_at")?,
            last_seen_at: row.try_get("last_seen_at")?,
            expires_at: row.try_get("expires_at")?,
            user_agent: row.try_get("user_agent")?,
            current: row.try_get("current")?,
        })
    }
}

impl<'r> sqlx::FromRow<'r, sqlx::postgres::PgRow> for crate::model::account::AuthSession {
    fn from_row(row: &'r sqlx::postgres::PgRow) -> sqlx::Result<Self> {
        use sqlx::Row;
        Ok(Self {
            user_id: row.try_get("user_id")?,
            session_id: row.try_get("session_id")?,
            role: row.try_get("role")?,
            must_change_password: row.try_get("must_change_password")?,
            authorization_revision: row.try_get("authorization_revision")?,
            csrf_hash: row.try_get("csrf_hash")?,
            last_seen_at: row.try_get("last_seen_at")?,
        })
    }
}

impl<'r> sqlx::FromRow<'r, sqlx::postgres::PgRow> for crate::model::administration::User {
    fn from_row(row: &'r sqlx::postgres::PgRow) -> sqlx::Result<Self> {
        use sqlx::Row;
        Ok(Self {
            id: row.try_get("id")?,
            username: row.try_get("username")?,
            display_name: row.try_get("display_name")?,
            role: row.try_get("role")?,
            enabled: row.try_get("enabled")?,
            must_change_password: row.try_get("must_change_password")?,
            created_at: row.try_get("created_at")?,
        })
    }
}

impl<'r> sqlx::FromRow<'r, sqlx::postgres::PgRow> for crate::model::administration::Member {
    fn from_row(row: &'r sqlx::postgres::PgRow) -> sqlx::Result<Self> {
        use sqlx::Row;
        Ok(Self {
            user_id: row.try_get("user_id")?,
            username: row.try_get("username")?,
            display_name: row.try_get("display_name")?,
            enabled: row.try_get("enabled")?,
            role: row.try_get("role")?,
            scope: row.try_get("scope")?,
            grants: row.try_get::<sqlx::types::Json<_>, _>("grants")?.0,
            external_grants: row.try_get::<sqlx::types::Json<_>, _>("external_grants")?.0,
        })
    }
}

impl<'r> sqlx::FromRow<'r, sqlx::postgres::PgRow> for crate::model::administration::Project {
    fn from_row(row: &'r sqlx::postgres::PgRow) -> sqlx::Result<Self> {
        use sqlx::Row;
        Ok(Self {
            id: row.try_get("id")?,
            name: row.try_get("name")?,
            description: row.try_get("description")?,
            builtin: row.try_get("builtin")?,
            allow_bucket_create: row.try_get("allow_bucket_create")?,
            created_at: row.try_get("created_at")?,
        })
    }
}

impl<'r> sqlx::FromRow<'r, sqlx::postgres::PgRow> for crate::model::audit::Event {
    fn from_row(row: &'r sqlx::postgres::PgRow) -> sqlx::Result<Self> {
        use sqlx::Row;
        Ok(Self {
            id: row.try_get("id")?,
            created_at: row.try_get("created_at")?,
            finished_at: row.try_get("finished_at")?,
            actor_id: row.try_get("actor_id")?,
            actor_label: row.try_get("actor_label")?,
            token_id: row.try_get("token_id")?,
            source: row.try_get("source")?,
            action: row.try_get("action")?,
            target: row.try_get("target")?,
            project_id: row.try_get("project_id")?,
            bucket_id: row.try_get("bucket_id")?,
            outcome: row.try_get("outcome")?,
            request_id: row.try_get("request_id")?,
            status: row.try_get("http_status")?,
            detail: row.try_get("detail")?,
        })
    }
}

impl<'r> sqlx::FromRow<'r, sqlx::postgres::PgRow> for crate::model::bucket::BucketProject {
    fn from_row(row: &'r sqlx::postgres::PgRow) -> sqlx::Result<Self> {
        use sqlx::Row;
        Ok(Self {
            id: row.try_get("id")?,
            name: row.try_get("name")?,
        })
    }
}

impl<'r> sqlx::FromRow<'r, sqlx::postgres::PgRow> for crate::model::bucket::BucketStorage {
    fn from_row(row: &'r sqlx::postgres::PgRow) -> sqlx::Result<Self> {
        use sqlx::Row;
        Ok(Self {
            id: row.try_get("id")?,
            name: row.try_get("name")?,
            storage_type: row.try_get("storage_type")?,
        })
    }
}

impl<'r> sqlx::FromRow<'r, sqlx::postgres::PgRow> for crate::model::bucket::Domain {
    fn from_row(row: &'r sqlx::postgres::PgRow) -> sqlx::Result<Self> {
        use sqlx::Row;
        Ok(Self {
            host: row.try_get("host")?,
            bucket_id: row.try_get("bucket_id")?,
        })
    }
}

impl<'r> sqlx::FromRow<'r, sqlx::postgres::PgRow> for crate::model::bucket::BucketImpact {
    fn from_row(row: &'r sqlx::postgres::PgRow) -> sqlx::Result<Self> {
        use sqlx::Row;
        Ok(Self {
            bucket_id: row.try_get("bucket_id")?,
            name: row.try_get("name")?,
            project_id: row.try_get("project_id")?,
            state: row.try_get("state")?,
            revision: row.try_get("revision")?,
            uploads_paused: row.try_get("uploads_paused")?,
            objects: row.try_get("objects")?,
            logical_bytes: row.try_get("logical_bytes")?,
            reserved_bytes: row.try_get("reserved_bytes")?,
            inflight_bytes: row.try_get("inflight_bytes")?,
            active_uploads: row.try_get("active_uploads")?,
            writing_streams: row.try_get("writing_streams")?,
            member_bucket_grants: row.try_get("member_bucket_grants")?,
            service_grants: row.try_get("service_grants")?,
            token_bucket_grants: row.try_get("token_bucket_grants")?,
            domains: row.try_get("domains")?,
        })
    }
}

impl<'r> sqlx::FromRow<'r, sqlx::postgres::PgRow> for crate::model::bucket::TransferProject {
    fn from_row(row: &'r sqlx::postgres::PgRow) -> sqlx::Result<Self> {
        use sqlx::Row;
        Ok(Self {
            id: row.try_get("id")?,
            name: row.try_get("name")?,
            used_bytes: row.try_get("used_bytes")?,
            reserved_bytes: row.try_get("reserved_bytes")?,
            logical_bytes_limit: row.try_get("logical_bytes_limit")?,
            bucket_count: row.try_get("bucket_count")?,
            bucket_count_limit: row.try_get("bucket_count_limit")?,
            members: row.try_get("members")?,
        })
    }
}

impl<'r> sqlx::FromRow<'r, sqlx::postgres::PgRow> for crate::model::bucket::BucketView {
    fn from_row(row: &'r sqlx::postgres::PgRow) -> sqlx::Result<Self> {
        use sqlx::Row;
        Ok(Self {
            id: row.try_get("id")?,
            project_id: row.try_get("project_id")?,
            storage_id: row.try_get("storage_id")?,
            actions: row.try_get("actions")?,
            name: row.try_get("name")?,
            state: row.try_get("state")?,
            revision: row.try_get("revision")?,
            uploads_paused: row.try_get("uploads_paused")?,
            public_base_url: row.try_get("public_base_url")?,
            cors: row.try_get("cors")?,
            website_enabled: row.try_get("website_enabled")?,
            index_document: row.try_get("index_document")?,
            error_document: row.try_get("error_document")?,
            created_at: row.try_get("created_at")?,
        })
    }
}

impl<'r> sqlx::FromRow<'r, sqlx::postgres::PgRow> for crate::model::bucket::Website {
    fn from_row(row: &'r sqlx::postgres::PgRow) -> sqlx::Result<Self> {
        use sqlx::Row;
        Ok(Self {
            website_enabled: row.try_get("website_enabled")?,
            index_document: row.try_get("index_document")?,
            error_document: row.try_get("error_document")?,
        })
    }
}

impl<'r> sqlx::FromRow<'r, sqlx::postgres::PgRow> for crate::model::chunk::ReadExtent {
    fn from_row(row: &'r sqlx::postgres::PgRow) -> sqlx::Result<Self> {
        use sqlx::Row;
        Ok(Self {
            demand_pack: sqlx::Row::try_get(row, "demand_pack")?,
            extent: sqlx::FromRow::from_row(row)?,
            chunk: sqlx::FromRow::from_row(row)?,
        })
    }
}

impl<'r> sqlx::FromRow<'r, sqlx::postgres::PgRow> for crate::model::chunk::FragmentSource {
    fn from_row(row: &'r sqlx::postgres::PgRow) -> sqlx::Result<Self> {
        use sqlx::Row;
        Ok(Self {
            location_id: row.try_get("fragment_location")?,
            size: row.try_get("size")?,
            hash: row.try_get("hash")?,
            file_id: row.try_get("file_id")?,
            file_offset: row.try_get("file_offset")?,
            file_storage: row.try_get("file_storage")?,
            raw_source: row.try_get("raw_source")?,
            staging_storage_id: row.try_get("staging_storage_id")?,
            staging_object_key: row.try_get("staging_object_key")?,
        })
    }
}

impl<'r> sqlx::FromRow<'r, sqlx::postgres::PgRow> for crate::model::chunk::Mapping {
    fn from_row(row: &'r sqlx::postgres::PgRow) -> sqlx::Result<Self> {
        use sqlx::Row;
        Ok(Self {
            chunk: sqlx::FromRow::from_row(row)?,
            offset_bytes: row.try_get("offset_bytes")?,
            pack_id: row.try_get("pack_id")?,
        })
    }
}

impl<'r> sqlx::FromRow<'r, sqlx::postgres::PgRow> for crate::model::credential::Credential {
    fn from_row(row: &'r sqlx::postgres::PgRow) -> sqlx::Result<Self> {
        use sqlx::Row;
        Ok(Self {
            access_key: row.try_get("access_key_id")?,
            project_id: row.try_get("project_id")?,
            label: row.try_get("label")?,
            enabled: row.try_get("enabled")?,
            created_at: row.try_get("created_at")?,
            expires_at: row.try_get("expires_at")?,
            last_used_at: row.try_get("last_used_at")?,
            created_by: row.try_get("created_by")?,
            grants: row.try_get::<sqlx::types::Json<_>, _>("grants")?.0,
        })
    }
}

impl<'r> sqlx::FromRow<'r, sqlx::postgres::PgRow> for crate::model::credential::Token {
    fn from_row(row: &'r sqlx::postgres::PgRow) -> sqlx::Result<Self> {
        use sqlx::Row;
        Ok(Self {
            id: row.try_get("id")?,
            user_id: row.try_get("user_id")?,
            label: row.try_get("label")?,
            prefix: row.try_get("prefix")?,
            system: row.try_get("system")?,
            active: row.try_get("active")?,
            created_at: row.try_get("created_at")?,
            expires_at: row.try_get("expires_at")?,
            last_used_at: row.try_get("last_used_at")?,
            revoked_at: row.try_get("revoked_at")?,
            grants: row.try_get::<sqlx::types::Json<_>, _>("grants")?.0,
            external_grants: row.try_get::<sqlx::types::Json<_>, _>("external_grants")?.0,
        })
    }
}

impl<'r> sqlx::FromRow<'r, sqlx::postgres::PgRow> for crate::model::key::ManagedKeyRecord {
    fn from_row(row: &'r sqlx::postgres::PgRow) -> sqlx::Result<Self> {
        use sqlx::Row;
        Ok(Self {
            id: row.try_get("id")?,
            name: row.try_get("name")?,
            algorithm: row.try_get("algorithm")?,
        })
    }
}

impl<'r> sqlx::FromRow<'r, sqlx::postgres::PgRow> for crate::model::key::ManagedKeyMaterial {
    fn from_row(row: &'r sqlx::postgres::PgRow) -> sqlx::Result<Self> {
        use sqlx::Row;
        Ok(Self {
            id: row.try_get("id")?,
            name: row.try_get("name")?,
            purpose: row.try_get("purpose")?,
            algorithm: row.try_get("key_algorithm")?,
            encrypted: row.try_get("secret_encrypted")?,
            fingerprint: row.try_get("fingerprint")?,
        })
    }
}

impl<'r> sqlx::FromRow<'r, sqlx::postgres::PgRow> for crate::model::multipart::Upload {
    fn from_row(row: &'r sqlx::postgres::PgRow) -> sqlx::Result<Self> {
        use sqlx::Row;
        Ok(Self {
            id: row.try_get("id")?,
            bucket_id: row.try_get("bucket_id")?,
            object_key: row.try_get("object_key")?,
            state: row.try_get("state")?,
            metadata: row.try_get("metadata")?,
            public_read: row.try_get("public_read")?,
            checksum_algorithm: row.try_get("checksum_algorithm")?,
            checksum_type: row.try_get("checksum_type")?,
            manifest_hash: row.try_get("manifest_hash")?,
            result: row.try_get("result")?,
            created_at: row.try_get("created_at")?,
        })
    }
}

impl<'r> sqlx::FromRow<'r, sqlx::postgres::PgRow> for crate::model::multipart::Transfer {
    fn from_row(row: &'r sqlx::postgres::PgRow) -> sqlx::Result<Self> {
        use sqlx::Row;
        Ok(Self {
            id: row.try_get("id")?,
            bucket_id: row.try_get("bucket_id")?,
            bucket_name: row.try_get("bucket_name")?,
            object_key: row.try_get("object_key")?,
            state: row.try_get("state")?,
            source: row.try_get("source")?,
            user_id: row.try_get("user_id")?,
            owner: row.try_get("owner")?,
            file_name: row.try_get("file_name")?,
            expected_size: row.try_get("expected_size")?,
            part_size: row.try_get("part_size")?,
            modified_at: row.try_get("modified_at")?,
            received_bytes: row.try_get("received_bytes")?,
            parts: row.try_get("parts")?,
            remote_state: row.try_get("remote_state")?,
            created_at: row.try_get("created_at")?,
            touched_at: row.try_get("touched_at")?,
            multipart_idle_timeout: row.try_get("multipart_idle_timeout")?,
            expires_at: match row.try_get("expires_at") {
                Ok(value) => value,
                Err(sqlx::Error::ColumnNotFound(_)) => Default::default(),
                Err(error) => return Err(error),
            },
            can_resume: match row.try_get("can_resume") {
                Ok(value) => value,
                Err(sqlx::Error::ColumnNotFound(_)) => Default::default(),
                Err(error) => return Err(error),
            },
        })
    }
}

impl<'r> sqlx::FromRow<'r, sqlx::postgres::PgRow> for crate::model::multipart::ReceivedPart {
    fn from_row(row: &'r sqlx::postgres::PgRow) -> sqlx::Result<Self> {
        use sqlx::Row;
        Ok(Self {
            number: row.try_get("number")?,
            size: row.try_get("size")?,
            etag: row.try_get("etag")?,
            sha256: row.try_get("sha256")?,
        })
    }
}

impl<'r> sqlx::FromRow<'r, sqlx::postgres::PgRow> for crate::model::settings::RuntimeSettingsRow {
    fn from_row(row: &'r sqlx::postgres::PgRow) -> sqlx::Result<Self> {
        use sqlx::Row;
        Ok(Self {
            value: row.try_get("value")?,
            revision: row.try_get("revision")?,
        })
    }
}

impl<'r> sqlx::FromRow<'r, sqlx::postgres::PgRow> for crate::model::task::TaskRecord {
    fn from_row(row: &'r sqlx::postgres::PgRow) -> sqlx::Result<Self> {
        use sqlx::Row;
        Ok(Self {
            task_type: row.try_get("task_type")?,
            state: row.try_get("state")?,
            bucket_id: row.try_get("bucket_id")?,
            cursor: row.try_get("cursor")?,
            detail: row.try_get("detail")?,
            execution_token: row.try_get("lease_token")?,
        })
    }
}

impl<'r> sqlx::FromRow<'r, sqlx::postgres::PgRow> for crate::storage::model::Record {
    fn from_row(row: &'r sqlx::postgres::PgRow) -> sqlx::Result<Self> {
        use sqlx::Row;
        Ok(Self {
            id: row.try_get("id")?,
            name: row.try_get("name")?,
            r#type: row.try_get("type")?,
            path: row.try_get("path")?,
            endpoint_url: row.try_get("endpoint_url")?,
            remote_bucket_name: row.try_get("remote_bucket_name")?,
            region: row.try_get("region")?,
            prefix: row.try_get("prefix")?,
            access_key_encrypted: row.try_get("access_key_encrypted")?,
            secret_key_encrypted: row.try_get("secret_key_encrypted")?,
            identity_marker: row.try_get("identity_marker")?,
            enabled: row.try_get("enabled")?,
        })
    }
}

pub(in super::super) fn lock_task_finish(
    task: Execution,
) -> Rows<'static, (String, String, Option<Uuid>)> {
    sqlx::query_as("SELECT task_type,state,parent_task_id FROM tasks WHERE id=$1 AND lease_token=$2 AND (state IN ('completed','paused') OR (state='running' AND ((lease_node_id IS NULL AND lease_expires_at IS NULL) OR lease_expires_at>clock_timestamp()))) FOR UPDATE").bind(task.id).bind(task.token)
}
