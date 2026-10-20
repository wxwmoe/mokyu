use serde::{Deserialize, Serialize};
use sqlx::{Database, Decode, Encode, Type, error::BoxDynError};
use std::{fmt, str::FromStr};
use uuid::Uuid;

/// Database-native identity; byte offsets and encryption counters remain integers.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub(crate) enum Id {
    Integer(i64),
    Uuid(Uuid),
}

impl Default for Id {
    fn default() -> Self {
        Self::Integer(0)
    }
}

impl Serialize for Id {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.collect_str(self)
    }
}

impl<'de> Deserialize<'de> for Id {
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        #[derive(Deserialize)]
        #[serde(untagged)]
        enum Value {
            Text(String),
            Integer(i64),
        }
        match Value::deserialize(deserializer)? {
            Value::Text(text) => text.parse().map_err(serde::de::Error::custom),
            Value::Integer(value) if value >= 0 => Ok(Self::Integer(value)),
            _ => Err(serde::de::Error::custom("invalid database identity")),
        }
    }
}

impl utoipa::PartialSchema for Id {
    fn schema() -> utoipa::openapi::RefOr<utoipa::openapi::schema::Schema> {
        <String as utoipa::PartialSchema>::schema()
    }
}
impl utoipa::ToSchema for Id {}

impl Id {
    pub fn parse(text: &str, backend: crate::config::DatabaseType) -> anyhow::Result<Self> {
        let id: Self = text.parse().map_err(|_| s3s::s3_error!(InvalidArgument))?;
        id.validate(backend)?;
        Ok(id)
    }

    pub fn validate(self, backend: crate::config::DatabaseType) -> anyhow::Result<()> {
        let compatible = match backend {
            crate::config::DatabaseType::Postgres => self.uuid().is_ok(),
            _ => self.integer().is_ok(),
        };
        anyhow::ensure!(compatible, s3s::s3_error!(InvalidArgument));
        Ok(())
    }

    pub fn from_json(value: &serde_json::Value) -> anyhow::Result<Option<Self>> {
        if value.is_null() {
            return Ok(None);
        }
        if let Some(text) = value.as_str() {
            return Ok(Some(text.parse()?));
        }
        if let Some(number) = value.as_i64() {
            anyhow::ensure!(number >= 0, "invalid database identity");
            return Ok(Some(Self::Integer(number)));
        }
        anyhow::bail!("invalid database identity")
    }

    pub fn is_valid(self) -> bool {
        match self {
            Self::Integer(id) => id > 0,
            Self::Uuid(id) => !id.is_nil(),
        }
    }

    fn integer(self) -> Result<i64, BoxDynError> {
        match self {
            Self::Integer(id) => Ok(id),
            Self::Uuid(id) if id.is_nil() => Ok(0),
            _ => Err("UUID identity cannot be used with an integer database".into()),
        }
    }

    fn uuid(self) -> Result<Uuid, BoxDynError> {
        match self {
            Self::Uuid(id) => Ok(id),
            Self::Integer(0) => Ok(Uuid::nil()),
            _ => Err("integer identity cannot be used with a UUID database".into()),
        }
    }
}

impl fmt::Display for Id {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Integer(id) => id.fmt(f),
            Self::Uuid(id) => id.fmt(f),
        }
    }
}

impl FromStr for Id {
    type Err = anyhow::Error;
    fn from_str(text: &str) -> Result<Self, Self::Err> {
        if let Ok(id) = text.parse::<i64>() {
            anyhow::ensure!(id >= 0, "invalid database identity");
            return Ok(Self::Integer(id));
        }
        Ok(Self::Uuid(text.parse()?))
    }
}

impl From<i64> for Id {
    fn from(id: i64) -> Self {
        Self::Integer(id)
    }
}
impl TryFrom<u64> for Id {
    type Error = std::num::TryFromIntError;
    fn try_from(id: u64) -> Result<Self, Self::Error> {
        Ok(Self::Integer(i64::try_from(id)?))
    }
}
impl From<Uuid> for Id {
    fn from(id: Uuid) -> Self {
        Self::Uuid(id)
    }
}

macro_rules! native {
    ($database:ty, $native:ty, $convert:ident) => {
        impl Type<$database> for Id {
            fn type_info() -> <$database as Database>::TypeInfo {
                <$native as Type<$database>>::type_info()
            }
            fn compatible(info: &<$database as Database>::TypeInfo) -> bool {
                <$native as Type<$database>>::compatible(info)
            }
        }
        impl<'q> Encode<'q, $database> for Id {
            fn encode_by_ref(
                &self,
                buffer: &mut <$database as Database>::ArgumentBuffer,
            ) -> Result<sqlx::encode::IsNull, BoxDynError> {
                <$native as Encode<$database>>::encode(self.$convert()?, buffer)
            }
        }
        impl<'r> Decode<'r, $database> for Id {
            fn decode(value: <$database as Database>::ValueRef<'r>) -> Result<Self, BoxDynError> {
                Ok(<$native as Decode<$database>>::decode(value)?.into())
            }
        }
    };
}

native!(sqlx::Postgres, Uuid, uuid);
native!(sqlx::MySql, i64, integer);
native!(sqlx::Sqlite, i64, integer);

impl sqlx::postgres::PgHasArrayType for Id {
    fn array_type_info() -> sqlx::postgres::PgTypeInfo {
        <Uuid as sqlx::postgres::PgHasArrayType>::array_type_info()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn identity_json_preserves_full_integer_precision() {
        for id in [
            Id::from(9_007_199_254_740_993),
            Id::from(i64::MAX),
            Uuid::now_v7().into(),
        ] {
            let encoded = serde_json::to_value(id).unwrap();
            assert_eq!(encoded.as_str(), Some(id.to_string().as_str()));
            assert_eq!(serde_json::from_value::<Id>(encoded).unwrap(), id);
        }
        assert_eq!(serde_json::from_str::<Id>("42").unwrap(), Id::from(42));
        for invalid in [
            "-1",
            "1.5",
            "18446744073709551615",
            "null",
            "true",
            "\"invalid\"",
        ] {
            assert!(serde_json::from_str::<Id>(invalid).is_err(), "{invalid}");
        }
    }

    #[test]
    fn native_identities_reject_the_other_database_type() {
        let uuid = Uuid::now_v7();
        assert_eq!(uuid.to_string().parse::<Id>().unwrap(), Id::Uuid(uuid));
        assert_eq!("42".parse::<Id>().unwrap(), Id::Integer(42));
        assert!(Id::Integer(42).uuid().is_err());
        assert!(Id::Uuid(uuid).integer().is_err());
        assert_eq!(Id::default().uuid().unwrap(), Uuid::nil());
        assert_eq!(Id::default().integer().unwrap(), 0);
        assert!("-1".parse::<Id>().is_err());
        assert!(!Id::default().is_valid());
        use crate::config::DatabaseType::{Mysql, Postgres, Sqlite};
        for backend in [Postgres, Mysql, Sqlite] {
            assert!(Id::parse("0", backend).is_ok());
            assert!(Id::parse("-1", backend).is_err());
            assert!(Id::parse("invalid", backend).is_err());
            assert_eq!(Id::parse("42", backend).is_ok(), backend != Postgres);
            assert_eq!(
                Id::parse(&uuid.to_string(), backend).is_ok(),
                backend == Postgres
            );
        }
        assert!(Id::from_json(&serde_json::json!(-1)).is_err());
    }
}
