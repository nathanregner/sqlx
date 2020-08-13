use uuid::Uuid;

use crate::decode::Decode;
use crate::encode::{Encode, IsNull};
use crate::error::BoxDynError;
use crate::mssql::protocol::type_info::{DataType, TypeInfo};
use crate::mssql::{Mssql, MssqlTypeInfo, MssqlValueRef};
use crate::types::Type;
use std::convert::{TryFrom, TryInto};

impl Type<Mssql> for Uuid {
    fn type_info() -> MssqlTypeInfo {
        MssqlTypeInfo(TypeInfo::new(DataType::Guid, 16))
    }

    fn compatible(ty: &MssqlTypeInfo) -> bool {
        matches!(ty.0.ty, DataType::Guid)
    }
}

impl Encode<'_, Mssql> for Uuid {
    fn encode_by_ref(&self, buf: &mut Vec<u8>) -> IsNull {
        let mut bytes = *self.as_bytes();
        reorder_bytes(&mut bytes);
        buf.extend_from_slice(&bytes);
        IsNull::No
    }
}

impl Decode<'_, Mssql> for Uuid {
    fn decode(value: MssqlValueRef<'_>) -> Result<Self, BoxDynError> {
        let mut bytes = uuid::Bytes::try_from(value.as_bytes()?)?;
        reorder_bytes(&mut bytes);

        Ok(Uuid::from_bytes(bytes))
    }
}

/// UUIDs use network byte order (big endian) for the first 3 groups,
/// while GUIDs use native byte order (little endian).
///
/// https://github.com/microsoft/mssql-jdbc/blob/bec39dbba9544aef5f5f6a5495d5acf533efd6da/src/main/java/com/microsoft/sqlserver/jdbc/Util.java#L708-L730
pub(crate) fn reorder_bytes(bytes: &mut uuid::Bytes) {
    bytes.swap(0, 3);
    bytes.swap(1, 2);
    bytes.swap(4, 5);
    bytes.swap(6, 7);
}
