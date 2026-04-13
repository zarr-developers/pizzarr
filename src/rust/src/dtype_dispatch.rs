//! Map zarrs data types to R-compatible type families.
//!
//! zarrs 0.23+ uses [`DataType`] as a newtype wrapping
//! `Arc<dyn DataTypeExtension>`. Type identity is checked via
//! `PartialEq` against factory functions like [`data_type::float64()`].

use zarrs::array::data_type;
use zarrs::array::DataType;

/// R-compatible type family for a zarrs data type.
///
/// Used by `zarrs_open_array_metadata` to report the R type, and by
/// `zarrs_get_subset` to dispatch retrieval.
#[derive(Debug, Clone, Copy)]
pub(crate) enum RTypeFamily {
    /// R double (REALSXP), stored as f64 — zero-cost.
    Double,
    /// R double (REALSXP), stored as f32 — element-wise widening.
    Float32AsDouble,
    /// R integer (INTSXP), stored as i32 — zero-cost.
    Integer,
    /// R integer (INTSXP), stored as i16 — element-wise widening.
    Int16AsInteger,
    /// R integer (INTSXP), stored as i8 — element-wise widening.
    Int8AsInteger,
    /// R integer (INTSXP), stored as u8 — element-wise widening.
    Uint8AsInteger,
    /// R integer (INTSXP), stored as u16 — element-wise widening.
    Uint16AsInteger,
    /// R double (REALSXP), stored as u32 — element-wise widening.
    Uint32AsDouble,
    /// R double (REALSXP), stored as i64 — precision risk > 2^53.
    Int64AsDouble,
    /// R double (REALSXP), stored as u64 — precision risk > 2^53.
    Uint64AsDouble,
    /// R logical (LGLSXP), stored as bool.
    Logical,
}

impl RTypeFamily {
    /// The R type name for metadata reporting.
    #[must_use]
    pub(crate) fn r_type_name(self) -> &'static str {
        match self {
            Self::Double
            | Self::Float32AsDouble
            | Self::Uint32AsDouble
            | Self::Int64AsDouble
            | Self::Uint64AsDouble => "double",
            Self::Integer
            | Self::Int16AsInteger
            | Self::Int8AsInteger
            | Self::Uint8AsInteger
            | Self::Uint16AsInteger => "integer",
            Self::Logical => "logical",
        }
    }
}

/// Classify a zarrs [`DataType`] into an R-compatible type family.
///
/// Returns `None` for types not supported by the zarrs bridge
/// (strings, complex, etc.). Callers should fall back to the
/// R-native path.
pub(crate) fn dtype_family(dt: &DataType) -> Option<RTypeFamily> {
    if *dt == data_type::float64() {
        return Some(RTypeFamily::Double);
    }
    if *dt == data_type::float32() {
        return Some(RTypeFamily::Float32AsDouble);
    }
    if *dt == data_type::int32() {
        return Some(RTypeFamily::Integer);
    }
    if *dt == data_type::int16() {
        return Some(RTypeFamily::Int16AsInteger);
    }
    if *dt == data_type::int8() {
        return Some(RTypeFamily::Int8AsInteger);
    }
    if *dt == data_type::uint8() {
        return Some(RTypeFamily::Uint8AsInteger);
    }
    if *dt == data_type::uint16() {
        return Some(RTypeFamily::Uint16AsInteger);
    }
    if *dt == data_type::uint32() {
        return Some(RTypeFamily::Uint32AsDouble);
    }
    if *dt == data_type::int64() {
        return Some(RTypeFamily::Int64AsDouble);
    }
    if *dt == data_type::uint64() {
        return Some(RTypeFamily::Uint64AsDouble);
    }
    if *dt == data_type::bool() {
        return Some(RTypeFamily::Logical);
    }
    None
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn classify_float64() {
        let family = dtype_family(&data_type::float64()).unwrap();
        assert_eq!(family.r_type_name(), "double");
    }

    #[test]
    fn classify_int32() {
        let family = dtype_family(&data_type::int32()).unwrap();
        assert_eq!(family.r_type_name(), "integer");
    }

    #[test]
    fn classify_bool() {
        let family = dtype_family(&data_type::bool()).unwrap();
        assert_eq!(family.r_type_name(), "logical");
    }

    #[test]
    fn unsupported_returns_none() {
        // String type is not supported by the bridge.
        assert!(dtype_family(&data_type::string()).is_none());
    }
}
