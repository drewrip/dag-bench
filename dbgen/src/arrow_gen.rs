//! Columnar (Arrow) row-batch conversion for the parallel table generators.
//!
//! DuckDB's row-oriented `Appender::append_row` issues one FFI call per column per row,
//! which dominates runtime once tables reach millions of rows. `append_record_batch` instead
//! hands DuckDB a whole vectorized chunk at once. `Col`/`RowBatch` let each generator keep
//! writing plain `Fn(usize) -> (T1, T2, ...)` closures — the tuple is transposed into Arrow
//! arrays right before the (infrequent, chunk-sized) append call.

use chrono::{NaiveDate, NaiveDateTime};
use duckdb::arrow::array::{
    ArrayRef, BooleanArray, Date32Array, Float64Array, Int16Array, Int32Array, Int64Array,
    Int8Array, StringArray, TimestampMicrosecondArray,
};
use duckdb::arrow::datatypes::{DataType, Field, Schema};
use std::sync::Arc;

const EPOCH: NaiveDate = match NaiveDate::from_ymd_opt(1970, 1, 1) {
    Some(d) => d,
    None => unreachable!(),
};

/// A single column's value type: knows its Arrow representation and how to build a full
/// column array from a `Vec` of itself.
pub trait Col {
    fn arrow_type() -> DataType;
    fn to_array(vals: Vec<Self>) -> ArrayRef
    where
        Self: Sized;
}

macro_rules! impl_col_primitive {
    ($t:ty, $arrow_ty:expr, $arr:ty) => {
        impl Col for $t {
            fn arrow_type() -> DataType {
                $arrow_ty
            }
            fn to_array(vals: Vec<$t>) -> ArrayRef {
                Arc::new(<$arr>::from(vals))
            }
        }
    };
}

impl_col_primitive!(i8, DataType::Int8, Int8Array);
impl_col_primitive!(i16, DataType::Int16, Int16Array);
impl_col_primitive!(i32, DataType::Int32, Int32Array);
impl_col_primitive!(i64, DataType::Int64, Int64Array);
impl_col_primitive!(f64, DataType::Float64, Float64Array);
impl_col_primitive!(bool, DataType::Boolean, BooleanArray);
impl_col_primitive!(String, DataType::Utf8, StringArray);

impl Col for Option<i32> {
    fn arrow_type() -> DataType {
        DataType::Int32
    }
    fn to_array(vals: Vec<Option<i32>>) -> ArrayRef {
        Arc::new(Int32Array::from(vals))
    }
}

impl Col for Option<i64> {
    fn arrow_type() -> DataType {
        DataType::Int64
    }
    fn to_array(vals: Vec<Option<i64>>) -> ArrayRef {
        Arc::new(Int64Array::from(vals))
    }
}

impl<'a> Col for &'a str {
    fn arrow_type() -> DataType {
        DataType::Utf8
    }
    fn to_array(vals: Vec<&'a str>) -> ArrayRef {
        Arc::new(StringArray::from(vals))
    }
}

impl<'a> Col for Option<&'a str> {
    fn arrow_type() -> DataType {
        DataType::Utf8
    }
    fn to_array(vals: Vec<Option<&'a str>>) -> ArrayRef {
        Arc::new(StringArray::from(vals))
    }
}

impl Col for NaiveDate {
    fn arrow_type() -> DataType {
        DataType::Date32
    }
    fn to_array(vals: Vec<NaiveDate>) -> ArrayRef {
        let days: Vec<i32> = vals
            .into_iter()
            .map(|d| (d - EPOCH).num_days() as i32)
            .collect();
        Arc::new(Date32Array::from(days))
    }
}

impl Col for NaiveDateTime {
    fn arrow_type() -> DataType {
        DataType::Timestamp(duckdb::arrow::datatypes::TimeUnit::Microsecond, None)
    }
    fn to_array(vals: Vec<NaiveDateTime>) -> ArrayRef {
        let micros: Vec<i64> = vals
            .into_iter()
            .map(|d| d.and_utc().timestamp_micros())
            .collect();
        Arc::new(TimestampMicrosecondArray::from(micros))
    }
}

/// A full table row: knows its Arrow schema and how to transpose a batch of rows into
/// column arrays.
pub trait RowBatch {
    fn schema() -> Schema;
    fn to_columns(rows: Vec<Self>) -> Vec<ArrayRef>
    where
        Self: Sized;
}

macro_rules! impl_row_batch {
    ($($t:ident $v:ident $i:tt),+) => {
        impl<$($t: Col + Send),+> RowBatch for ($($t,)+) {
            fn schema() -> Schema {
                Schema::new(vec![$(Field::new(stringify!($v), $t::arrow_type(), true)),+])
            }

            fn to_columns(rows: Vec<Self>) -> Vec<ArrayRef> {
                $(let mut $v: Vec<$t> = Vec::with_capacity(rows.len());)+
                for row in rows {
                    $($v.push(row.$i);)+
                }
                vec![$($t::to_array($v)),+]
            }
        }
    };
}

impl_row_batch!(T0 c0 0, T1 c1 1);
impl_row_batch!(T0 c0 0, T1 c1 1, T2 c2 2);
impl_row_batch!(T0 c0 0, T1 c1 1, T2 c2 2, T3 c3 3);
impl_row_batch!(T0 c0 0, T1 c1 1, T2 c2 2, T3 c3 3, T4 c4 4);
impl_row_batch!(T0 c0 0, T1 c1 1, T2 c2 2, T3 c3 3, T4 c4 4, T5 c5 5);
impl_row_batch!(T0 c0 0, T1 c1 1, T2 c2 2, T3 c3 3, T4 c4 4, T5 c5 5, T6 c6 6);
impl_row_batch!(T0 c0 0, T1 c1 1, T2 c2 2, T3 c3 3, T4 c4 4, T5 c5 5, T6 c6 6, T7 c7 7);
impl_row_batch!(T0 c0 0, T1 c1 1, T2 c2 2, T3 c3 3, T4 c4 4, T5 c5 5, T6 c6 6, T7 c7 7, T8 c8 8);
impl_row_batch!(T0 c0 0, T1 c1 1, T2 c2 2, T3 c3 3, T4 c4 4, T5 c5 5, T6 c6 6, T7 c7 7, T8 c8 8, T9 c9 9);
