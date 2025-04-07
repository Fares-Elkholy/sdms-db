// This file will be replaced by the runner
// Author: Jonathan Schild

//! # Scalable Datamanagement Systems - Lab 3

use std::fmt::{Debug, Display};
use std::io;
use std::rc::Rc;
use thiserror::Error;

pub mod engine;
pub mod iceberg;
pub mod storage;
pub mod value_cmp;

#[derive(Error, Debug)]
pub enum DatabaseError {
    #[error("optimistic update failed, unknown changes occurred")]
    OptimisticFail,
    #[error("IOError")]
    IOError(#[from] io::Error),
    #[error("type mismatch")]
    TypeMismatch(Value),
    #[error("invalid utf-8")]
    InvalidUTF8,
    #[error("an unknown error occurred!")]
    Unknown,
    #[error("Error in opening or closing table for changes")]
    EngineError,
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Copy)]
pub struct RowID(pub u64);

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Copy)]
pub enum TypeID {
    Int = 0,
    UInt,
    RowID,
    Varchar,
}

impl TryFrom<u64> for TypeID {
    type Error = DatabaseError;

    fn try_from(value: u64) -> Result<Self, Self::Error> {
        match value {
            0 => Ok(TypeID::Int),
            1 => Ok(TypeID::UInt),
            2 => Ok(TypeID::RowID),
            3 => Ok(TypeID::Varchar),
            _ => Err(DatabaseError::Unknown),
        }
    }
}

impl Into<u64> for TypeID {
    fn into(self) -> u64 {
        self as u64
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum Value {
    Int(i32),
    UInt(u32),
    RowID(RowID),
    Varchar(Rc<String>),
}

impl Value {
    pub fn get_id(&self) -> TypeID {
        match self {
            Value::Int(_) => TypeID::Int,
            Value::UInt(_) => TypeID::UInt,
            Value::RowID(_) => TypeID::RowID,
            Value::Varchar(_) => TypeID::Varchar,
        }
    }
}

impl Display for Value {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Value::Int(num) => write!(f, "{}", num),
            Value::UInt(num) => write!(f, "{}", num),
            Value::RowID(RowID(num)) => write!(f, "{}", num),
            Value::Varchar(st) => write!(f, "{}", st),
        }
    }
}

impl TryInto<i32> for Value {
    type Error = DatabaseError;

    fn try_into(self) -> Result<i32, Self::Error> {
        match self {
            Value::Int(i) => Ok(i),
            _ => Err(DatabaseError::TypeMismatch(self)),
        }
    }
}

impl TryInto<u32> for Value {
    type Error = DatabaseError;

    fn try_into(self) -> Result<u32, Self::Error> {
        match self {
            Value::UInt(i) => Ok(i),
            _ => Err(DatabaseError::TypeMismatch(self)),
        }
    }
}

impl TryInto<RowID> for Value {
    type Error = DatabaseError;

    fn try_into(self) -> Result<RowID, Self::Error> {
        match self {
            Value::RowID(i) => Ok(i),
            _ => Err(DatabaseError::TypeMismatch(self)),
        }
    }
}

impl TryInto<String> for Value {
    type Error = DatabaseError;

    fn try_into(self) -> Result<String, Self::Error> {
        match self {
            Value::Varchar(v) => Ok(String::from(v.as_ref())),
            _ => Err(DatabaseError::TypeMismatch(self)),
        }
    }
}

impl From<String> for Value {
    fn from(value: String) -> Self {
        Value::Varchar(Rc::new(value))
    }
}

impl From<&str> for Value {
    fn from(value: &str) -> Self {
        Value::Varchar(Rc::new(value.to_string()))
    }
}

impl From<Rc<String>> for Value {
    fn from(value: Rc<String>) -> Self {
        Value::Varchar(value)
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct Record {
    record: Vec<Value>,
}

impl Record {
    pub fn new(values: Vec<Value>) -> Self {
        Record { record: values }
    }

    pub fn len(&self) -> usize {
        self.record.len()
    }

    pub fn is_empty(&self) -> bool {
        self.record.is_empty()
    }

    pub fn get(&self, idx: usize) -> Option<&Value> {
        self.record.get(idx)
    }

    pub fn get_mut(&mut self, idx: usize) -> Option<&mut Value> {
        self.record.get_mut(idx)
    }
}

impl From<Vec<Value>> for Record {
    fn from(value: Vec<Value>) -> Self {
        Record { record: value }
    }
}

impl From<Record> for Vec<Value> {
    fn from(value: Record) -> Self {
        value.record
    }
}

// Volcano operator trait
pub trait Operator {
    // Initialise child operators and itself.
    fn open(&mut self);
    // Get next chunk. Returns [None] if end of data stream is reached.
    fn next(&mut self) -> Option<TableChunk>;
    // Cleans up child amd itself.
    fn close(&mut self);
}

// A chunk of a table, in column layout.
pub type TableChunk = Vec<Vec<Value>>; // horizontal fragment of org table

// A table schema
pub type Schema = Vec<TypeID>;
