use crate::Value;
use std::cmp::Ordering;

impl PartialOrd for Value {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        if self.get_id() != other.get_id() {
            panic!("Can't compare values with different Types!")
        }
        
        match (self, other) {
            (Value::Int(first), Value::Int(second)) => Some(first.cmp(second)),
            (Value::UInt(first), Value::UInt(second)) => Some(first.cmp(second)),
            (Value::RowID(first), Value::RowID(second)) => Some(first.cmp(second)),
            (Value::Varchar(first), Value::Varchar(second)) => Some(first.cmp(second)),
            _ => None
        }
    }
}

impl Ord for Value {
    fn cmp(&self, other: &Self) -> Ordering {
        match (self, other) {
            (Value::Int(first), Value::Int(second)) => first.cmp(second),
            (Value::UInt(first), Value::UInt(second)) => first.cmp(second),
            (Value::RowID(first), Value::RowID(second)) => first.cmp(second),
            (Value::Varchar(first), Value::Varchar(second)) => first.cmp(second),
            _ => panic!()
        }
    }
}
