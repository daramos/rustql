use definitions::*;
use std::mem::replace;

pub mod insert_plan;
pub mod select_plan;


struct StaticRow {
    row: Option<Vec<LiteralValue>>
}
impl StaticRow {
    fn next(&mut self) -> Option<Vec<LiteralValue>> {
        if self.row.is_none() {
            return None;
        }
        let row = replace(&mut self.row,None).unwrap();
        Some(row)
    }
}
