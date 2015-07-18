use std::vec::Vec;
use definitions::*;
use std::iter::repeat;
use bit_vec::BitVec;

pub type RowId = usize;

#[derive(Clone,PartialEq,Debug)]
pub struct MemoryTable {
    column_defs: Vec<ColumnDefinition>,
    columns_data: Vec<Vec<LiteralValue>>,
    rows_status: BitVec
}

impl MemoryTable {
    pub fn new() -> MemoryTable {
        MemoryTable {
            column_defs: Vec::new(),
            columns_data: Vec::new(),
            rows_status: BitVec::new()
        }
    }
    
    pub fn columns(&self) -> &[ColumnDefinition] {
        &self.column_defs[..]
    }
    
    pub fn raw_len(&self) -> usize {
        self.rows_status.len()
    }
    
    pub fn len(&self) -> usize {
        self.rows_status.iter().filter(|x| *x).count()
    }
    
    pub fn add_column(&mut self, column_def: ColumnDefinition) {
        self.column_defs.push(column_def);
        
        // Add new column, fill with Null for all existing rows
        let nulls = repeat(LiteralValue::Null).take(self.raw_len()).collect();
        self.columns_data.push(nulls);
    }
    
    pub fn insert_row(&mut self, mut row: Vec<LiteralValue>) -> SqlError<RowId> {
        let num_columns = row.len();
        if num_columns != self.column_defs.len() {
            return Err("Wrong number of columns for table".to_string());
        }
        
        for (dst, col) in self.columns_data.iter_mut().zip(row.drain(..)) {
            dst.push(col)
        }
        self.rows_status.push(true);
        
        return Ok(self.raw_len()-1);
    }
    
    pub fn get_row(&self, rowid: RowId) -> SqlError<Vec<LiteralValue>> {
        match self.rows_status.get(rowid) {
            None => { Err(format!("Row {} doesn't exist", rowid)) },
            Some(false) => { Err(format!("Row {} is no longer valid", rowid)) },
            Some(true) => {
                let mut row = Vec::new();
                for col in self.columns_data.iter() {
                    row.push(col[rowid].clone());
                }
                Ok(row)
            }
        }
    }
    
    pub fn rowid_iter(&self) -> RowIdIterator {
        RowIdIterator {
            bit_vec: self.rows_status.clone(),
            next_rowid: 0
        }
    }
    
    pub fn get_row_col(&self, rowid: RowId, column_id: usize) -> Option<&LiteralValue> {
        self.columns_data.get(column_id).and_then(|x| x.get(rowid))
    }
}

pub struct RowIdIterator {
    bit_vec: BitVec,
    next_rowid: usize
}

impl Iterator for RowIdIterator {
    type Item = RowId;

    fn next(&mut self) -> Option<RowId> {
        let rowid = self.next_rowid;
        loop {
            match self.bit_vec.get(rowid) {
                Some(true) => {
                    self.next_rowid += 1;
                    return Some(rowid);
                },
                Some(false) => {
                    continue;
                },
                None => {
                    return None
                }
            }
        }
    }
}
#[cfg(test)]
mod tests {
    use definitions::*;
    use super::*;
    fn create_table() -> MemoryTable {
        let mut column_defs = vec![
            ColumnDefinition {
                ctype: ColumnType::Text,
                name: "test_column".to_string()
            }
        ];
        
        let mut table = MemoryTable::new();
        for col in column_defs.drain(..) {
            table.add_column(col);
        }
        table
    }
    
    
    #[test]
    fn memory_table() {
        let mut mt = create_table();
        let mut row_vec = vec![LiteralValue::Text("Test".to_string())];
        let row_id = mt.insert_row(row_vec.clone()).unwrap();
        let mut row_ret = mt.get_row(row_id);
        assert_eq!(Ok(row_vec.clone()),row_ret);
        
        mt.add_column( 
            ColumnDefinition {
                ctype: ColumnType::Text,
                name: "test_column2".to_string()
            }
        );
        row_vec.push(LiteralValue::Null);
        row_ret = mt.get_row(row_id);
        
        assert_eq!(Ok(row_vec.clone()),row_ret);
    }
}
