use tables::*;
use definitions::*;
use schema::*;
use std::mem::replace;

pub mod insert;

struct FullTableScan {
    table_name: String,
    table_iter: RowIdIterator
}
impl FullTableScan {
    fn get_next_row(&mut self, schema: &mut Schema) -> SqlError<Vec<LiteralValue>> {
        let row_id = match self.table_iter.next() {
                Some(id) => id,
                None => {
                    return Err(format!("No more rows."));
                }
            };
        let table_index = try!(schema.find_table_or_err(&self.table_name));
        schema.map_on_table(table_index, |table|
            table.get_row(row_id)
        )
    }

    fn new(table_name: String, schema: &mut Schema) -> SqlError<FullTableScan> {
        let table_index = try!(schema.find_table_or_err(&table_name));
        let iterator = try!(schema.map_on_table(table_index,|table| Ok(table.rowid_iter())));

        Ok(FullTableScan {
            table_name: table_name,
            table_iter: iterator
        })
    }
}

pub struct ResultSet {
    table_scan: FullTableScan
}
impl ResultSet {
    pub fn get_next_row(&mut self, schema: &mut Schema) -> SqlError<Vec<LiteralValue>> {
        self.table_scan.get_next_row(schema)
    }
}


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

pub fn build_select_plan(select: &SelectStmt, schema: &mut Schema) -> SqlError<ResultSet> {

    Ok(ResultSet {
        table_scan: try!(FullTableScan::new(
            match &select.from[0] {
                &SelectFromTable::NamedTable(ref tab) => { tab.clone() },
                _ => {panic!("Need table name")}
            }, schema))
        }
    )
}
