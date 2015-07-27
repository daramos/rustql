use definitions::*;
use schema::*;
use ir::select_ir::*;
use tables::*;

struct FullTableScan {
    table_index: usize,
    column_ids: Vec<usize>,
    table_iter: RowIdIterator
}
impl FullTableScan {
    fn get_next_row(&mut self, schema: &Schema) -> SqlError<Vec<LiteralValue>> {
        let row_id = match self.table_iter.next() {
                Some(id) => id,
                None => {
                    return Err(format!("No more rows."));
                }
            };
        schema.map_on_table(self.table_index, |table| {
            let row = try!(table.get_row(row_id));
            let mut filtered_row = Vec::new();
            for (index,column_val) in row.iter().enumerate() {
                match self.column_ids.iter().position(|&col_id| col_id == index) {
                    Some(_) => {
                        filtered_row.push(column_val.clone());
                    },
                    None => {}
                }
            }
            Ok(filtered_row)
        })
    }

    fn new(table_index: usize, column_ids: Vec<usize>, schema: &Schema) -> SqlError<FullTableScan> {
        let iterator = try!(schema.map_on_table(table_index,|table| Ok(table.rowid_iter())));

        Ok(FullTableScan {
            table_index: table_index,
            table_iter: iterator,
            column_ids: column_ids
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


pub fn build_select_plan(ir: &SelectIr, schema: &Schema) -> SqlError<ResultSet> {
    let mut table_column_ids = Vec::new();
    for column_ref in ir.columns.iter() {
        if column_ref.table_ref_index == 0 {
            table_column_ids.push(column_ref.column_index);
        }
    }
    Ok(ResultSet {
        table_scan: try!(FullTableScan::new(ir.tables[0].table_index, table_column_ids, schema))
        }
    )
}
