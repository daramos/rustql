use definitions::*;
use schema::*;
use std::iter::repeat;
use super::StaticRow;
use ir::*;

pub struct InsertPlan {
    table_index: usize,
    source: StaticRow
}
impl InsertPlan {
    pub fn run(&mut self, schema: &mut Schema) -> SqlError<usize> {
        let mut count = 0usize;
        loop {
            match self.source.next() {
                Some(row) => {
                    let _ = try!(schema.map_on_table_mut(self.table_index, |table| {
                        Ok(table.insert_row(row.to_owned()))
                        }));
                },
                None => {
                    break;
                }
            }
            count += 1;
        }
        Ok(count)
    }
}

pub fn build_insert_plan(insert_ir: &InsertIr, schema: &Schema) -> SqlError<InsertPlan> {

    // First convert what might potentially be a partial row into a full one by
    // making all the absent columns NULLs
    let table_col_len = try!(schema.map_on_table(insert_ir.table.table_index, |table| {
        Ok(table.columns().len())
    }));
    let mut row: Vec<LiteralValue> = repeat(LiteralValue::Null).take(table_col_len).collect();
    let mut cur_index = 0;

    for val in insert_ir.values.iter() {
        row[insert_ir.columns[cur_index].column_index] = val.clone();
        cur_index += 1;
    }

    let static_row = StaticRow {
        row: Some(row)
    };

    Ok(InsertPlan {
        table_index: insert_ir.table.table_index,
        source: static_row
    })

}
