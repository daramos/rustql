//use definitions::*;
//use schema::*;
use super::{TableRef,ColumnRef};


pub struct SelectIr {
    pub columns: Vec<ColumnRef>,
    pub tables: Vec<TableRef>
}


// pub fn ir_from_select_stmt(stmt: &SelectStmt, schema: &Schema) -> SqlError<SelectIr> {
//     // Get all named tables
//     let mut table_names = Vec::new();
//     for table in stmt.from.iter() {
//         match table {
//             SelectFromTable::NamedTable(named_tab) => {
//                 table_names.append(named_tab.clone());
//             }
//         }
//     }
//     let table_refs = try!(resolve_table_references(&table_names[..],schema));
//     //let column_refs = try!(resolve_column_references(&stmt.column_names, &table_refs[..], schema));
//     Err("tmp".to_string())
//
// }
