use definitions::*;
use schema::*;
use super::{TableRef,ColumnRef,resolve_table_reference,resolve_column_references,resolve_column_wildcard};


pub struct SelectIr {
    pub columns: Vec<ColumnRef>,
    pub tables: Vec<TableRef>
}

fn resolve_projection_columns(stmt: &SelectStmt, table_refs: &Vec<TableRef>, schema: &Schema) -> SqlError<Vec<ColumnRef>> {
    let mut column_names = Vec::new();
    for column in stmt.projection.iter() {
        match column {
            &SelectProjectionColumn::Named(ref column_name) => {
                column_names.push(column_name.clone());
            },
            &SelectProjectionColumn::LiteralValue(_) => {
                return Err("Select LiteralValue not implemented".to_string());
            },
            &SelectProjectionColumn::Wildcard => {
                if table_refs.len() != 1 {
                    return Err("Select wildcard not supported for multiple tables".to_string());
                }
                // TODO: A wildcard column should not cause all the other projection columns to be ignored
                return resolve_column_wildcard(&table_refs[0],0,schema);
            }
        }
    }
    resolve_column_references(&column_names, &table_refs[..], schema)
}

pub fn ir_from_select_stmt(stmt: &SelectStmt, schema: &Schema) -> SqlError<SelectIr> {
    // Get all named tables
    let mut table_refs = Vec::new();
    for table in stmt.from.iter() {
        match table {
            &SelectFromTable::NamedTable(ref table_name) => {
                table_refs.push(try!(resolve_table_reference(table_name,schema)));
            },
            &SelectFromTable::Function(_) => {
                return Err("Select from function not implemented".to_string());
            }
        }
    }

    let column_refs = try!(resolve_projection_columns(stmt, &table_refs, schema));


    Ok(SelectIr {
        columns: column_refs,
        tables: table_refs
    })
}
