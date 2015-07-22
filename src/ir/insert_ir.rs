use definitions::*;
use schema::*;
use super::{TableRef,ColumnRef,resolve_table_reference,resolve_column_references};

pub struct InsertIr {
    pub values: Vec<LiteralValue>,
    pub table: TableRef,
    pub columns: Vec<ColumnRef>
}

pub fn ir_from_insert_stmt(stmt: &InsertStmt, schema: &Schema) -> SqlError<InsertIr> {
    // First lets ensure that the columns listed are not duplicated
    let mut col_names_deduped = stmt.column_names.clone();
    col_names_deduped.sort_by(|a,b| a.cmp(b));
    col_names_deduped.dedup();
    if stmt.column_names.len() != col_names_deduped.len() {
        return Err("Duplicated column names in insert statement".to_string());
    }

    // Ensure the number of listed columns match the number of supplied values
    if stmt.column_values.len() < stmt.column_names.len() {
        return Err("Not enough values".to_string());
    }
    else if stmt.column_values.len() > stmt.column_names.len() {
        return Err("Too many values".to_string());
    }

    // Now lets resolve the table and column references
    let table_ref = try!(resolve_table_reference(&stmt.table_name,schema));
    let column_refs = try!(resolve_column_references(&stmt.column_names, &vec![table_ref.clone()], schema));

    Ok(InsertIr {
        columns: column_refs,
        table: table_ref,
        values: stmt.column_values.clone()
    })
}
