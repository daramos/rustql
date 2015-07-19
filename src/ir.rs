use definitions::*;
use schema::*;

pub struct TableRef {
    pub table_index: usize
}

pub struct ColumnRef {
    pub column_index: usize,
    pub table_ref_index: usize
}


pub struct SelectIr {
    pub columns: Vec<ColumnRef>,
    pub tables: Vec<TableRef>
}

pub struct InsertIr {
    pub values: Vec<LiteralValue>,
    pub table: TableRef,
    pub columns: Vec<ColumnRef>
}

fn find_table_references(table_names: &[String], schema: &Schema) -> SqlError<Vec<TableRef>> {
    let mut refs = Vec::new();

    for name in table_names.iter() {
        refs.push(TableRef {
            table_index: try!(schema.find_table_or_err(name))
        });
    }
    Ok(refs)
}

fn find_column_references(column_names: &[String], table_refs: &[TableRef], schema: &Schema) -> SqlError<Vec<ColumnRef>> {
    #[derive(Clone)]
    struct ColumnTableMappings {
        table_ref_index: usize,
        index: usize,
        name: String
    };


    // First lets build a vector of all the available columns
    let mut all_columns = Vec::new();
    for (table_ref_index,table_ref) in table_refs.iter().enumerate() {
        let table_columns =
            try!(schema.map_on_table(table_ref.table_index, |table| {
                Ok(table.columns().to_owned())
            }));
        for (column_index, ref column_def) in table_columns.iter().enumerate() {
            all_columns.push(ColumnTableMappings {
                table_ref_index: table_ref_index,
                index: column_index,
                name: column_def.name.to_owned()
            });
        }

    }

    // Now lets try to match against them
    let mut column_refs = Vec::new();
    for column_name in column_names.iter() {
        let mut all_columns_iter = all_columns.iter();
        match all_columns_iter.find(|&c| {c.name == *column_name}) {
            Some(matched_col) => {
                match all_columns_iter.find(|&c| {c.name == *column_name}) {
                    Some(_) => {
                        return Err(format!("Ambiguous column {}", column_name));
                    },
                    None => {
                        column_refs.push(ColumnRef{
                            column_index: matched_col.index,
                            table_ref_index: matched_col.table_ref_index
                            });
                    }
                }
            },
            None => {
                return Err(format!("Column {} not found", column_name));
            }
        }
    }

    Ok(column_refs)
}


// pub fn select_from_ast(ast: SelectStmt) -> SqlResult<SelectIr> {
//
// }
//
pub fn ir_from_insert_stmt(stmt: &InsertStmt, schema: &Schema) -> SqlError<InsertIr> {
    let table_refs = try!(find_table_references(&vec![stmt.table_name.to_owned()][..],schema));
    let column_refs = try!(find_column_references(&stmt.column_names, &table_refs[..], schema));
    Ok(InsertIr {
        columns: column_refs,
        table: try!(table_refs.into_iter().next().ok_or("Internal Error: insert_from_stmt".to_string())),
        values: stmt.column_values.clone()
    })
}
