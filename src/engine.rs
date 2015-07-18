use definitions::*;
use plan::*;
use schema::*;
use ir::*;
use std::iter::repeat;

pub struct SqlEngine {
    schema: Schema
}


impl SqlEngine {

    pub fn new() -> SqlEngine {
        SqlEngine {
            schema: Schema::new()
        }
    }

    pub fn excecute_stmt(&mut self,stmt: SqlStmt) -> SqlError<SqlResult> {
        match stmt {
            SqlStmt::Select(sel) => {
                Ok(SqlResult::Rows(try!(self.select(sel))))
            },
            SqlStmt::DumpTables => {
                println!("{:?}",self.schema);
                Ok(SqlResult::None)
            }
            SqlStmt::CreateTable(table) => {
                self.create_table(table);
                Ok(SqlResult::None)
            },
            SqlStmt::DropTable(table) => {
                self.drop_table(table);
                Ok(SqlResult::None)
            },
            SqlStmt::Insert(insert) => {
                try!(self.insert(insert));
                Ok(SqlResult::None)
            },
        }
    }

    fn select(&mut self,stmt: SelectStmt) -> SqlError<Vec<Vec<LiteralValue>>> {
        let mut plan = try!(build_select_plan(&stmt, &mut self.schema));
        let row = try!(plan.get_next_row(&mut self.schema));

        println!("{:?}",&row);
        Ok(vec!(row))
    }

    fn create_table(&mut self, stmt: CreateTableStmt) -> SqlError<()> {
        let table_index = try!(self.schema.create_table(&stmt.table_name));
        self.schema.map_on_table_mut(table_index, |table|
            {
                for col in stmt.column_defs.iter() {
                    table.add_column(col.clone());
                }
                Ok(())
            });
        println!("Table {} created",stmt.table_name);
        Ok(())
    }

    fn drop_table(&mut self,stmt: DropTableStmt) -> SqlError<()> {
        self.schema.drop_table(&stmt.table_name)
    }

    fn insert(&mut self,stmt: InsertStmt) -> SqlError<()> {
        let ir = try!(insert_from_stmt(&stmt, &self.schema));

        let table_col_len = try!(self.schema.map_on_table(ir.table.table_index, |table| {
            Ok(table.columns().len())
        }));

        let mut row: Vec<LiteralValue> = repeat(LiteralValue::Null).take(table_col_len).collect();


        let cur_index = 0;
        for val in ir.values.iter() {
            row[ir.columns[cur_index].column_index] = val.clone()
        }


        try!(self.schema.map_on_table_mut(ir.table.table_index, |table| {
            table.insert_row(row.clone())
        }));


        Ok(())
    }
}
