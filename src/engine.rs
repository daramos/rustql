use definitions::*;
use plan::*;
use schema::*;
use ir::*;


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
                try!(self.create_table(table));
                Ok(SqlResult::None)
            },
            SqlStmt::DropTable(table) => {
                try!(self.drop_table(table));
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
        try!(self.schema.map_on_table_mut(table_index, |table|
            {
                for col in stmt.column_defs.iter() {
                    table.add_column(col.clone());
                }
                Ok(())
            }));
        println!("Table {} created",stmt.table_name);
        Ok(())
    }

    fn drop_table(&mut self,stmt: DropTableStmt) -> SqlError<()> {
        let table_index = try!(self.schema.find_table_or_err(&stmt.table_name));
        self.schema.drop_table(table_index)
    }

    fn insert(&mut self,stmt: InsertStmt) -> SqlError<()> {
        let ir = try!(insert_ir::ir_from_insert_stmt(&stmt, &self.schema));
        let mut plan = try!(insert_plan::build_insert_plan(&ir, &self.schema));
        let _ = try!(plan.run(&mut self.schema));

        Ok(())
    }
}
