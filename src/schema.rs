use definitions::*;
use tables::*;

#[derive(Clone,PartialEq,Debug)]
pub struct Schema {
    tables: Vec<MemoryTable>,
    // If a name is None then the table was deleted
    names: Vec<Option<String>>
}
impl Schema {
    pub fn new() -> Schema {
        Schema {
            tables: Vec::new(),
            names: Vec::new()
        }
    }

    pub fn find_table(&self, name: &str) -> Option<usize> {
        self.names.iter().position(|x| {
            match x {
                &Some(ref n) => {
                    n == name
                },
                &None => false
            }
        })
    }

    pub fn find_table_or_err(&self, name: &str) -> SqlError<usize> {
        self.find_table(name).ok_or(format!("Table {} doesn't exist", name))
    }

    pub fn map_on_table_mut<F,H>(&mut self, index: usize, mut closure: F) -> SqlError<H>
        where F: FnMut(&mut MemoryTable) -> SqlError<H> {
            match self.tables.get_mut(index) {
                Some(table) => {
                    closure(table)
                },
                None => {
                    Err(format!("Internal Error: Table index {} doesn't exist in map_on_table_mut", index))
                }
            }
    }

    pub fn map_on_table<F,H>(&self, index: usize, mut closure: F) -> SqlError<H>
        where F: FnMut(&MemoryTable) -> SqlError<H> {
            match self.tables.get(index) {
                Some(table) => {
                    closure(table)
                },
                None => {
                    Err(format!("Internal Error: Table index {} doesn't exist in map_on_table", index))
                }
            }
    }


    pub fn create_table(&mut self, table_name: &String) -> SqlError<usize> {
        if self.find_table(table_name).is_some() {
            return Err(format!("Table {} already exists",table_name));
        }

        self.names.push(Some(table_name.clone()));
        self.tables.push(MemoryTable::new());
        Ok(self.names.len()-1)

    }

    pub fn drop_table(&mut self, index: usize) -> SqlError<()> {
        match self.names.get_mut(index) {
            Some(t) => {
                *t = None;
                Ok(())
            },
            None => {
                Err(format!("Internal Error: Table index {} doesn't exist in drop_table", index))
            }
        }
    }
}
