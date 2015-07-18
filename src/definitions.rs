pub type SqlError<T> = Result<T,String>;

#[derive(Debug,Clone,PartialEq)]
pub enum SqlStmt {
    Select(SelectStmt),
    DumpTables,
    CreateTable(CreateTableStmt),
    DropTable(DropTableStmt),
    Insert(InsertStmt)
}

#[derive(Debug,Clone,PartialEq)]
pub enum SqlResult {
    Message(String),
    RowsUpdated(usize),
    Rows(Vec<Vec<LiteralValue>>),
    None
}


#[derive(Clone,PartialEq,Debug)]
pub enum LiteralValue {
    Null,
    Text(String),
    Bool(bool)
}

impl LiteralValue {
    pub fn as_string(&self) -> SqlError<String> {
        match *self {
            LiteralValue::Null => {
                Ok(String::with_capacity(0))
            },
            LiteralValue::Text(ref s) => {
                Ok(s.clone())
            },
            LiteralValue::Bool(b) => {
                Ok(format!("{}",b))
            }
        }
    }

    pub fn as_bool(&self) -> SqlError<bool> {
        match *self {
            LiteralValue::Null => {
                Err("Cannot convert Null value to bool".to_string())
            },
            LiteralValue::Text(ref s) => {
                Err(format!("Cannot convert Text value ({}) to bool",s))
            },
            LiteralValue::Bool(b) => {
                Ok(b)
            }
        }
    }

    pub fn to_type(&self,col_type: ColumnType) -> SqlError<LiteralValue> {
        match col_type {
            ColumnType::Text => {
                Ok(LiteralValue::Text(try!(self.as_string())))
            },
            ColumnType::Bool => {
                Ok(LiteralValue::Bool(try!(self.as_bool())))
            }
        }
    }
}

#[derive(Clone,PartialEq,Debug)]
pub enum FunctionArgument {
    LiteralValue(LiteralValue),
    Identifier(String)
}

#[derive(Clone,PartialEq,Debug)]
pub struct FunctionCall {
    pub function_name: String,
    pub arguments: Vec<FunctionArgument>
}

#[derive(Debug,Clone,PartialEq)]
pub struct SelectStmt {
    pub projection: SelectProjection,
    pub from: Vec<SelectFromTable>,
    pub filter: Vec<SelectWhereFilter>
}

#[derive(Debug,Clone,PartialEq)]
pub enum SelectProjection {
    Columns(Vec<SelectProjectionColumn>),
    Wildcard
}

#[derive(Debug,Clone,PartialEq)]
pub enum SelectProjectionColumn {
    Named(String),
    LiteralValue(LiteralValue)
}


#[derive(Debug,Clone,PartialEq)]
pub enum SelectFromTable {
    Function(FunctionCall),
    NamedTable(String)
}

#[derive(Debug,Clone,PartialEq)]
pub enum Comparator {
    Equals
}

#[derive(Debug,Clone,PartialEq)]
pub enum SelectWhereFilter {
    ColumnColumn(String,Comparator,String),
    ColumnLiteral(String,Comparator,LiteralValue)
}

#[derive(Debug,Clone,PartialEq)]
pub struct CreateTableStmt {
    pub table_name: String,
    pub column_defs: Vec<ColumnDefinition>
}

#[derive(Debug,Clone,PartialEq)]
pub struct DropTableStmt {
    pub table_name: String
}

#[derive(Debug,Clone,PartialEq)]
pub struct InsertStmt {
    pub table_name: String,
    pub column_names: Vec<String>,
    pub column_values: Vec<LiteralValue>
}

#[derive(Clone,PartialEq,Debug,Copy)]
pub enum ColumnType {
    Text,
    Bool
}

impl ColumnType {
    pub fn accomodate_literal(val: &LiteralValue) -> ColumnType {
        match val {
            &LiteralValue::Null => {
                ColumnType::Text
            },
            &LiteralValue::Text(_) => {
                ColumnType::Text
            },
            &LiteralValue::Bool(_) => {
                ColumnType::Bool
            }
        }
    }
}

#[derive(Clone,PartialEq,Debug)]
pub struct ColumnDefinition {
    pub ctype: ColumnType,
    pub name: String
}
