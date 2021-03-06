#[cfg(test)]
pub mod tests {
    use sql_parse::sql_expression;
    use definitions::*;
    use engine::*;

    #[test]
    fn parser_select() {
        // Base test
        let mut stmt = SelectStmt {
            projection: vec![SelectProjectionColumn::Wildcard],
            from: vec![SelectFromTable::NamedTable("dual".to_string())],
            filter: vec![]
        };
        assert_eq!(sql_expression("SELECT * FROM DUAL"), Ok(SqlStmt::Select(stmt.clone())));

        let mut where_stmt = stmt.clone();
        where_stmt.filter = vec![SelectWhereFilter::ColumnLiteral("dummy".to_string(),Comparator::Equals,LiteralValue::Text("X".to_string()))];
        assert_eq!(sql_expression("SELECT * FROM DUAL WHERE dummy = 'X'"), Ok(SqlStmt::Select(where_stmt)));

        stmt.projection = vec![SelectProjectionColumn::Named("my_column1".to_string()),SelectProjectionColumn::Named("my_column2".to_string())];
        assert_eq!(sql_expression("SELECT my_column1,my_column2 FROM DUAL"), Ok(SqlStmt::Select(stmt.clone())));

        stmt.from = vec![SelectFromTable::Function(
                FunctionCall{
                    function_name: "csv_table".to_string(),
                    arguments: vec![
                        FunctionArgument::LiteralValue(
                            LiteralValue::Text("my_table.csv".to_string())
                        ),
                        FunctionArgument::LiteralValue(
                            LiteralValue::Bool(true)
                        )
                    ]
                }
            )];
        assert_eq!(sql_expression("SELECT my_column1,my_column2 FROM csv_table('my_table.csv',true)"), Ok(SqlStmt::Select(stmt.clone())));

    }

    #[test]
    fn parser_create() {
        let stmt = CreateTableStmt {
            table_name : "test_table_1".to_string(),
            column_defs : vec![
                ColumnDefinition {
                    name: "test_column1".to_string(),
                    ctype: ColumnType::Text
                }
            ]
        };
        assert_eq!(sql_expression("create table test_table_1(test_column1 text)"), Ok(SqlStmt::CreateTable(stmt.clone())));
    }

    #[test]
    fn create_table_insert_select() {
        let mut engine = SqlEngine::new();
        engine.excecute_stmt(sql_expression("CREATE TABLE ABC(COL1 BOOL, COL2 BOOL)").unwrap()).unwrap();
        engine.excecute_stmt(sql_expression("INSERT INTO ABC(COL1, COL2) VALUES (TRUE, FALSE)").unwrap()).unwrap();
        assert!(engine.excecute_stmt(sql_expression("INSERT INTO ABC(COL1, COL2) VALUES (TRUE)").unwrap()).is_err());
        assert!(engine.excecute_stmt(sql_expression("INSERT INTO ABC(COL1) VALUES (TRUE,FALSE)").unwrap()).is_err());
        assert!(engine.excecute_stmt(sql_expression("INSERT INTO ABC(COL1,COL1) VALUES (TRUE,FALSE)").unwrap()).is_err());

        let mut result = engine.excecute_stmt(sql_expression("SELECT * FROM ABC").unwrap()).unwrap();
        let mut expected_result = SqlResult::Rows(vec![vec![LiteralValue::Bool(true), LiteralValue::Bool(false)]]);
        assert_eq!(result,expected_result);

        result = engine.excecute_stmt(sql_expression("SELECT COL1 FROM ABC").unwrap()).unwrap();
        expected_result = SqlResult::Rows(vec![vec![LiteralValue::Bool(true)]]);
        assert_eq!(result,expected_result);

        result = engine.excecute_stmt(sql_expression("SELECT COL2 FROM ABC").unwrap()).unwrap();
        expected_result = SqlResult::Rows(vec![vec![LiteralValue::Bool(false)]]);
        assert_eq!(result,expected_result);

    }

}
