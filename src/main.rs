#![feature(plugin,drain,owned_ascii_ext)]
#![plugin(peg_syntax_ext)]
#![allow(dead_code)]

extern crate bit_vec;

use sql_parse::sql_expression;
use std::io::BufRead;
use engine::*;

mod tests;
mod engine;
mod tables;
mod definitions;
mod plan;
mod schema;
mod ir;

peg_file! sql_parse("sql.rustpeg");


fn main() {
    let mut engine = SqlEngine::new();
    let io = std::io::stdin();

    loop {
        let mut statement_bytes = Vec::new();
        let _ = io.lock().read_until(b';', &mut statement_bytes);
        // Get rid of semicolon
        let _ = statement_bytes.pop();
        let statement_string = String::from_utf8(statement_bytes);

        let expr_result = sql_expression(&statement_string.unwrap().trim());
        match expr_result {
            Ok(stmt) => {
                match engine.excecute_stmt(stmt) {
                    Ok(_) => {},
                    Err(e) => {
                        println!("Error: {}",e);
                    }
                }
            },
            Err(e) => {
                println!("Error: {:?}",e);
            }
        }
    }

}
