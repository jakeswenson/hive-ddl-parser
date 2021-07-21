use hive_ddl_parser;
use std::io::stdin;
use std::io::BufRead;

fn main() {
    let ddl: String = stdin().lock().lines().map(|l| l.unwrap()).collect();
    println!(
        "{:?}",
        hive_ddl_parser::parse_hive_create_table(&ddl).unwrap()
    );
}
