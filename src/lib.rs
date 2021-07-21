pub mod ddl;
mod parser;

pub mod python;

pub use ddl::create_table::{
    parse_hive_create_table, CreateTableStatement, PropertyPair, RowFormat, StoredAs, TableColumn,
};

pub use ddl::select::SelectStatement;

use pest;
use snafu::Snafu;

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("Unable to parse ddl: {}", source))]
    ParseError {
        source: pest::error::Error<parser::Rule>,
    },
}

type Result<T, E = Error> = std::result::Result<T, E>;

pub enum Ddl<'a> {
    CreateTable(CreateTableStatement<'a>),
    Select(SelectStatement<'a>),
}
