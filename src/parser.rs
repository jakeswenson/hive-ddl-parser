use crate::Result;
use pest::Parser;
use pest_derive::Parser;
use snafu::ResultExt;

#[derive(Parser)]
#[grammar = "hive_ddl.pest"]
pub(crate) struct HiveDDLParser;

pub(crate) type Pair<'a> = pest::iterators::Pair<'a, Rule>;

pub(crate) fn _parse(ddl: &str) -> Result<Pair> {
    let mut pairs = HiveDDLParser::parse(Rule::statements, ddl).context(crate::ParseError)?;
    let parse_result: Pair = pairs.next().unwrap();

    Ok(parse_result)
}

pub(crate) fn parse_create_table_only(ddl: &str) -> Result<Pair> {
    let mut pairs = HiveDDLParser::parse(Rule::create_table, ddl).context(crate::ParseError)?;
    let parse_result: Pair = pairs.next().unwrap();

    Ok(parse_result)
}
