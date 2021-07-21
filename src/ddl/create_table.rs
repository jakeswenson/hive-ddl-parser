use crate::parser::{Pair, Rule};

use crate::Result;

fn clean_parsed_string(s: &str) -> String {
    if s.starts_with("'") || s.starts_with("\"") {
        s[1..s.len() - 1].replace("\\", "")
    } else {
        s.to_string()
    }
}

fn parse_column(pair: Pair) -> TableColumn {
    let mut name = None;
    let mut data_type = None;
    let mut comment = None;

    for part in pair.into_inner() {
        match part.as_rule() {
            Rule::column_name => name = Some(part.as_str()),
            Rule::data_type => data_type = Some(part.as_str()),
            Rule::ddl_comment => comment = Some(part.into_inner().next().unwrap().as_str()),
            rule => unreachable!("Bad column parse! Rule::{:?}", rule),
        }
    }

    TableColumn {
        name: name.unwrap(),
        data_type: data_type.unwrap(),
        comment,
    }
}

impl<'l> From<Pair<'l>> for RowFormat<'l> {
    fn from(pair: Pair<'l>) -> Self {
        fn parse_serde(pair: Pair) -> RowFormat {
            let mut type_name = None;
            let mut properties = Vec::new();

            for serde_part in pair.into_inner() {
                match serde_part.as_rule() {
                    Rule::serde_name => type_name = Some(serde_part.as_str()),
                    Rule::serde_properties => {
                        for part in serde_part.into_inner() {
                            match part.as_rule() {
                                Rule::serde_property => {
                                    properties.push((part.as_str(), part.as_str()))
                                }
                                _ => unreachable!(),
                            }
                        }
                    }
                    _ => unreachable!(),
                }
            }

            RowFormat::Serde {
                type_name: type_name.unwrap(),
                properties,
            }
        }

        for part in pair.into_inner() {
            return match part.as_rule() {
                Rule::row_format_serde => parse_serde(part),
                _ => unreachable!("Unsupported format "),
            };
        }

        unreachable!()
    }
}

impl<'l> From<Pair<'l>> for StoredAs<'l> {
    fn from(pair: Pair<'l>) -> Self {
        fn parse_file_formats(pair: Pair) -> StoredAs {
            let mut input_type = None;
            let mut output_type = None;

            for part in pair.into_inner() {
                match part.as_rule() {
                    Rule::input_format_classname => input_type = Some(part.as_str()),
                    Rule::output_format_classname => output_type = Some(part.as_str()),
                    _ => unreachable!(),
                }
            }

            StoredAs::InputOutputFormat {
                input_type: input_type.unwrap(),
                output_type: output_type.unwrap(),
            }
        }

        for part in pair.into_inner() {
            return match part.as_rule() {
                Rule::file_format => parse_file_formats(part),
                _ => unreachable!("Unsupported format "),
            };
        }

        unreachable!()
    }
}

impl<'l> From<Pair<'l>> for PropertyPair<'l> {
    fn from(pair: Pair<'l>) -> Self {
        let mut pairs = pair.into_inner();
        Self(
            pairs.next().unwrap().as_str(),
            pairs.next().unwrap().as_str(),
        )
    }
}

impl<'l> From<Pair<'l>> for TableColumn<'l> {
    fn from(pair: Pair<'l>) -> Self {
        parse_column(pair)
    }
}

pub fn parse_hive_create_table(ddl: &str) -> Result<CreateTableStatement> {
    let mut database_name = None;
    let mut table_name = None;
    let mut columns = Vec::new();
    let mut partition_keys = Vec::new();
    let mut row_format = None;
    let mut stored_as = None;
    let mut location = None;
    let mut table_properties = Vec::new();

    let parse_result = crate::parser::parse_create_table_only(ddl)?;

    for pair in parse_result.into_inner() {
        match pair.as_rule() {
            Rule::table_name => table_name = Some(pair.as_str()),
            Rule::database_name => database_name = Some(pair.as_str()),
            Rule::table_column => columns.push(parse_column(pair)),
            Rule::row_format => row_format = Some(pair.into()),
            Rule::stored_as => stored_as = Some(pair.into()),
            Rule::table_location => location = Some(pair.into_inner().next().unwrap().as_str()),
            Rule::table_properties => {
                table_properties = pair.into_inner().map(Into::into).collect()
            }
            Rule::partitioned_by => partition_keys = pair.into_inner().map(Into::into).collect(),
            _ => unimplemented!("Unimplemented for rule: {:?}", pair),
        }
    }

    Ok(CreateTableStatement {
        database_name,
        table_name: table_name.expect("A table name is required, and parsing should have failed."),
        columns,
        partition_keys,
        row_format,
        stored_as,
        location,
        table_properties,
    })
}

#[derive(Debug, Default, Eq, Ord, PartialOrd, PartialEq)]
pub struct PropertyPair<'l>(&'l str, &'l str);

impl<'l> PropertyPair<'l> {
    pub fn key(&self) -> String {
        clean_parsed_string(self.0)
    }

    pub fn value(&self) -> String {
        clean_parsed_string(self.1)
    }
}

#[derive(Debug, Default, Eq, PartialEq, Ord, PartialOrd)]
pub struct TableColumn<'p> {
    name: &'p str,
    data_type: &'p str,
    comment: Option<&'p str>,
}

impl<'p> TableColumn<'p> {
    pub fn name(&self) -> &'p str {
        self.name
    }

    pub fn data_type(&self) -> &'p str {
        self.data_type
    }

    pub fn comment(&self) -> Option<&'p str> {
        self.comment
    }
}

#[derive(Debug, Eq, PartialEq, Ord, PartialOrd)]
pub enum RowFormat<'p> {
    Serde {
        type_name: &'p str,
        properties: Vec<(&'p str, &'p str)>,
    },
}

#[derive(Debug, Eq, PartialEq, Ord, PartialOrd)]
pub enum StoredAs<'p> {
    InputOutputFormat {
        input_type: &'p str,
        output_type: &'p str,
    },
}

#[derive(Debug, Default, Eq, PartialEq, Ord, PartialOrd)]
pub struct CreateTableStatement<'p> {
    database_name: Option<&'p str>,
    table_name: &'p str,
    columns: Vec<TableColumn<'p>>,
    partition_keys: Vec<TableColumn<'p>>,
    row_format: Option<RowFormat<'p>>,
    stored_as: Option<StoredAs<'p>>,
    location: Option<&'p str>,
    table_properties: Vec<PropertyPair<'p>>,
}

impl<'p> CreateTableStatement<'p> {
    pub fn database_name(&self) -> Option<&'p str> {
        self.database_name
    }

    pub fn table_name(&self) -> &'p str {
        self.table_name
    }

    pub fn columns(&self) -> &[TableColumn<'p>] {
        &self.columns
    }

    pub fn partition_keys(&self) -> &[TableColumn<'p>] {
        &self.partition_keys
    }

    pub fn row_format(&self) -> &Option<RowFormat<'p>> {
        &self.row_format
    }

    pub fn stored_as(&self) -> &Option<StoredAs<'p>> {
        &self.stored_as
    }

    pub fn location(&self) -> Option<&'p str> {
        self.location
    }

    pub fn table_properties(&self) -> &[PropertyPair<'p>] {
        &self.table_properties
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn create_table_no_db() {
        let create = parse_hive_create_table("CREATE TABLE `test`").unwrap();

        assert_eq!(create.database_name, None);
        assert_eq!(create.table_name, "`test`")
    }

    #[test]
    fn create_table_with_db() {
        let create = parse_hive_create_table("CREATE TABLE `db`.`test`").unwrap();

        assert_eq!(create.database_name, Some("`db`"));
        assert_eq!(create.table_name, "`test`")
    }

    #[test]
    fn parser_with_column() {
        let create = parse_hive_create_table("CREATE TABLE `db`.`test` (test INT)").unwrap();
        assert_eq!(create.database_name, Some("`db`"));
        assert_eq!(create.table_name, "`test`");
        assert_eq!(
            create.columns,
            vec![TableColumn {
                name: "test",
                data_type: "INT",
                comment: None
            }]
        )
    }

    #[test]
    fn table_with_multiple_columns() {
        let create =
            parse_hive_create_table("CREATE TABLE `db`.`test` (test INT, other DECIMAL(18, 2));")
                .unwrap();

        assert_eq!(create.database_name, Some("`db`"));
        assert_eq!(create.table_name, "`test`");
        assert_eq!(
            create.columns,
            vec![
                TableColumn {
                    name: "test",
                    data_type: "INT",
                    comment: None
                },
                TableColumn {
                    name: "other",
                    data_type: "DECIMAL(18, 2)",
                    comment: None
                }
            ]
        )
    }

    #[test]
    fn table_with_row_format_and_file_format() {
        let create = parse_hive_create_table(
            "CREATE EXTERNAL TABLE `hive_example`.`example`(
   `source` string COMMENT '',
   `table_name` string COMMENT '',
   `action` string COMMENT '',
   `created_at` timestamp COMMENT '',
   `row_id` binary COMMENT '',
   `event_time` timestamp COMMENT '',
   `group` int COMMENT 'Group',
   `group_name` string COMMENT 'group name',
   `description` string COMMENT 'description'
   )
    ROW FORMAT SERDE
        'org.apache.hadoop.hive.ql.io.orc.OrcSerde'
    STORED AS INPUTFORMAT
        'org.apache.hadoop.hive.ql.io.orc.OrcInputFormat'
        OUTPUTFORMAT
            'org.apache.hadoop.hive.ql.io.orc.OrcOutputFormat'
   ",
        )
        .unwrap();

        assert_eq!(create.columns.len(), 9);

        assert_eq!(
            create.row_format,
            Some(RowFormat::Serde {
                type_name: "'org.apache.hadoop.hive.ql.io.orc.OrcSerde'",
                properties: vec![]
            })
        );

        assert_eq!(
            create.stored_as,
            Some(StoredAs::InputOutputFormat {
                input_type: "'org.apache.hadoop.hive.ql.io.orc.OrcInputFormat'",
                output_type: "'org.apache.hadoop.hive.ql.io.orc.OrcOutputFormat'"
            })
        );
    }

    #[test]
    fn with_location() {
        let create = parse_hive_create_table(
            "CREATE EXTERNAL TABLE db.test
    LOCATION
        's3://location/'
   ",
        )
        .unwrap();

        assert_eq!(create.location, Some("'s3://location/'"))
    }

    #[test]
    fn table_properties() {
        let create = parse_hive_create_table(
            "CREATE EXTERNAL TABLE db.test
    TBLPROPERTIES (
        'pk.cols'='this, that',
        'provisioned.by.class'='Snappy',
        'lastDdlTime'='214123523')
",
        )
        .unwrap();

        assert_eq!(
            create.table_properties,
            vec![
                PropertyPair("'pk.cols'", "'this, that'"),
                PropertyPair("'provisioned.by.class'", "'Snappy'"),
                PropertyPair("'lastDdlTime'", "'214123523'")
            ]
        );
    }

    #[test]
    fn table_with_partitions() {
        let create = parse_hive_create_table(
            "CREATE EXTERNAL TABLE `hive_example`.`example`(
   `source` string COMMENT '',
   `table_name` string COMMENT '',
   `action` string COMMENT '')
PARTITIONED BY (
   `created_at` timestamp COMMENT 'create time',
   group int
   )",
        )
        .unwrap();

        assert_eq!(
            create.partition_keys,
            vec![
                TableColumn {
                    name: "`created_at`",
                    data_type: "timestamp",
                    comment: Some("'create time'")
                },
                TableColumn {
                    name: "group",
                    data_type: "int",
                    comment: None
                }
            ]
        )
    }
}
