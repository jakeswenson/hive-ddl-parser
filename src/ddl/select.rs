pub enum OuterJoinType {
    Left,
    Right,
    Full,
}

pub enum JoinType {
    Inner,
    Outer(OuterJoinType),
    LeftSemi,
    CrossJoin,
}

pub struct TableName<'a>(Option<&'a str>, &'a str);

pub enum SelectFrom<'a> {
    Table(TableName<'a>),
    Join(JoinType, TableName<'a>),
}

pub struct SelectStatement<'a> {
    _from: Vec<SelectFrom<'a>>,
}
