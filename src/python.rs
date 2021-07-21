use std::collections::HashMap;

use pyo3::prelude::*;

use crate::{CreateTableStatement, PropertyPair, RowFormat, StoredAs, TableColumn};

// add bindings to the generated Python module
// N.B: "rust2py" must be the name of the `.so` or `.pyd` file.

/// This module is implemented in Rust.
#[pymodule]
fn hive_ddl_parser(_py: Python, _module: &PyModule) -> PyResult<()> {
    // PyO3 aware function. All of our Python interfaces could be declared in a separate module.
    // Note that the `#[pyfn()]` annotation automatically converts the arguments from
    // Python objects to Rust values, and the Rust return value back into a Python object.
    // The `_py` argument represents that we're holding the GIL.
    #[pyfn(_module, "parse_ddl")]
    fn sum_as_string_py<'a>(_py: Python<'a>, ddl: &'a str) -> PyResult<CreateTableStatement<'a>> {
        let result = crate::parse_hive_create_table(ddl)
            .map_err(|_| pyo3::exceptions::PyValueError::new_err("Failed to parse ddl!"))?;

        Ok(result)
    }

    Ok(())
}

impl ToPyObject for TableColumn<'_> {
    fn to_object(&self, py: Python) -> PyObject {
        (self.name(), self.data_type()).to_object(py)
    }
}

impl ToPyObject for PropertyPair<'_> {
    fn to_object(&self, py: Python) -> PyObject {
        (self.key(), self.value()).to_object(py)
    }
}

impl ToPyObject for StoredAs<'_> {
    fn to_object(&self, py: Python) -> PyObject {
        let mut map = HashMap::new();

        match self {
            Self::InputOutputFormat {
                input_type,
                output_type,
            } => {
                map.insert("input_type", input_type.to_object(py));
                map.insert("output_type", output_type.to_object(py));
            }
        }

        map.to_object(py)
    }
}

impl ToPyObject for RowFormat<'_> {
    fn to_object(&self, py: Python) -> PyObject {
        let mut map = HashMap::new();

        match self {
            Self::Serde {
                type_name,
                properties,
            } => {
                map.insert("type_name", type_name.to_object(py));
                map.insert("properties", properties.to_object(py));
            }
        }

        map.to_object(py)
    }
}

impl ToPyObject for CreateTableStatement<'_> {
    fn to_object(&self, py: Python) -> PyObject {
        let mut map = HashMap::new();
        map.insert("database_name", self.database_name().to_object(py));
        map.insert("table_name", self.table_name().to_object(py));
        map.insert("columns", self.columns().to_object(py));
        map.insert("partitioned_by", self.partition_keys().to_object(py));
        map.insert("location", self.location().to_object(py));
        map.insert("table_properties", self.table_properties().to_object(py));
        map.insert("row_format", self.row_format().to_object(py));
        map.insert("stored_as", self.stored_as().to_object(py));

        map.to_object(py)
    }
}

impl IntoPy<PyObject> for CreateTableStatement<'_> {
    fn into_py(self, py: Python) -> PyObject {
        self.to_object(py)
    }
}
