use serde::{de, Deserialize, Deserializer, Serialize};
use serde_json::Value;
use std::collections::HashMap;
use std::fmt;

#[derive(Deserialize, Serialize)]
pub struct Element<V> {
    pub parent: Option<String>,
    pub fk: Option<usize>,
    pub pk: usize,
    pub value: V,
    pub key: Option<String>,
}

/*
pub fn t<'de, T, Q>(t: T) where T: Deserialize<'de> {
}
*/

#[derive(Debug)]
pub struct Table {
    pub name: String,
    pub columns: HashMap<String, u16>,
    pub rows: HashMap<usize, HashMap<String, Value>>,
}

impl Table {
    pub fn new(name: &str) -> Self {
        Table {
            name: name.to_owned(),
            columns: HashMap::new(),
            rows: HashMap::new(),
        }
    }

    pub fn create_entry(&mut self, parent_table: Option<(usize, &str)>) -> usize {
        let pk = self.rows.len();
        let vals = match parent_table {
            Some((fk, parent_name)) => {
                let col_name = format!("{}_id", parent_name);
                self.columns.insert(col_name.clone(), 0);
                let mut hs = HashMap::new();
                hs.insert(col_name, Value::from(fk));
                hs
            }
            _ => HashMap::new(),
        };
        self.rows.insert(pk, vals);
        pk
    }

    pub fn set_row(&mut self, pk: usize, row: HashMap<String, Value>) {
        if let Some(existing_row) = self.rows.get_mut(&pk) {
            for (col, value) in row {
                existing_row.entry(col).or_insert(value);
            }
        }
    }

    pub fn add_row(&mut self, row: HashMap<String, Value>) {
        self.rows.insert(self.rows.len(), row);
    }
}

impl fmt::Display for Table {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}\n  Columns: {:?}\n  Rows:\n", self.name, self.columns)?;
        for (pk, val) in &self.rows {
            write!(f, "    {} {:?}\n", pk, val)?;
        }
        write!(f, "")
    }
}

#[derive(Debug)]
pub struct Schema {
    // key: (depth, table name)
    pub data: HashMap<String, Table>,
}

impl fmt::Display for Schema {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        for (_, table) in &self.data {
            write!(f, "{}\n", table)?;
        }
        write!(f, "")
    }
}

impl Schema {
    pub fn new() -> Self {
        Schema {
            data: HashMap::new(),
        }
    }
    pub fn create_entry(
        &mut self,
        parent_table: Option<(usize, &str)>,
        tables: &[String],
    ) -> usize {
        let table_name = tables.join("_");
        if !self.data.contains_key(&table_name) {
            let t = Table::new(&table_name);
            self.data.insert(table_name.clone(), t);
        }
        if let Some(t) = self.data.get_mut(&table_name) {
            t.create_entry(parent_table)
        } else {
            panic!("Could not find table which is bizare");
        }
    }

    pub fn set_table_row(&mut self, tables: &[String], idx: usize, row: HashMap<String, Value>) {
        let table_name = tables.join("_");
        if let Some(t) = self.data.get_mut(&table_name) {
            t.set_row(idx, row);
        } else {
            panic!("set_row could not find the table, by this point this should not happen");
        }
    }

    pub fn add_table_row(&mut self, tables: &[String], row: HashMap<String, Value>) {
        let table_name = tables.join("_");
        if let Some(t) = self.data.get_mut(&table_name) {
            t.add_row(row);
        } else {
            panic!("set_row could not find the table, by this point this should not happen");
        }
    }

    pub fn trav(
        &mut self,
        depth: u16,
        parents_info: Option<(usize, &str)>,
        parents: Vec<String>,
        val: Value,
    ) -> Option<Value> {
        match &val {
            Value::Object(o) => {
                let fk = self.create_entry(parents_info, &parents);
                let mut values = HashMap::new();
                //println!("Obj Create: {:?}", &parents);

                for (key, val) in o {
                    let mut p = parents.clone();
                    p.push(key.clone());
                    if let Some(v) = self.trav(
                        depth + 1,
                        Some((fk, &parents.as_slice().join("_"))),
                        p,
                        val.to_owned(),
                    ) {
                        match v {
                            Value::Array(_) => {}
                            Value::Object(_) => {}
                            other => {
                                values.insert(key.clone(), other);
                            }
                        };
                    }
                }
                //println!("Table => {:?}", &parents);
                //println!("  Row => {:?} FK: {:?}", &values, fk);
                self.set_table_row(&parents, fk, values);
                None
            }
            Value::Array(arr) => {
                //let fk = self.create_entry(parents_info, &parents);
                let fk = self.create_entry(None, &parents);

                let mut col_name = String::from("ERROR");
                if parents.len() > 0 {
                    col_name = String::from(&parents[parents.len()-1]);
                }
                for val in arr {
                    let v = self.trav(
                        depth + 1,
                        Some((fk, &parents.as_slice().join("_"))),
                        parents.clone(),
                        val.to_owned(),
                    );
                    if let Some(v) = v {
                        match v {
                            Value::Array(_) => {}
                            Value::Object(_) => {}
                            other => {
                                //values.push(other.clone());
                                //println!("array fk {} parent_info {:?}", fk, parents_info);
                                let mut hs = HashMap::with_capacity(2);
                                //hs.insert(parents.join("_"), other);
                                hs.insert(col_name.clone(), other);
                                if let Some((pk, parent_name)) = parents_info {
                                    hs.insert(format!("{}_id", parent_name), Value::from(pk));
                                }
                                self.add_table_row(&parents, hs);
                            }
                        };
                    }
                }
                //println!("Array Table => {:?}", &parents);
                //println!("  Row => {:?}", &values);
                None
            }
            other => {
                Some(other.to_owned())
            }
        }
    }
}
