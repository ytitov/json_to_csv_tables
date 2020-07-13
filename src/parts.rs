use serde_json::Value;
use std::collections::{BTreeMap, HashMap};
use std::fmt;

use std::io::{self, BufReader};
use std::io::prelude::*;
use std::fs::File;

#[derive(Debug)]
pub struct Table {
    pub name: String,
    pub columns: BTreeMap<String, u16>,
    pub rows: BTreeMap<usize, BTreeMap<String, Value>>,
}

impl Table {
    pub fn new(name: &str) -> Self {
        Table {
            name: name.to_owned(),
            columns: BTreeMap::new(),
            rows: BTreeMap::new(),
        }
    }

    pub fn create_entry(&mut self, parent_table: Option<(usize, &str)>) -> usize {
        let pk = self.rows.len();
        if let Some((fk, parent_name)) = parent_table {
            let col_name = format!("{}_id", parent_name);
            self.columns.insert(col_name.clone(), 0);
            let mut hs = BTreeMap::new();
            hs.insert(col_name, Value::from(fk));
            self.rows.insert(pk, hs);
        }
        pk
    }

    pub fn set_row(&mut self, pk: usize, row: BTreeMap<String, Value>) {
        if let Some(existing_row) = self.rows.get_mut(&pk) {
            for (col, value) in row {
                existing_row.entry(col).or_insert(value);
            }
            for (key, _) in existing_row {
                self.columns.entry(key.to_owned()).or_insert(0);
            }
        } else {
            println!("set_row with pk: {} row: {:?} \ntable: {}", pk, row, self);
            panic!("Tried to set row which does not exist");
        }
    }

    pub fn add_row(&mut self, row: BTreeMap<String, Value>) {
        for (key, _) in &row {
            self.columns.entry(key.to_owned()).or_insert(0);
        }
        self.rows.insert(self.rows.len(), row);
    }

    pub fn export_csv(self, export_path: &str) {
        use std::io::prelude::*;
        use std::path::Path;
        let fname = format!("{}/{}.csv", export_path, &self.name);
        let path = Path::new(&fname);
        let display = path.display();

        let mut file = match File::create(&path) {
            Err(why) => panic!("couldn't create {}: {}", display, why),
            Ok(file) => file,
        };

        // Write the `LOREM_IPSUM` string to `file`, returns `io::Result<()>`
        let mut columns_str = format!("{}_id", self.name);
        for (key, val) in &self.columns {
            columns_str.push_str(",");
            columns_str.push_str(&key);
        }
        columns_str.push_str("\n");
        match file.write_all(columns_str.as_bytes()) {
            Err(why) => panic!("couldn't write to {}: {}", display, why),
            Ok(_) => (),
        }

        for (idx, row) in self.rows {
            let mut line = format!("{}", idx);
            for (col, _) in &self.columns {
                if let Some(val) = row.get(col) {
                    line.push_str(&format!(",{}", val));
                } else {
                    line.push_str(",");
                }
            }
            line.push_str("\n");
            match file.write_all(line.as_bytes()) {
                Err(why) => panic!("couldn't write to {}: {}", display, why),
                Ok(_) => (),
            }
        }
        println!("successfully wrote to {}", display);
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
    pub input_file_path: String,
    pub export_path: String,
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
    pub fn new(input_file_path: &str, export_path: &str) -> Self {
        Schema {
            data: HashMap::new(),
            input_file_path: input_file_path.to_owned(),
            export_path: export_path.to_owned(),
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

    pub fn set_table_row(&mut self, tables: &[String], idx: usize, row: BTreeMap<String, Value>) {
        let table_name = tables.join("_");
        if let Some(t) = self.data.get_mut(&table_name) {
            t.set_row(idx, row);
        } else {
            panic!("set_row could not find the table, by this point this should not happen");
        }
    }

    pub fn add_table_row(&mut self, tables: &[String], row: BTreeMap<String, Value>) {
        let table_name = tables.join("_");
        if let Some(t) = self.data.get_mut(&table_name) {
            t.add_row(row);
        } else {
            panic!("set_row could not find the table, by this point this should not happen");
        }
    }

    /*
    pub fn trav2(&mut self, parents: Vec<String>, val: Value) {
        match val {
            Value::Object(obj) => {
                let mut row = BTreeMap::new();
                for (key, value) in obj {
                    match value {
                        Value::Object(obj) => {
                            let mut p = parents.clone();
                            p.push(key.clone());
                            row.insert(format!("{}_ID", parents.join("_")), Value::from(0));
                            self.trav2(p, Value::from(obj));
                        }
                        Value::Array(arr) => {
                        }
                        other => {
                            row.insert(parents.join("_"), Value::from(other));
                        }
                    }
                }
                println!("ROW: {:?}", &row);
            }
            Value::Array(arr) => {
            }
            other_value => {
            }
        }
    }
    */

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
                let mut values = BTreeMap::new();
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
                if values.len() > 0 {
                    if let Some((pk, parent_name)) = parents_info {
                        values.insert(format!("{}_id", parent_name), Value::from(pk));
                        // the fk will only be valid if there was parents_info
                        self.set_table_row(&parents, fk, values);
                    } else {
                        self.add_table_row(&parents, values);
                    }
                }
                None
            }
            Value::Array(arr) => {
                //let fk = self.create_entry(parents_info, &parents);
                let fk = self.create_entry(None, &parents);

                let mut col_name = String::from("ERROR");
                if parents.len() > 0 {
                    col_name = String::from(&parents[parents.len() - 1]);
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
                                let mut hs = BTreeMap::new();
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
            other => Some(other.to_owned()),
        }
    }

    pub fn export_csv(mut self) {
        for (_, table) in self.data {
            table.export_csv(&self.export_path);
        }
    }

    pub fn process_file(mut self) -> std::result::Result<(), Box<dyn std::error::Error>> {
        let f = File::open(&self.input_file_path)?;
        let f = BufReader::new(f);

        for line in f.lines() {
            //println!("{}", line.unwrap());
            match serde_json::from_str(&line?) {
                Ok::<Value,_>(val) => {
                    self.trav(0, None, vec![String::from("ROOT")], val);
                }
                Err(e) => {
                    println!("ERROR: {:?}", e);
                }
            }
        }

        self.export_csv();

        Ok(())
    }
}
