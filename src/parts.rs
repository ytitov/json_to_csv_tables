use clap::Clap;
use serde_json::Value;
use std::collections::{BTreeMap, HashMap};
use std::fmt;

use std::fs::File;
use std::io::prelude::*;
use std::io::{self, BufReader};

#[derive(Clap, Debug, Clone)]
#[clap(version = "1.0", author = "Yuri Titov <ytitov@gmail.com>")]
pub struct Opts {
    pub in_file: String,
    pub out_folder: String,
    #[clap(short, long, default_value = "ROOT", about = "The root table")]
    pub root_table_name: String,
    #[clap(short, long, default_value = "_ID")]
    pub column_id_postfix: String,
    #[clap(long, about = "This will indicate if the object represented by this table contains a value represented by the property named: CONTAINS_<what table it is in> The property name will be the last item delimited by _")]
    pub child_prop_hint_columns: bool,
}

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

    pub fn create_entry(&mut self, parent_table: Option<(usize, &str)>, opts: &Opts) -> usize {
        let pk = self.rows.len();
        if let Some((fk, parent_name)) = parent_table {
            let col_name = format!("{}{}", parent_name, &opts.column_id_postfix);
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

    pub fn set_last_row(&mut self, row: BTreeMap<String, Value>) {
        if self.rows.len() > 0 {
            let pk = self.rows.len() - 1;
            self.set_row(pk, row);
        } else {
            println!("WARNING: set last row called on empty table");
        }
    }

    pub fn add_row(&mut self, row: BTreeMap<String, Value>) {
        for (key, _) in &row {
            self.columns.entry(key.to_owned()).or_insert(0);
        }
        self.rows.insert(self.rows.len(), row);
    }

    pub fn export_csv(self, opts: &Opts) {
        use std::io::prelude::*;
        use std::path::Path;
        let fname = format!("{}/{}.csv", &opts.out_folder, &self.name);
        let path = Path::new(&fname);
        let display = path.display();

        let mut file = match File::create(&path) {
            Err(why) => panic!("couldn't create {}: {}", display, why),
            Ok(file) => file,
        };

        // Write the `LOREM_IPSUM` string to `file`, returns `io::Result<()>`
        let mut columns_str = format!("{}{}", self.name, &opts.column_id_postfix);
        for (key, _val) in &self.columns {
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
    pub data: HashMap<String, Table>,
    pub opts: Opts,
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
    pub fn new(opts: Opts) -> Self {
        Schema {
            data: HashMap::new(),
            opts,
        }
    }
    /*
    pub fn create_entry(
        &mut self,
        parent_table: Option<(usize, &str)>,
        tables: &[String],
        opts: &Opts,
    ) -> usize {
        let table_name = tables.join("_");
        if !self.data.contains_key(&table_name) {
            let t = Table::new(&table_name);
            self.data.insert(table_name.clone(), t);
        }
        if let Some(t) = self.data.get_mut(&table_name) {
            t.create_entry(parent_table, opts)
        } else {
            panic!("Could not find table which is bizare");
        }
    }
    */

    pub fn create_table(&mut self, table_name: String) {
        if !self.data.contains_key(&table_name) {
            let t = Table::new(&table_name);
            self.data.insert(table_name, t);
        }
    }

    /*
    pub fn set_table_row(&mut self, tables: &[String], idx: usize, row: BTreeMap<String, Value>) {
        let table_name = tables.join("_");
        if let Some(t) = self.data.get_mut(&table_name) {
            t.set_row(idx, row);
        } else {
            panic!("set_row could not find the table, by this point this should not happen");
        }
    }
    */

    pub fn get_num_table_rows(&mut self, tables: &[String]) -> usize {
        let table_name = tables.join("_");
        if let Some(t) = self.data.get_mut(&table_name) {
            return t.rows.len()
        } else {
            panic!("set_row could not find the table, by this point this should not happen");
        }
    }

    /*
    pub fn set_or_add_table_row(
        &mut self,
        tables: &[String],
        idx: usize,
        row: BTreeMap<String, Value>,
    ) {
        let table_name = tables.join("_");
        if let Some(t) = self.data.get_mut(&table_name) {
            if t.rows.len() > idx {
                t.set_row(idx, row);
            } else {
                t.add_row(row);
            }
        } else {
            panic!("set_row could not find the table, by this point this should not happen");
        }
    }
    */

    /*
    pub fn set_last_table_row(&mut self, tables: &[String], row: BTreeMap<String, Value>) {
        let table_name = tables.join("_");
        if let Some(t) = self.data.get_mut(&table_name) {
            t.set_last_row(row);
        } else {
            panic!("set_last_row could not find the table, by this point this should not happen");
        }
    }
    */

    pub fn add_table_row(&mut self, tables: &[String], row: BTreeMap<String, Value>) {
        let table_name = tables.join("_");
        if let Some(t) = self.data.get_mut(&table_name) {
            t.add_row(row);
        } else {
            panic!(format!("set_row could not find the table ({:?})\n    {:?}", tables, &row));
        }
    }

    fn as_fk(&self, s: &str) -> String {
        return format!("{}{}", s, self.opts.column_id_postfix);
    }

    pub fn trav2(&mut self, parents: Vec<String>, val: Value) -> Option<(String, Value)> {
        //println!("Processing: {:?} \n   {:?}", &parents, &val);
        match val {
            Value::Object(obj) => {
                self.create_table(parents.join("_"));
                let mut row_values = BTreeMap::new();
                for (key, val) in obj {
                    let mut p = parents.clone();
                    p.push(key);
                    if let Some((key, value)) = self.trav2(p, val) {
                        row_values.insert(key, value);
                    }
                }
                if row_values.len() > 0 {
                    if parents.len() > 1 {
                        let grand_parents = parents
                            .clone()
                            .into_iter()
                            .take(parents.len() - 1)
                            .collect::<Vec<String>>();
                        let grand_parent_name = grand_parents.join("_");
                        row_values.insert(self.as_fk(&grand_parent_name), Value::from(self.get_num_table_rows(&grand_parents)));
                    }
                    self.add_table_row(&parents, row_values);
                }
                None
            }
            Value::Array(arr) => {
                self.create_table(parents.join("_"));
                let mut row_values = BTreeMap::new();
                for val in arr {
                    if let Some((key, value)) = self.trav2(parents.clone(), val) {
                        row_values.insert(key, value);
                    }
                }
                if row_values.len() > 0 {
                    if parents.len() > 1 {
                        let grand_parents = parents
                            .clone()
                            .into_iter()
                            .take(parents.len() - 1)
                            .collect::<Vec<String>>();
                        let grand_parent_name = grand_parents.join("_");
                        row_values.insert(self.as_fk(&grand_parent_name), Value::from(self.get_num_table_rows(&grand_parents)));
                    }
                    self.add_table_row(&parents, row_values);
                }
                None
            }
            other_value => {
                // ignore parents when its a non container
                let key = &parents[parents.len() - 1];
                Some((key.to_owned(), other_value))
            }
        }
    }

    /*
    fn add_child_hint_column(
        &mut self,
        parents_info: &Option<(usize, &str)>,
        parents: &Vec<String>,
    ) {
        if let Some((pk, parent_table)) = &parents_info {
            let object_table_name = &parents[parents.len() - 1];
            //println!("{:} - {:?}", &parent_table, &object_table_name);
            let mut vals = BTreeMap::new();
            vals.insert(
                format!("CONTAINS_{}__{}", parent_table, object_table_name),
                Value::Bool(true),
            );
            let parent_full_path = parents
                .clone()
                .into_iter()
                .take(parents.len() - 1)
                .collect::<Vec<String>>();
            /*
               println!(
               "add these values to siblings {:?} OF: {:?}",
               &vals, &parent_full_path
               );
               */
            self.set_or_add_table_row(&parent_full_path, *pk, vals);
        }
    }
*/

    /*
    pub fn trav(
        &mut self,
        depth: u16,
        parents_info: Option<(usize, &str)>,
        parents: Vec<String>,
        val: Value,
    ) -> Option<Value> {
        match &val {
            Value::Object(o) => {
                //println!("{:?} - {:?}", &parents_info, &parents);
                let fk = self.create_entry(parents_info, &parents, &self.opts.clone());
                let mut values = BTreeMap::new();
                // create a hint that this table contains elements from another table
                // not sure if this is that useful
                
                if self.opts.child_prop_hint_columns == true {
                    self.add_child_hint_column(&parents_info, &parents);
                }

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
                        values.insert(
                            format!("{}{}", parent_name, &self.opts.column_id_postfix),
                            Value::from(pk),
                        );
                        // the fk will only be valid if there was parents_info
                        // TODO: check this better
                        self.set_table_row(&parents, fk, values);
                    } else {
                        //self.add_table_row(&parents, values);
                        self.set_or_add_table_row(&parents, fk, values);
                    }
                }
                None
            }
            Value::Array(arr) => {
                // do not pass parent information of arrays
                let fk = self.create_entry(None, &parents, &self.opts.clone());

                let col_name;
                if parents.len() > 0 {
                    col_name = String::from(&parents[parents.len() - 1]);
                } else {
                    panic!("Got parents length of zero, this was unexpected");
                }
                for val in arr {
                    //println!("parents {:?} value: {:?}", &parents, &val);
                    // the parent table of these values.  The idea here is that the FK is from the
                    // parent table of this array, but we continue passing the path that is one
                    // level up (I'm still scratching my head on this one)
                    let parent_full_path = parents
                        .clone()
                        .into_iter()
                        .take(parents.len() - 1)
                        .collect::<Vec<String>>();
                    let v = self.trav(
                        depth + 1,
                        //Some((fk, &parents.as_slice().join("_"))),
                        Some((fk, &parent_full_path.as_slice().join("_"))),
                        parents.clone(),
                        val.to_owned(),
                    );
                    if let Some(v) = v {
                        match v {
                            Value::Array(_) => {}
                            Value::Object(_) => {}
                            other => {
                                let mut hs = BTreeMap::new();
                                hs.insert(col_name.clone(), other);
                                if let Some((pk, parent_name)) = parents_info {
                                    hs.insert(
                                        format!("{}{}", parent_name, &self.opts.column_id_postfix),
                                        Value::from(pk),
                                    );
                                }
                                self.add_table_row(&parents, hs);
                            }
                        };
                    }
                }
                None
            }
            other => Some(other.to_owned()),
        }
    }
    */

    pub fn export_csv(self) {
        for (_, table) in self.data {
            table.export_csv(&self.opts);
        }
    }

    pub fn process_file(mut self) -> std::result::Result<(), Box<dyn std::error::Error>> {
        let f = File::open(&self.opts.in_file)?;
        let f = BufReader::new(f);

        for line in f.lines() {
            match serde_json::from_str(&line?) {
                Ok::<Value, _>(val) => {
                    //self.trav(0, None, vec![String::from(&self.opts.root_table_name)], val);
                    self.trav2(vec![String::from(&self.opts.root_table_name)], val);
                }
                Err(e) => {
                    println!("JsonError: {:?}", e);
                }
            }
        }

        self.export_csv();

        Ok(())
    }
}
