use clap::Clap;
use serde_json::Value;
use std::collections::{BTreeMap, HashMap};
use std::fmt;

use std::fs::File;
use std::io::prelude::*;
use std::io::{BufReader};

#[derive(Clap, Debug, Clone)]
#[clap(version = "1.0", author = "Yuri Titov <ytitov@gmail.com>")]
pub struct Opts {
    pub in_file: String,
    pub out_folder: String,
    #[clap(short, long, default_value = "ROOT", about = "The root table")]
    pub root_table_name: String,
    #[clap(short, long, default_value = "_ID")]
    pub column_id_postfix: String,
    /*
    #[clap(long, about = "This will indicate if the object represented by this table contains a value represented by the property named: CONTAINS_<what table it is in> The property name will be the last item delimited by _")]
    pub child_prop_hint_columns: bool,
    */
    #[clap(long, about = "Add a column to the table inside the given csv file")]
    pub add_column_name: Option<String>,
}

struct CsvFileInfo {
    pub columns: BTreeMap<String, u16>,
    pub lines_in_file: usize,
}

fn get_csv_file_info(fname: &str) -> std::result::Result<CsvFileInfo, Box<dyn std::error::Error>> {
    use std::io::prelude::*;
    use std::path::Path;
    let path = Path::new(fname);
    let display = path.display();
    let mut columns: BTreeMap<String, u16> = BTreeMap::new();
    let file = match File::open(&path) {
        Err(_why) => {
            println!("INFO: Did not see file {}, will create one", display);
            return Ok(CsvFileInfo{
                    columns, lines_in_file: 0
                });
        },
        Ok(file) => file,
    };
    let f = BufReader::new(file);


    let mut num: usize = 0;
    for line in f.lines() {
        match &line {
            Ok(line) => {
                if num == 0 {
                    let first_line = String::from(line);
                    let cols: Vec<&str> = first_line.trim().split(",").collect();
                    let mut idx = 0;
                    for col in cols {
                        columns.entry(col.to_owned()).or_insert(idx);
                        idx += 1;
                    }

                }
                num += 1;
            }
            Err(_) => {
                return Ok(CsvFileInfo{
                    columns, lines_in_file: num
                });
            }
        }
    }
    //println!("{:} has {:} lines", &display, num);
    return Ok(CsvFileInfo { columns, lines_in_file: num });
}

#[derive(Debug)]
pub struct Table {
    pub name: String,
    pub columns: BTreeMap<String, u16>,
    pub rows: BTreeMap<usize, BTreeMap<String, Value>>,
    pub row_offset: usize,
    /// are we appending to an existing CSV file and columns should be prespecified
    pub appending_mode: bool,
}

impl Table {
    pub fn new(name: &str, opts: &Opts) -> Self {
        let fname = format!("{}/{}.csv", &opts.out_folder, name);
        let mut appending_mode = false;
        let columns;
        let row_offset = match get_csv_file_info(&fname) {
            Ok(csv_info) => {
                let mut num = csv_info.lines_in_file;
                columns = csv_info.columns;
                if num > 0 {
                    num -= 1;
                    appending_mode = true;
                }
                num
            },
            Err(er) => {
                panic!("Ran into a fatal error while looking for file {:?}", er);
            }
        };
        Table {
            name: name.to_owned(),
            columns,
            rows: BTreeMap::new(),
            row_offset,
            appending_mode,
        }
    }

    pub fn load(opts: &Opts) -> Result<Self, Box<dyn std::error::Error>> {
        let file_info = get_csv_file_info(&opts.in_file)?;
        let f = File::open(&opts.in_file)?;
        let f = BufReader::new(f);
        let columns = file_info.columns;
        let mut rows = BTreeMap::new();
        let mut idx_to_name: HashMap<u16, String> = HashMap::new();
        for (col_name, col_idx) in &columns {
            idx_to_name.insert(*col_idx, col_name.to_owned());
        }
        let mut idx = 0;
        for line in f.lines() {
            match &line {
                Ok(line) => {
                    //self.trav(0, None, vec![String::from(&self.opts.root_table_name)], val);
                    if idx > 0 {
                        let cur_line = String::from(line);
                        let col_vals: Vec<&str> = cur_line.trim().split(",").collect();
                        let mut row: BTreeMap<String, Value> = BTreeMap::new();
                        for (idx, value) in col_vals.into_iter().enumerate() {
                            let col_name = idx_to_name.get(&(idx as u16))
                                .expect(&format!("Fatal: ran into a non-existing column while scanning the file: {} - {:?}", idx, value));
                            if let Ok(value) = serde_json::from_str(value) {
                                row.insert(col_name.to_owned(), value);
                            }
                        }
                        rows.insert(rows.len(),row);
                    }
                    idx += 1;
                }
                Err(e) => {
                    println!("Reached end of line: {}", e);
                }
            }
        }
        Ok( Table {
            name: opts.root_table_name.clone(),
            columns,
            rows,
            row_offset: 0,
            appending_mode: false,
        })
    }

    pub fn add_row(&mut self, row: BTreeMap<String, Value>) {
        for (key, _) in &row {
            if self.appending_mode == true && !self.columns.contains_key(key) {
                panic!("You are appending to {}, which does not have the required column {:?}", &self.name, key);
            }
            let num_cols = self.columns.len() as u16;
            self.columns.entry(key.to_owned()).or_insert(num_cols);
        }
        self.rows.insert(self.rows.len() + self.row_offset + 1, row);
    }

    pub fn export_csv(self, opts: &Opts) {
        use std::io::prelude::*;
        use std::path::Path;
        let fname = format!("{}/{}.csv", &opts.out_folder, &self.name);
        let path = Path::new(&fname);
        let display = path.display();

        /*
        let mut file = match File::create(&path) {
            Err(why) => panic!("couldn't create {}: {}", display, why),
            Ok(file) => file,
        };
        */

        use std::fs::OpenOptions;
        //let mut file = match File::open(&path) {
        let mut file = match OpenOptions::new().write(true).append(true).open(&path) {
            Err(why) => {
                println!("Creating file {}\n   Reason: {}", display, why);
                match File::create(&path) {
                    Err(why) => panic!("couldn't create {}: {}", display, why),
                    Ok(file) => file,
                }
            }
            Ok(file) => file,
        };

        let total_lines = get_csv_file_info(&fname).expect("Could not count lines").lines_in_file;

        // only add columns if needed
        if total_lines == 0 {
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

    pub fn create_table(&mut self, table_name: String) {
        if !self.data.contains_key(&table_name) {
            let t = Table::new(&table_name, &self.opts);
            self.data.insert(table_name, t);
        }
    }

    pub fn get_num_table_rows(&mut self, tables: &[String]) -> usize {
        let table_name = tables.join("_");
        if let Some(t) = self.data.get_mut(&table_name) {
            return t.rows.len() + t.row_offset
        } else {
            panic!("set_row could not find the table, by this point this should not happen");
        }
    }

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
                //if row_values.len() > 0 {
                    if parents.len() > 1 {
                        let grand_parents = parents
                            .clone()
                            .into_iter()
                            .take(parents.len() - 1)
                            .collect::<Vec<String>>();
                        let grand_parent_name = grand_parents.join("_");
                        row_values.insert(self.as_fk(&grand_parent_name), Value::from(self.get_num_table_rows(&grand_parents)));
                    }
                    //self.add_table_row(&parents, row_values);
                //}
                    self.add_table_row(&parents, row_values);
                None
            }
            Value::Array(arr) => {
                self.create_table(parents.join("_"));
                for val in arr {
                    let mut row_values = BTreeMap::new();
                    if let Some((key, value)) = self.trav2(parents.clone(), val) {
                        row_values.insert(key, value);
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
                    println!("WARNING: {}, skipping this json string.", e);
                }
            }
        }

        self.export_csv();

        Ok(())
    }
}
