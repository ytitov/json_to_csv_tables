use clap::Clap;
use serde_json::Value;
use std::collections::{BTreeMap, HashMap};
use std::fmt;

use std::fs::File;
use std::io::prelude::*;
use std::io::BufReader;

pub mod err;
pub mod table;

pub use table::Table;

#[derive(Clap, Debug, Clone)]
#[clap(version = "0.1.0", author = "Yuri Titov <ytitov@gmail.com>")]
pub struct Opts {
    pub in_file: String,
    pub out_folder: String,
    #[clap(short, long, default_value = "ROOT", about = "The root table")]
    pub root_table_name: String,
    #[clap(short, long, default_value = "_ID")]
    pub column_id_postfix: String,
    #[clap(long, about = "Add a column to the table inside the given csv file")]
    pub add_column_name: Option<String>,
    #[clap(
        long,
        about = "Number of json objects to scan before writing to disk, if not specified, the full file is scanned into memory"
    )]
    pub json_buf_size: Option<usize>,
    #[clap(long, about = "Output mysql files")]
    pub as_mysql: bool,
    #[clap(long, about = "Only scan, do not write rows.  The point is to scan the json and hopefully catch all of the fields")]
    pub scan_only: bool,
}

struct CsvFileInfo {
    pub columns: BTreeMap<String, u16>,
    pub lines_in_file: usize,
}

impl From<&File> for CsvFileInfo {
    fn from(file: &File) -> Self {
        let f = BufReader::new(file);
        let mut columns: BTreeMap<String, u16> = BTreeMap::new();
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
                    return CsvFileInfo {
                        columns,
                        lines_in_file: num,
                    };
                }
            }
        }
        return CsvFileInfo {
            columns,
            lines_in_file: num,
        };
    }
}

fn find_or_create_file(filepath: &str) -> Result<File, err::CsvError> {
    use std::fs::OpenOptions;
    use std::path::Path;

    let path = Path::new(filepath);
    let display = path.display();
    let file = match OpenOptions::new().write(true).append(true).open(&path) {
        Err(_) => {
            //println!("Creating file {}\n   Reason: {}", display, why);
            match File::create(filepath) {
                Err(why) => {
                    return Err(err::CsvError::CouldNotCreate(format!(
                        "{}, because {}",
                        display, why
                    )))
                }
                Ok(file) => file,
            }
        }
        Ok(file) => file,
    };
    Ok(file)
}

fn get_csv_file_info(fname: &str) -> CsvFileInfo {
    use std::io::prelude::*;
    use std::path::Path;
    let path = Path::new(fname);
    //let display = path.display();
    let mut columns: BTreeMap<String, u16> = BTreeMap::new();
    let file = match File::open(&path) {
        Err(_why) => {
            //println!("INFO: Did not see file {}, will create one", display);
            return CsvFileInfo {
                columns,
                lines_in_file: 0,
            };
        }
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
                return CsvFileInfo {
                    columns,
                    lines_in_file: num,
                };
            }
        }
    }
    return CsvFileInfo {
        columns,
        lines_in_file: num,
    };
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
            return t.rows.len() + t.row_offset;
        } else {
            panic!("set_row could not find the table, by this point this should not happen");
        }
    }

    pub fn add_table_row(
        &mut self,
        tables: &[String],
        row: BTreeMap<String, Value>,
    ) -> Result<(), err::CsvError> {
        let table_name = tables.join("_");
        if let Some(t) = self.data.get_mut(&table_name) {
            t.add_row(row)?;
            Ok(())
        } else {
            panic!(format!(
                "set_row could not find the table ({:?})\n    {:?}",
                tables, &row
            ));
        }
    }

    fn as_fk(&self, s: &str) -> String {
        return format!("{}{}", s, self.opts.column_id_postfix);
    }

    pub fn walk_props(
        &mut self,
        parents: Vec<String>,
        val: Value,
    ) -> Result<Option<(String, Value)>, err::CsvError> {
        match val {
            Value::Object(obj) => {
                self.create_table(parents.join("_"));
                let mut row_values = BTreeMap::new();
                for (key, val) in obj {
                    let mut p = parents.clone();
                    p.push(key);
                    if let Some((key, value)) = self.walk_props(p, val)? {
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
                    row_values.insert(
                        self.as_fk(&grand_parent_name),
                        Value::from(self.get_num_table_rows(&grand_parents)),
                    );
                }
                self.add_table_row(&parents, row_values)?;
                Ok(None)
            }
            Value::Array(arr) => {
                self.create_table(parents.join("_"));
                for val in arr {
                    let mut row_values = BTreeMap::new();
                    if let Some((key, value)) = self.walk_props(parents.clone(), val)? {
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
                            row_values.insert(
                                self.as_fk(&grand_parent_name),
                                Value::from(self.get_num_table_rows(&grand_parents)),
                            );
                        }
                        self.add_table_row(&parents, row_values)?;
                    }
                }
                Ok(None)
            }
            other_value => {
                // ignore parents when its a non container
                let key = &parents[parents.len() - 1];
                Ok(Some((key.to_owned(), other_value)))
            }
        }
    }

    pub fn process_file(mut self) -> std::result::Result<(), Box<dyn std::error::Error>> {
        let f = File::open(&self.opts.in_file)?;
        let f = BufReader::new(f);

        let mut num_lines_read = 0;

        for line in f.lines() {
            match serde_json::from_str(&line?) {
                Ok::<Value, _>(val) => {
                    //self.trav(0, None, vec![String::from(&self.opts.root_table_name)], val);
                    self.walk_props(vec![String::from(&self.opts.root_table_name)], val)?;
                    num_lines_read += 1;
                }
                Err(e) => {
                    println!("WARNING: {}, skipping this json string.", e);
                }
            }
            if let Some(json_buf_size) = self.opts.json_buf_size {
                if num_lines_read >= json_buf_size {
                    for (_, table) in &mut self.data {
                        table.flush_to_file(&self.opts)?;
                    }
                num_lines_read = 0;
                }
            }
        }

        for (_, table) in &mut self.data {
            table.flush_to_file(&self.opts)?;
        }


        Ok(())
    }
}
