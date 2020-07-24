use super::*;

#[derive(Debug)]
pub struct Table {
    pub name: String,
    pub columns: BTreeMap<String, u16>,
    pub rows: BTreeMap<usize, BTreeMap<String, Value>>,
    pub col_str_max_lenghts: BTreeMap<String, usize>,
    pub row_offset: usize,
    /// are we appending to an existing CSV file and columns should be prespecified
    pub appending_mode: bool,
    pub opts: Opts,
}

impl Table {
    pub fn new(name: &str, opts: &Opts) -> Self {
        let fname = format!("{}/{}.csv", &opts.out_folder, name);
        let mut appending_mode = false;
        let mut columns;
        let row_offset;
        let csv_info = get_csv_file_info(&fname);
        let mut num = csv_info.lines_in_file;
        columns = csv_info.columns;
        if num > 0 {
            num -= 1;
            appending_mode = true;
        }
        row_offset = num;
        columns.insert(format!("{}{}", name, &opts.column_id_postfix), 0);
        Table {
            name: name.to_owned(),
            columns,
            rows: BTreeMap::new(),
            col_str_max_lenghts: BTreeMap::new(),
            row_offset,
            appending_mode,
            opts: opts.clone(),
        }
    }

    pub fn load(opts: &Opts) -> Result<Self, Box<dyn std::error::Error>> {
        let file_info = get_csv_file_info(&opts.in_file);
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
                            let col_name = idx_to_name.get(&(idx as u16)).unwrap_or(Err(
                                err::CsvError::MissingColumn(format!(
                                    "File has more columns in data than in header, index: {}",
                                    idx
                                )),
                            )?);
                            if let Ok(value) = serde_json::from_str(value) {
                                row.insert(col_name.to_owned(), value);
                            }
                        }
                        rows.insert(rows.len(), row);
                    }
                    idx += 1;
                }
                Err(e) => {
                    println!("Reached end of line: {}", e);
                }
            }
        }
        Ok(Table {
            name: opts.root_table_name.clone(),
            columns,
            rows,
            row_offset: 0,
            appending_mode: false,
            col_str_max_lenghts: BTreeMap::new(),
            opts: opts.clone(),
        })
    }

    pub fn get_pk_name(&self) -> String {
        return format!("{}{}", self.name, &self.opts.column_id_postfix);
    }

    pub fn add_row(&mut self, mut row: BTreeMap<String, Value>) -> Result<(), err::CsvError> {
        for (key, val) in &row {
            if self.appending_mode == true && !self.columns.contains_key(key) {
                return Err(err::CsvError::MissingColumn(key.to_owned()));
            }
            let num_cols = self.columns.len() as u16;
            match val {
                Value::String(s) => {
                    if let Some(cur_max) = self.col_str_max_lenghts.get(key) {
                        if *cur_max < s.len() {
                            self.col_str_max_lenghts.insert(String::from(key), s.len());
                        }
                    } else {
                        self.col_str_max_lenghts.insert(String::from(key), s.len());
                    }
                }
                _ => {}
            }
            self.columns.entry(key.to_owned()).or_insert(num_cols);
        }
        let pk_idx = self.rows.len() + self.row_offset;
        row.entry(self.get_pk_name()).or_insert(Value::from(pk_idx));
        //println!("--> add_row {} - {:?}", pk_idx, &row);
        self.rows.insert(pk_idx, row);
        Ok(())
    }

    fn columns_as_str(&self) -> String {
        let mut columns_vec = Vec::new();
        for (key, _val) in &self.columns {
            columns_vec.push(String::from(key));
        }
        let columns_str = columns_vec.join(",");
        columns_str
    }

    pub fn export_csv(mut self, opts: &Opts) -> Result<(), err::CsvError> {
        use std::io::prelude::*;
        use std::path::Path;
        let fname = format!("{}/{}.csv", &opts.out_folder, &self.name);
        let path = Path::new(&fname);
        let display = path.display();

        let col_idx = self.columns.len();
        self.columns
            .entry(self.get_pk_name())
            .or_insert(col_idx as u16);
        println!(
            "    export_csv num_rows: {} - {:?}",
            &self.name,
            &self.rows.len()
        );

        let mut file = find_or_create_file(&fname)?;

        let total_lines = get_csv_file_info(&fname).lines_in_file;

        // only add columns if needed
        if total_lines == 0 {
            let columns_str = self.columns_as_str();
            match file.write_all(columns_str.as_bytes()) {
                Err(why) => {
                    return Err(err::CsvError::CouldNotWrite(format!(
                        "{}, because {}",
                        display, why
                    )))
                }
                Ok(_) => (),
            }
        }

        for (_, row) in self.rows {
            let mut row_vec = Vec::new();
            for (col, _) in &self.columns {
                if let Some(val) = row.get(col) {
                    row_vec.push(format!("{}", val));
                } else {
                    row_vec.push(format!(""));
                }
            }
            let line = format!("\n{}", row_vec.join(","));
            match file.write_all(line.as_bytes()) {
                Err(why) => {
                    return Err(err::CsvError::CouldNotWrite(format!(
                        "{}, because {}",
                        display, why
                    )))
                }
                Ok(_) => (),
            }
        }
        println!("successfully wrote to {}", display);
        Ok(())
    }

    fn create_table_sql(&self, _opts: &Opts) -> Result<String, err::CsvError> {
        let mut s = format!("CREATE TABLE IF NOT EXISTS {} (", self.name);
        let mut col_strings = Vec::with_capacity(self.columns.len());
        if let Some(first_row) = self.rows.iter().next() {
            for (col, _) in &self.columns {
                if let Some(row_value) = first_row.1.get(col) {
                    let col_name_str = match &row_value {
                        Value::Number(_) => {
                            format!("`{}` BIGINT", col) 
                        }
                        Value::String(_) => {
                            let str_len: usize = if let Some(v) = self.col_str_max_lenghts.get(col) {
                                *v
                            } else {
                                100
                            };
                            format!("`{}` VARCHAR({})", col, str_len) 
                        }
                        Value::Bool(_) => {
                            format!("`{}` BOOL", col) 
                        }
                        _ => {
                            panic!("While creating SQL table creation statement, ran into an unsupported datatype")
                        }
                    };
                    col_strings.push(col_name_str);
                }
            }
            s.push_str(&format!("{})", col_strings.join(",")));
        }
        Ok(s)
    }

    fn create_insert_row_statement(
        &self,
        row: &BTreeMap<String, Value>,
    ) -> Result<String, err::CsvError> {
        let cols_str: Vec<String> = self.columns.keys().cloned().map( | cname | format!("`{}`", cname)).collect();
        let mut vals = Vec::new();
        for (col, _) in &self.columns {
            if let Some(val) = row.get(col) {
                vals.push(format!("{}", val));
            } else {
                vals.push(format!("NULL"));
            }
        }
        let s = format!("INSERT INTO `{}` ({}) VALUES ({})", self.name, cols_str.join(", "), vals.join(","));
        Ok(s)
    }

    /// writes what is currently in the buffer, updates rows_offset and clears the rows
    pub fn flush_to_file(&mut self, opts: &Opts) -> Result<(), err::CsvError> {
        let fname = format!("{}/{}.csv", &opts.out_folder, &self.name);
        let csv_info = get_csv_file_info(&fname);
        let mut file = find_or_create_file(&fname)?;
        if csv_info.lines_in_file == 0 {
            //if self.appending_mode == false {
            let columns_str = self.columns_as_str();
            println!("flush_to_file columns: {:?}", &columns_str);
            match file.write_all(columns_str.as_bytes()) {
                Err(why) => return Err(err::CsvError::CouldNotWrite(format!("because {}", why))),
                Ok(_) => (),
            }
        }

        let mut sql_file = None;

        // handle sql output
        if opts.as_mysql {
            let mut sql = self.create_table_sql(opts)?;
            sql.push_str(";\n");
            //println!("SQL: {}", &sql);
            sql_file = Some(find_or_create_file(&format!(
                "{}/schema-{}-{}.sql",
                &opts.out_folder, self.name, self.row_offset
            ))?);
                if let Some(sql_file) = &mut sql_file {
            match sql_file.write_all(sql.as_bytes()) {
                Err(why) => return Err(err::CsvError::CouldNotWrite(format!("because {}", why))),
                Ok(_) => (),
            }
                }
        }

        let mut num_rows_added = 0;
        for (_, row) in &self.rows {
            if opts.as_mysql {
                let mut row_str = self.create_insert_row_statement(row)?;
                row_str.push_str(";\n");
                //println!("ROW: {}", &row_str);
                if let Some(sql_file) = &mut sql_file {
                match sql_file.write_all(row_str.as_bytes()) {
                    Err(why) => {
                        return Err(err::CsvError::CouldNotWrite(format!("because {}", why)))
                    }
                    Ok(_) => (),
                }
                }
            }
            let mut row_vec = Vec::new();
            for (col, _) in &self.columns {
                if let Some(val) = row.get(col) {
                    row_vec.push(format!("{}", val));
                } else {
                    row_vec.push(format!(""));
                }
            }
            let line = format!("\n{}", row_vec.join(","));
            num_rows_added += 1;
            match file.write_all(line.as_bytes()) {
                Err(why) => {
                    return Err(err::CsvError::CouldNotWrite(format!("because {}", why)))
                }
                Ok(_) => (),
            }
        }

        self.row_offset += num_rows_added;
        //println!("    row offset: {} - {:?}", &self.name, self.row_offset);
        println!(
            "[{}] - flushed {} lines.  Total: {}",
            self.name, num_rows_added, self.row_offset
        );
        self.rows = BTreeMap::new();

        Ok(())
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
