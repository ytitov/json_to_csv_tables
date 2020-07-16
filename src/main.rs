mod parts;
use clap::Clap;


use parts::*;

fn main() {
    let opts: Opts = Opts::parse();
    println!("OPTS: {:?}", &opts);
    let s = Schema::new(opts.clone());
    if let Some(add_column_name) = &opts.add_column_name {
        match Table::load(&opts) {
            Ok(mut table) => {
                if table.columns.len() == 0 {
                    panic!("Ended up with zero columns, this is not good");
                }
                let col_idx = table.columns.len() - 1;
                table.columns.entry(add_column_name.to_owned()).or_insert(col_idx as u16);
                table.export_csv(&opts);
            }
            Err(e) => {
                println!("{}", e);
            }
        }
    } else {
        match s.process_file() {
            Ok(_) => {
            },
            Err(e) => {
                println!("Given: {:?}", &opts);
                println!("there was an error: {:?}", e);
            },
        }
    }
}

