use serde_json as json;
mod parts;

use clap::Clap;

use parts::*;

#[derive(Clap, Debug)]
#[clap(version = "1.0", author = "Yuri Titov <ytitov@gmail.com>")]
struct Opts {
    pub in_file: String,
    pub out_folder: String,
}

fn main() {
    let opts: Opts = Opts::parse();
    let mut s = Schema::new(&opts.in_file, &opts.out_folder);
    match s.process_file() {
        Ok(_) => {
        },
        Err(e) => {
            println!("Given: {:?}", &opts);
            println!("there was an error: {:?}", e);
        },
    }
}

/*
fn main_old() {
    let data = r#"
        {
            "name": "John Doe",
            "phones": [
                "+44 1234567",
                "+44 2345678"
            ],
            "features": {
                "onTime": "always", 
                "pretty": "nice",
                "complex": {
                    "kind": "special",
                    "value": 10
                }
            },
            "age": 43,
            "documents": {
                "rent": { "pages": [1,2,3,5,6] },
                "electricBill": { "pages": ["a", "b"] }
            }
        }"#;
    let data2 = r#"
        {
            "name": "Mary Jane",
            "extra_name": "MJ",
            "age": 25,
            "phones": [
                "402 444 1212"
            ],
            "features": {
                "onTime": "never", 
                "pretty": "cool"
            },
            "features_extra": {
                "extra": "air"
            },
            "documents": {
                "rent": { "pages": [7,8] },
                "electricBill": { "pages": ["f", "q"] }
            }
        }"#;
    //let val = json::to_value(v);
    let val = json::from_str(data).expect("Bad json");
    let val2 = json::from_str(data2).expect("Bad json");
    let mut s = Schema::new(".");
    s.trav(0, None, vec![String::from("ROOT")], val);
    //s.trav2(vec![String::from("ROOT")], val);
    //println!("\n\n{}", &s);
    s.trav(0, None, vec![String::from("ROOT")], val2);
    println!("\n\n{}", &s);
    s.export_csv();
}
*/
