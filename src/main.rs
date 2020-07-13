use serde_json as json;
mod parts;

use parts::*;

fn main() {
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
            "age": 25,
            "documents": {
                "rent": { "pages": [7,8] },
                "electricBill": { "pages": ["f", "q"] }
            }
        }"#;
    let v: Element<String> = Element {
        parent: None,
        fk: None,
        pk: 1,
        key: Some(String::from("sample")),
        value: String::from("example"),
    };
    //let val = json::to_value(v);
    let val = json::from_str(data).expect("Bad json");
    //let val2 = json::from_str(data2).expect("Bad json");
    let mut s = Schema::new();
    s.trav(0, None, vec![String::from("ROOT")], val);
    //println!("\n\n{}", &s);
    //s.trav(0, None, vec![String::from("ROOT")], val2);
    println!("\n\n{}", &s);
}
