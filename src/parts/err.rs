use std::fmt;

#[derive(Debug, Clone)]
pub enum CsvError {
    MissingColumn(String),
    CouldNotOpen(String),
    CouldNotCreate(String),
    CouldNotWrite(String),
    CouldNotFindFile(String), 
}

impl fmt::Display for CsvError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        //write!(f, "invalid first item to double")
        use CsvError::*;
        match self {
            MissingColumn(s) => write!(f, "Missing a column with name of: {}", &s),
            CouldNotOpen(s) => write!(f, "Could not open file for {}", &s),
            CouldNotCreate(s) => write!(f, "Could not create file for {}", &s),
            CouldNotWrite(s) => write!(f, "Could not write file for {}", &s),
            CouldNotFindFile(s) => write!(f, "Could not find file for {}", &s),
        }
    }
}

impl std::error::Error for CsvError {
}
