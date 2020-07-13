Converts a json file into a relational schema as csv files.

To run it:
`cargo run -- test.json test` or to use cargo watch:
`cargo watch -x 'run -- test.json test' -w ./src`

current issues:
- [fixed][needs test] list of objects is giving wrong FK name, its one table too deep, works with plain non container values
- an array of polymorphic types might be handled in a weird way so, needs to be tested
