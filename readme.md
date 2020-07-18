Converts a json file into a relational schema as csv files.

To run it:
`cargo run -- test.json test` or to use cargo watch:
`cargo watch -x 'run -- test.json test' -w ./src`

current issues:
- when using the buffering feature, columns which did not get imported on the first `--json-buf-size` writes will get silently dropped if they appear after that initial buffer slice
