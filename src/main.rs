extern crate bson;
extern crate docopt;
extern crate mongodb;
extern crate rustc_serialize;

#[macro_use] mod version;
mod client;

use docopt::Docopt;
use client::ImportExportClient;

static USAGE : &'static str = "
USAGE: tariff (-e | -i) <file> --db <db> --coll <coll>
       tariff (-x | -m) <file> --db <db>
       tariff (--help | --version)

Options:
  -e, --export       Exports collection to file.
  -i, --import       Imports collection from file.
  -d, --db           Which database to import/export.
  -c, --coll         Which collection to import/export.
  -x, --export-all   Exports all collection in database to file.
  -m, --import-all   Imports multiple collections from file.
  -h, --help         Show this message.
  -v, --version      Show the version of tariff.
";

fn main() {
  let argv = std::env::args();
  let args = Docopt::new(USAGE).and_then(|d| d.argv(argv).parse()).unwrap_or_else(|e| e.exit());

  if args.get_bool("--version") {
      println!("{}", version!());
      return;
  }

  let file = args.get_str("<file>");
  let mut client = match ImportExportClient::new() {
      Ok(client) => client,
      Err(e) => panic!("Error: {}", e)
  };

  let db = args.get_str("<db>");
  let coll = args.get_str("<coll>");

  if args.get_bool("--export") {
      if let Err(e) = client.export_collection(db, coll, file) {
          panic!("Error: {}", e);
      }
  }

  if args.get_bool("--export-all") {
      if let Err(e) = client.export_all(db, file) {
          panic!("Error: {}", e);
      }
  }

  if args.get_bool("--import") {
      if let Err(e) = client.import_collection(db, coll, file) {
          panic!("Error: {}", e)
      }
  }

  if args.get_bool("--import-all") {
      if let Err(e) = client.import_all(db, file) {
          panic!("Error: {}", e)
      }
  }
}
