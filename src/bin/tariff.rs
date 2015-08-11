extern crate docopt;
extern crate tariff;

use std::str::FromStr;

use docopt::Docopt;
use tariff::client::ImportExportClient;

static USAGE : &'static str = "
USAGE: tariff (-e | -i) <file> --db <db> --coll <coll> [--port <port>]
       tariff (-x | -m) <file> --db <db> [--port <port>]
       tariff (--help | --version)

Options:
  -e, --export       Exports collection to file.
  -i, --import       Imports collection from file.
  -d, --db           Which database to import/export.
  -c, --coll         Which collection to import/export.
  -x, --export-all   Exports all collection in database to file.
  -m, --import-all   Imports multiple collections from file.
  -p, --port         Which port to connect to the database on.
  -h, --help         Show this message.
  -v, --version      Show the version of tariff.
";

macro_rules! version {
    () => {
        format!("{}.{}.{}{}",
            env!("CARGO_PKG_VERSION_MAJOR"),
            env!("CARGO_PKG_VERSION_MINOR"),
            env!("CARGO_PKG_VERSION_PATCH"),
            option_env!("CARGO_PKG_VERSION_PRE").unwrap_or(""))
    }
}

fn main() {
  let argv = std::env::args();
  let args = Docopt::new(USAGE).and_then(|d| d.argv(argv).parse()).unwrap_or_else(|e| e.exit());

  if args.get_bool("--version") {
      println!("{}", version!());
      return;
  }

  let file = args.get_str("<file>");
  let port = u16::from_str(args.get_str("<port>")).unwrap_or(27017);
  let mut client = match ImportExportClient::new("localhost", port) {
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
