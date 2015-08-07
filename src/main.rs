extern crate bson;
extern crate docopt;
extern crate mongodb;
extern crate rustc_serialize;

#[macro_use] mod version;
mod export;

use docopt::Docopt;
use export::Exporter;

static USAGE : &'static str = "
USAGE: tariff (-e | -i) <file> --db <db> --coll <coll>
       tariff (--help | --version)

Options:
  -e, --export    Exports database to file.
  -i, --import    Imports database from file.
  -d, --db        Which database to import/export.
  -c, --coll      Which collection to import/export.
  -h, --help      Show this message.
  -v, --version   Show the version of tariff.
";

fn main() {
  let argv = std::env::args();
  let args = Docopt::new(USAGE).and_then(|d| d.argv(argv).parse()).unwrap_or_else(|e| e.exit());

  if args.get_bool("--version") {
      println!("{}", version!());
      return;
  }

  let file = args.get_str("<file>");
  let mut exporter = match Exporter::new() {
      Ok(exporter) => exporter,
      Err(e) => panic!("Error: {}", e)
  };

  let db = args.get_str("<db>");
  let coll = args.get_str("<coll>");

  if args.get_bool("--export") {
      if let Err(e) = exporter.dump(db, coll, file) {
          panic!("Error: {}", e);
      }
  }
}
