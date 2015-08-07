extern crate docopt;

#[macro_use]
mod version;
mod export;

use docopt::Docopt;

static USAGE : &'static str = "
USAGE: tariff (-e | -i) <file>
       tariff (--help | --version)

Options:
  -e, --export    Exports database to file.
  -i, --import    Imports database from file.
  -h, --help      Show this message.
  -v, --version   Show the version of tariff.
";

fn main() {
  let argv = std::env::args();
  let args = Docopt::new(USAGE).and_then(|d| d.argv(argv).parse()).unwrap_or_else(|e| e.exit());

  if args.get_bool("--version") {
      println!("{}", version!());
  }

  let file = args.get_str("<file>");

  if args.get_bool("--export") {
      export::export(file);
  }
}
