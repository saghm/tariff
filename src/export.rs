use std::fs::OpenOptions;
use std::fmt::Write as FmtWrite;
use std::io::Write;

use bson::Bson;
use mongodb::{Client, ThreadedClient};
use mongodb::db::ThreadedDatabase;

pub struct Exporter {
    client: Client,
}

impl Exporter {
    pub fn new<'a>() -> Result<Exporter, &'a str> {
        match Client::connect("localhost", 27017) {
            Ok(client) => Ok(Exporter { client: client }),
            Err(_) => Err("Unable to connect to database.")
        }
    }

    pub fn dump(&mut self, db: &str, coll: &str, out: &str) -> Result<(), &str> {
        let mut out = match OpenOptions::new().write(true).create(true).truncate(true).open(out) {
            Ok(file) => file,
            Err(_) => return Err("Unable to open file.")
        };

        let db = self.client.db(db);
        let coll = db.collection(coll);

        let cursor = match coll.find(None, None) {
            Ok(docs) => docs,
            Err(_) => return Err("Unable to query database.")
        };

        for result in cursor {
            let doc = match result {
                Ok(doc) => doc,
                Err(_) => return Err("Unable to read document from database")
            };

            let json = Bson::Document(doc).to_json();

            match writeln!(out, "{}", json) {
                Ok(_) => (),
                Err(_) => return Err("Unable to write document to file.")
            };
        }

        Ok(())
    }
}
