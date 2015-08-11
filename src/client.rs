use std::fs::{File, OpenOptions};
use std::fmt::Write as FmtWrite;
use std::io::{BufRead, BufReader, Write};

use std::error::Error;

use bson::{Bson, Document};
use mongodb::{Client, ThreadedClient};
use mongodb::cursor::Cursor;
use mongodb::db::ThreadedDatabase;
use rustc_serialize::json::{Json, Object};

pub struct ImportExportClient {
    client: Client,
}

impl ImportExportClient {
    pub fn new<'a>() -> Result<Self, &'a str> {
        match Client::connect("localhost", 27017) {
            Ok(client) => Ok(ImportExportClient { client: client }),
            Err(_) => Err("Unable to connect to database.")
        }
    }

    pub fn export_collection(&mut self, db_name: &str, coll_name: &str, out: &str) -> Result<(), &str> {
        let mut out = match OpenOptions::new().write(true).create(true).truncate(true).open(out) {
            Ok(file) => file,
            Err(_) => return Err("Unable to open file.")
        };

        let cursor = try!(self.get_collection(db_name, coll_name));

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

    pub fn export_all(&self, db_name: &str, out: &str) -> Result<(), &str> {
        let colls = try!(self.get_collection_names(db_name));
        let mut object = Object::new();

        for coll in colls {
            let cursor = try!(self.get_collection(db_name, &coll));
            let mut jsons = vec![];

            for result in cursor {
                let doc = match result {
                    Ok(doc) => doc,
                    Err(_) => return Err("Unable to read document from database")
                };

                jsons.push(Bson::Document(doc).to_json());
            }

            object.insert(coll, Json::Array(jsons));
        }

        let mut out = match OpenOptions::new().write(true).create(true).truncate(true).open(out) {
            Ok(file) => file,
            Err(_) => return Err("Unable to open file.")
        };

        match writeln!(out, "{}", Json::Object(object)) {
            Ok(_) => Ok(()),
            Err(_) => return Err("Unable to write document to file.")
        }
    }

    fn get_collection_names(&self, db_name: &str) -> Result<Vec<String>, &str> {
        let db = self.client.db(db_name);

        let cursor = match db.list_collections(None) {
            Ok(cursor) => cursor,
            Err(_) => return Err("Unable to get list of collections")
        };

        let mut collections = vec![];

        for result in cursor {
            let doc = match result {
                Ok(doc) => doc,
                Err(_) => return Err("Error getting collection document")
            };

            match doc.get("name") {
                Some(&Bson::String(ref s)) => collections.push(s.to_owned()),
                _ => return Err("Invalid collection document returned")
            };
        }

        Ok(collections)
    }

    fn get_collection(&self, db_name: &str, coll_name: &str) -> Result<Cursor, &str> {
        let db = self.client.db(db_name);
        let coll = db.collection(coll_name);

        match coll.find(None, None) {
            Ok(docs) => Ok(docs),
            Err(_) => Err("Unable to query database.")
        }
    }

    pub fn import_all(&self, db_name: &str, input: &str) -> Result<(), &str> {
        let mut file = match File::open(input) {
            Ok(file) => file,
            Err(_) => return Err("Unable to open file")
        };

        let obj = match Json::from_reader(&mut file) {
            Ok(Json::Object(obj)) => obj,
            _ => return Err("Invalid top-level JSON object in file")
        };

        for (coll_name, json) in obj {
            let mut docs = vec![];

            let array = match json {
                Json::Array(array) => array,
                _ => return Err("Invalid JSON array as value of top-level object")
            };

            for json in array {
                match json {
                    Json::Object(obj) => {
                        let mut doc = Document::new();

                        for (key, value) in obj {
                            doc.insert(key, Bson::from_json(&value));
                        }

                        docs.push(doc);
                    },
                    _ => return Err("Invalid JSON object in collection array")
                };
            }

            println!("{}.{}", db_name, coll_name);
            try!(self.import_documents(db_name, &coll_name, docs));
        }

        Ok(())
    }

    pub fn import_collection(&self, db_name: &str, coll_name: &str, input: &str) -> Result<(), &str>{
        let file = match File::open(input) {
            Ok(file) => file,
            Err(_) => return Err("Unable to open file")
        };

        let reader = BufReader::new(file);
        let mut docs = vec![];

        for result in reader.lines() {
            let line = match result {
                Ok(line) => line,
                Err(_) => return Err("Unable to read document from file")
            };

            match Json::from_str(&line) {
                Ok(Json::Object(obj)) => {
                    let mut doc = Document::new();

                    for (key, value) in obj {
                        doc.insert(key, Bson::from_json(&value));
                    }

                    docs.push(doc);
                }
                _ => return Err("Invalid JSON object in file")
            };
        }

        self.import_documents(db_name, coll_name, docs)
    }

    fn import_documents(&self, db_name: &str, coll_name: &str, docs: Vec<Document>) -> Result<(), &str> {
        let db = self.client.db(db_name);
        let coll = db.collection(coll_name);
        let chunk_size = if coll_name.eq("system.indexes") {
            1
        } else {
            1000
        };

        for chunk in docs.chunks(chunk_size) {
            match coll.insert_many(chunk.to_owned(), false, None) {
                Ok(_) => (),
                Err(e) => {
                    println!("{}", e.description());
                    return Err("Unable to insert documents")
                }
            };
        }

        Ok(())
    }
}