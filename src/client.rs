use std::fs::{File, OpenOptions};
use std::fmt::Write as FmtWrite;
use std::io::{BufRead, BufReader, Write};

use std::error::Error;

use bson::{Bson, Document};
use mongodb::{Client, ClientOptions, ThreadedClient};
use mongodb::common::{ReadMode, ReadPreference};
use mongodb::cursor::Cursor;
use mongodb::db::ThreadedDatabase;
use rustc_serialize::json::{Json, Object};

pub struct ImportExportClient {
    client: Client,
}

impl ImportExportClient {
    /// Create a new client for importing/exporting MongoDB data
    ///
    /// # Arguments
    ///
    /// `host` - The host to connect to.
    /// `port` - The port to connect to.
    ///
    /// # Return value
    ///
    /// Returns a new client if the client successfully connects to the database,
    /// or an error string on failure.
    pub fn new<'a>(host: &str, port: u16, secondary: bool) -> Result<Self, &'a str> {
        let client_result = if secondary {
            let mut options = ClientOptions::new();
            let preference = ReadPreference::new(ReadMode::Secondary, None);
            options.read_preference = Some(preference);
            let uri = format!("mongodb://{}:{}", host, port);
            println!("Trying to connect to secondary...");
            Client::with_uri_and_options(&uri, options)
        } else {
            Client::connect(host, port)
        };

        match client_result {
            Ok(client) => Ok(ImportExportClient { client: client }),
            Err(_) => Err("Unable to connect to database.")
        }
    }

    /// Export a collection in the database to a JSON file.
    ///
    /// # Arguments
    ///
    /// `db_name` - The name of the database to export from.
    /// `coll_name` - The name of the collection to export.
    /// `out` - The name of the file to store the outputted data.
    ///
    /// # Return value
    ///
    /// Returns nothing on success, or an error string on failure.
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

    /// Export all collections in the database to a JSON file.
    ///
    /// # Arguments
    ///
    /// `db_name` - The name of the database to export.
    /// `out` - The name of the file to store the outputted data.
    ///
    /// # Return value
    ///
    /// Returns nothing on success, or an error string on failure.
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

    /// Import a collection to the database from a JSON file.
    ///
    /// # Arguments
    ///
    /// `db_name` - The name of the database to import into.
    /// `coll_name` - The name of the collection to import into.
    /// `input` - The name of the file containing the data to import.
    ///
    /// # Return value
    ///
    /// Returns nothing on success, or an error string on failure.
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

    /// Import a set of collections to the database from a JSON file.
    ///
    /// # Arguments
    ///
    /// `db_name` - The name of the database to import into.
    /// `input` - The name of the file containing the data to import.
    ///
    /// # Return value
    ///
    /// Returns nothing on success, or an error string on failure.
    pub fn import_collection(&self, db_name: &str, coll_name: &str, input: &str) -> Result<(), &str> {
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
            match coll.insert_many(chunk.to_owned(), None) {
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
