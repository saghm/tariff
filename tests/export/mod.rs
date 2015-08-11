use std::fs::{self, File};
use std::io::{BufRead, BufReader};

use rustc_serialize::json::Json;
use mongodb::{Client, ThreadedClient};
use mongodb::db::ThreadedDatabase;
use tariff::client::ImportExportClient;

#[test]
fn export_collection() {
    let db_name = "tariff_test";
    let coll_name = "export_collection";
    let output_file = format!("{}.{}.tmp", db_name, coll_name);

    let client = Client::connect("localhost", 27017).unwrap();
    let db = client.db(db_name);
    db.drop_database().ok().expect("Unable to drop database in preparation for tests");
    let coll = db.collection(coll_name);

    let docs = (1..5).map(|i| doc! { "_id" => i, "x" => (i * 11) }).collect();
    coll.insert_many(docs, None).ok().expect("Unable to insert documents into collection for testing");

    let mut tariff = ImportExportClient::new().ok().unwrap();
    tariff.export_collection(db_name, coll_name, &output_file).unwrap();

    let file = match File::open(&output_file) {
        Ok(file) => file,
        Err(_) => panic!("Unable to generated output file")
    };

    let reader = BufReader::new(file);

    for (idx, result) in reader.lines().enumerate() {
        let i = idx + 1;
        let line = result.ok().expect("Unable to read document from generated output file");

        let object = match Json::from_str(&line) {
            Ok(Json::Object(obj)) => obj,
            _ => panic!("Invalid JSON object in generated output file")
        };

        match object.get("_id") {
            Some(&Json::I64(n)) => assert_eq!(n, i as i64),
            Some(&Json::U64(n)) => assert_eq!(n, i as u64),
            _ => panic!("Invalid id for object in generated output file")
        };

        match object.get("x") {
            Some(&Json::I64(n)) => assert_eq!(n, 11 * i as i64),
            Some(&Json::U64(n)) => assert_eq!(n, 11 * i as u64),
            _ => panic!("Invalid id for object in generated output file")
        };
    }

    let _ = fs::remove_file(&output_file);
}

// #[test]
// fn export_all() {
//     let obj = match Json::from_reader(&mut file) {
//         Ok(Json::Object(obj)) => obj,
//         _ => panic!("Invalid top-level JSON object in generated output file")
//     };
//
//     for (i, (coll_name, json)) in obj.into_iter().enumerate() {
//         assert_eq!()
//     }
//
// }
