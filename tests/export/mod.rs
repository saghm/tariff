use std::fs::{self, File};
use std::io::{BufRead, BufReader};

use mongodb::{Client, ThreadedClient};
use mongodb::db::ThreadedDatabase;
use rustc_serialize::json::Json;
use tariff::client::ImportExportClient;

#[test]
fn export_collection() {
    let db_name = "tariff_test";
    let coll_name = "export_collection";
    let output_file = format!("{}.{}.tmp", db_name, coll_name);

    let client = Client::connect("localhost", 27017).ok().expect("Unable to connect to database to input test data");
    let db = client.db(db_name);
    db.drop_database().ok().expect("Unable to drop database in preparation for tests");
    let coll = db.collection(coll_name);

    let docs = (1..5).map(|i| doc! { "_id" => i, "x" => (i * 11) }).collect();
    coll.insert_many(docs, None).ok().expect("Unable to insert documents into collection for testing");

    let mut tariff = ImportExportClient::new("localhost", 27017, false).ok().unwrap();
    tariff.export_collection(db_name, coll_name, &output_file).unwrap();

    let file = File::open(&output_file).ok().expect("Unable to open generated output file");
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
            _ => panic!("Invalid `x` field for object in generated output file")
        };
    }

    let _ = fs::remove_file(&output_file);
}

#[test]
fn export_all() {
    let db_name = "tariff_test";

    let coll_names = vec![
        "export_all_coll1",
        "export_all_coll2",
        "export_all_coll3",
        "export_all_coll4",
    ];

    let len = coll_names.len();
    let output_file = format!("{}.export_all.tmp", db_name);

    let client = Client::connect("localhost", 27017).ok().expect("Unable to connect to database to input sample data");
    let db = client.db(db_name);
    db.drop_database().ok().expect("Unable to drop database in preparation for tests");

    for (i, coll_name) in coll_names.iter().enumerate() {
        let docs = (1..5).map(|j| {
            let id = (i * len + j) as i32;
            doc! { "_id" => id, "x" => (11 * id) }
        }).collect();

        let coll = db.collection(coll_name);
        coll.insert_many(docs, None).ok().expect("Unable to insert documents into collection for testing");
    }

    let tariff = ImportExportClient::new("localhost", 27017, false).ok().unwrap();
    tariff.export_all(db_name, &output_file).unwrap();

    let mut file = File::open(&output_file).ok().expect("Unable to open generated output file");

    let obj = match Json::from_reader(&mut file) {
        Ok(Json::Object(obj)) => obj,
        _ => panic!("Invalid top-level JSON object in generated output file")
    };

    for (i, coll_name) in coll_names.into_iter().enumerate() {
        let arr = match obj.get(coll_name) {
            Some(&Json::Array(ref arr)) => arr.clone(),
            _ => panic!("The value for a column in an exported database should be an array")
        };

        for (j, json) in arr.into_iter().enumerate() {
            let object = match json {
                Json::Object(obj) => obj,
                _ => panic!("Each value in the array for a column in an exported database should be an object")
            };

            let id = i * len + j + 1;

            match object.get("_id") {
                Some(&Json::I64(n)) => assert_eq!(n, id as i64),
                Some(&Json::U64(n)) => assert_eq!(n, id as u64),
                _ => panic!("Invalid id for object in generated output file")
            };

            match object.get("x") {
                Some(&Json::I64(n)) => assert_eq!(n, 11 * id as i64),
                Some(&Json::U64(n)) => assert_eq!(n, 11 * id as u64),
                _ => panic!("Invalid id for object in generated output file")
            };
        }
    }

    let _ = fs::remove_file(&output_file);
}
