use std::fs::{self, OpenOptions};
use std::io::Write;

use bson::Bson;
use mongodb::{Client, ThreadedClient};
use mongodb::db::ThreadedDatabase;
use rustc_serialize::json::{Json, Object};
use tariff::client::ImportExportClient;

#[test]
fn import_collection() {
    let db_name = "tariff_test";
    let coll_name = "import_collection";
    let input_file = format!("{}.{}.tmp", db_name, coll_name);

    let mut file = OpenOptions::new()
                       .write(true)
                       .create(true)
                       .truncate(true)
                       .open(&input_file)
                       .ok().expect("Unable to open file to put test data into");

    for i in 1..5 {
        let mut obj = Object::new();
        obj.insert("_id".to_owned(), Json::I64(i as i64));
        obj.insert("_x".to_owned(), Json::I64(11 * i as i64));

        writeln!(file, "{}", Json::Object(obj)).ok().expect("Unable to write test data to file");
    }

    let tariff = ImportExportClient::new("localhost", 27017, false).unwrap();
    tariff.import_collection(db_name, coll_name, &input_file).unwrap();
    let _ = fs::remove_file(input_file);

    let client = Client::connect("localhost", 27017).ok().expect("Unable to connect to database to verify that the data was imported correctly");
    let db = client.db(db_name);
    db.drop_database().ok().expect("Unable to drop database in preparation for tests");
    let coll = db.collection(coll_name);

    let cursor = coll.find(None, None).ok().expect("Unable to query documents from database to verify that the data imported correctly");

    for (idx, result) in cursor.into_iter().enumerate() {
        let i = idx + 1;
        let doc = result.ok().expect("Unable to get document from cursor");

        match doc.get("_id") {
            Some(&Bson::I32(n)) => assert_eq!(n, i as i32),
            Some(&Bson::I64(n)) => assert_eq!(n, i as i64),
            _ => panic!("Invalid id for imported document in database")
        };

        match doc.get("x") {
            Some(&Bson::I32(n)) => assert_eq!(n, 11 * i as i32),
            Some(&Bson::I64(n)) => assert_eq!(n, 11 * i as i64),
            _ => panic!("Invalid `x` field for imported document in database")
        };
    }
}

#[test]
fn import_all() {
    let db_name = "tariff_test";

    let coll_names = vec![
        "import_all_coll1",
        "import_all_coll2",
        "import_all_coll3",
        "import_all_coll4",
    ];

    let len = coll_names.len();
    let input_file = format!("{}.import_all.tmp", db_name);

    let mut data = Object::new();

    for (i, coll_name) in coll_names.iter().enumerate() {
        let arr = (1..5).map(|j| {
            let id = (i * len + j) as i64;
            let mut obj = Object::new();
            obj.insert("_id".to_owned(), Json::I64(id));
            obj.insert("_x".to_owned(), Json::I64(11 * id));
            Json::Object(obj)
        }).collect();

        data.insert(coll_name.clone().to_owned(), Json::Array(arr));
    }

    let mut file = OpenOptions::new()
                       .write(true)
                       .create(true)
                       .truncate(true)
                       .open(&input_file)
                       .ok().expect("Unable to open file to put test data into");

    writeln!(file, "{}", Json::Object(data)).ok().expect("Unable to write test data to file");

    let tariff = ImportExportClient::new("localhost", 27017, false).unwrap();
    tariff.import_all(db_name, &input_file).unwrap();
    let _ = fs::remove_file(input_file);

    let client = Client::connect("localhost", 27017).ok().expect("Unable to connect to database to verify that the data was imported correctly");
    let db = client.db(db_name);
    db.drop_database().ok().expect("Unable to drop database in preparation for tests");

    for (i, coll_name) in coll_names.into_iter().enumerate() {
        let coll = db.collection(coll_name);
        let cursor = coll.find(None, None).ok().expect("Unable to query documents from database to verify that the data imported correctly");

        for (j, result) in cursor.into_iter().enumerate() {
            let id = i * len + j + 1;

            let doc = result.ok().expect("Unable to get document from cursor");

            match doc.get("_id") {
                Some(&Bson::I32(n)) => assert_eq!(n, id as i32),
                Some(&Bson::I64(n)) => assert_eq!(n, id as i64),
                _ => panic!("Invalid id for imported document in database")
            };

            match doc.get("x") {
                Some(&Bson::I32(n)) => assert_eq!(n, 11 * id as i32),
                Some(&Bson::I64(n)) => assert_eq!(n, 11 * id as i64),
                _ => panic!("Invalid `x` field for imported document in database")
            };
        }
    }
}
