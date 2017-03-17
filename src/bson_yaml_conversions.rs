use bson::{Bson, Document};
use hex::ToHex;
use serde_yaml::{Mapping, Value};

macro_rules! yaml_string {
    ($string:expr) => (Value::String($string.to_string()))
}

pub fn bson_document_to_yaml(doc: Document) -> Value {
    Value::Mapping(doc.into_iter()
                       .map(|(k, v)| (Value::String(k), bson_to_yaml(v)))
                       .collect())
}

pub fn bson_to_yaml(bson: Bson) -> Value {
    match bson {
        Bson::FloatingPoint(f) => Value::F64(f),
        Bson::Array(array) => Value::Sequence(array.into_iter().map(bson_to_yaml).collect()),
        Bson::String(s) => Value::String(s),
        Bson::Document(doc) => bson_document_to_yaml(doc),
        Bson::Boolean(b) => Value::Bool(b),
        Bson::Null => Value::Null,
        Bson::RegExp(pattern, options) => {
            let mut mapping = Mapping::new();
            mapping.insert(yaml_string!("$regex"), Value::String(pattern));
            mapping.insert(yaml_string!("$options"), Value::String(options));

            Value::Mapping(mapping)
        }
        Bson::JavaScriptCode(code) => {
            let mut mapping = Mapping::new();
            mapping.insert(yaml_string!("$code"), Value::String(code));

            Value::Mapping(mapping)
        }
        Bson::JavaScriptCodeWithScope(code, scope) => {
            let mut mapping = Mapping::new();
            mapping.insert(yaml_string!("$code"), Value::String(code));
            mapping.insert(yaml_string!("$scope"), bson_document_to_yaml(scope));

            Value::Mapping(mapping)
        }
        Bson::I32(i) => Value::I64(i as i64),
        Bson::I64(i) => Value::I64(i),
        Bson::TimeStamp(i) => {
            let time = i >> 32;
            let inc = i & 0x0000FFFF;

            let mut inner = Mapping::new();
            inner.insert(yaml_string!("t"), Value::I64(time));
            inner.insert(yaml_string!("i"), Value::I64(inc));

            let mut outer = Mapping::new();
            outer.insert(yaml_string!("$timestamp"), Value::Mapping(inner));

            Value::Mapping(outer)
        }
        Bson::Binary(t, v) => {
            let tval: u8 = t.into();

            let mut mapping = Mapping::new();
            mapping.insert(yaml_string!("$type"), Value::I64(tval as i64));
            mapping.insert(yaml_string!("$binary"), Value::String(v.to_hex()));

            Value::Mapping(mapping)
        }
        Bson::ObjectId(id) => {
            let mut mapping = Mapping::new();
            mapping.insert(yaml_string!("$oid"), Value::String(id.to_string()));

            Value::Mapping(mapping)
        }
        Bson::UtcDatetime(datetime) => {
            let mut mapping = Mapping::new();
            mapping.insert(yaml_string!("$date"), Value::String(datetime.to_rfc3339()));

            Value::Mapping(mapping)
        }
        Bson::Symbol(_) => unimplemented!(),
    }
}

