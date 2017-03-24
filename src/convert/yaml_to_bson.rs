use bson::{Bson, Document};
use bson::oid::ObjectId;
use chrono::{DateTime, UTC};
use hex::{FromHex, ToHex};
use serde_yaml::{Mapping, Sequence, Value};

use error::{self, Error};

macro_rules! try_string {
    ($value:expr) => { match $value {
        Value::String(s) => s,
        _ => {
            return Err(
                Error::coversion_error("Unable to convert YAML mapping with non-string keys"));
        }
    }}
}

fn convert_javascript_code(value: Value) -> Result<Value, Bson> {
    match value {
        Value::String(s) => Err(Bson::JavaScriptCode(s)),
        other => Ok(other),
    }
}

fn convert_timestamp(value: Value) -> Result<Value, Bson> {
    let mapping = match value {
        Value::Mapping(m) => m,
        other => return Ok(other),
    };

    if mapping.len() != 2 {
        return Ok(Value::Mapping(mapping));
    }

    let t = match mapping.get(&yaml_string!("t")) {
        Some(&Value::I64(i)) => i,
        _ => return Ok(Value::Mapping(mapping)),
    };

    let v = match mapping.get(&yaml_string!("v")) {
        Some(&Value::I64(i)) => i,
        _ => return Ok(Value::Mapping(mapping)),
    };

    let timestamp = (t << 32) | v;
    Err(Bson::TimeStamp(timestamp))
}

fn convert_oid(value: Value) -> Result<Value, Bson> {
    let oid_string = match value {
        Value::String(s) => s,
        other => return Ok(other),
    };

    match ObjectId::with_string(&oid_string) {
        Ok(oid) => Err(Bson::ObjectId(oid)),
        Err(_) => Ok(Value::String(oid_string)),
    }
}

fn convert_datetime(value: Value) -> Result<Value, Bson> {
    let datetime_string = match value {
        Value::String(s) => s,
        other => return Ok(other),
    };

    match DateTime::parse_from_rfc3339(&datetime_string) {
        Ok(datetime) => Err(Bson::UtcDatetime(datetime.with_timezone(&UTC))),
        Err(_) => Ok(Value::String(datetime_string)),
    }
}

fn convert_single_pair_mapping(key: String, mut value: Value) -> Result<Mapping, Bson> {
    if key == "$code" {
        value = convert_javascript_code(value)?;
    } else if key == "$timestamp" {
        value = convert_timestamp(value)?;
    } else if key == "$oid" {
        value = convert_oid(value)?;
    } else if key == "$date" {
        value = convert_datetime(value)?;
    }

    let mut mapping = Mapping::new();
    mapping.insert(Value::String(key), value);

    Ok(mapping)
}

fn convert_regex(v1: Value, v2: Value) -> Result<(Value, Value), Bson> {
    let pattern_string = match v1 {
        Value::String(s) => s,
        other => return Ok((other, v2)),
    };

    let options_string = match v2 {
        Value::String(s) => s,
        other => return Ok((Value::String(pattern_string), other)),
    };

    Err(Bson::RegExp(pattern_string, options_string))
}

fn convert_javascript_code_with_scope(v1: Value,
                                      v2: Value)
                                      -> error::Result<Result<(Value, Value), Bson>> {
    let code = match v1 {
        Value::String(s) => s,
        other => return Ok(Ok((other, v2))),
    };

    let scope = match v2 {
        Value::Mapping(m) => m,
        other => return Ok(Ok((Value::String(code), other))),
    };

    Ok(Err(Bson::JavaScriptCodeWithScope(code, convert_mapping_to_bson_document(scope)?)))
}

fn convert_binary(v1: Value, v2: Value) -> Result<(Value, Value), Bson> {
    let bin_type = match v1 {
        Value::I64(i) => i,
        other => return Ok((other, v2)),
    };

    let data = match v2 {
        Value::String(m) => m,
        other => return Ok((Value::I64(bin_type), other)),
    };

    let binary = match FromHex::from_hex(data.clone()) {
        Ok(b) => b,
        _ => return Ok((Value::I64(bin_type), Value::String(data))),
    };

    Err(Bson::Binary(From::from(bin_type as u8), binary))
}

fn convert_double_pair_mapping(key1: String,
                               mut value1: Value,
                               key2: String,
                               mut value2: Value)
                               -> error::Result<Result<Mapping, Bson>> {
    if key1 == "$regex" && key2 == "$options" {
        let pair = match convert_regex(value1, value2) {
            Ok(p) => p,
            Err(bson) => return Ok(Err(bson)),
        };

        value1 = pair.0;
        value2 = pair.1;
    } else if key1 == "$code" && key2 == "$scope" {
        let pair = match convert_javascript_code_with_scope(value1, value2)? {
            Ok(p) => p,
            Err(bson) => return Ok(Err(bson)),
        };

        value1 = pair.0;
        value2 = pair.1;
    } else if key1 == "$type" && key2 == "$binary" {
        let pair = match convert_binary(value1, value2) {
            Ok(p) => p,
            Err(bson) => return Ok(Err(bson)),
        };

        value1 = pair.0;
        value2 = pair.1;
    }

    let mut mapping = Mapping::new();
    mapping.insert(Value::String(key1), value1);
    mapping.insert(Value::String(key2), value2);

    Ok(Ok(mapping))
}

fn convert_mapping_to_bson(mut mapping: Mapping) -> error::Result<Bson> {
    if mapping.len() == 1 {
        let (key, value) = mapping.pop_front().unwrap();

        return match convert_single_pair_mapping(try_string!(key), value) {
                   Ok(m) => convert_mapping_to_bson_document(m).map(Bson::Document),
                   Err(bson) => Ok(bson),
               };
    } else if mapping.len() == 2 {
        let (key1, value1) = mapping.pop_front().unwrap();
        let (key2, value2) = mapping.pop_front().unwrap();

        return match convert_double_pair_mapping(try_string!(key1),
                                                 value1,
                                                 try_string!(key2),
                                                 value2)? {
                   Ok(m) => convert_mapping_to_bson_document(m).map(Bson::Document),
                   Err(bson) => Ok(bson),
               };
    }

    Ok(Bson::Document(convert_mapping_to_bson_document(mapping)?))
}

fn convert_mapping_to_bson_document(mapping: Mapping) -> error::Result<Document> {
    let mut document = Document::new();

    for (key, value) in mapping {
        document.insert(try_string!(key), yaml_to_bson(value)?);
    }

    Ok(document)
}

fn convert_array_to_bson(sequence: Sequence) -> error::Result<Bson> {
    let mut vec = Vec::new();

    for value in sequence {
        vec.push(yaml_to_bson(value)?);
    }

    Ok(Bson::Array(vec))
}

pub fn yaml_to_bson(yaml: Value) -> error::Result<Bson> {
    match yaml {
        Value::Null => Ok(Bson::Null),
        Value::Bool(b) => Ok(Bson::Boolean(b)),
        Value::I64(i) => Ok(Bson::I64(i)),
        Value::F64(f) => Ok(Bson::FloatingPoint(f)),
        Value::String(s) => Ok(Bson::String(s)),
        Value::Sequence(sequence) => convert_array_to_bson(sequence),
        Value::Mapping(m) => convert_mapping_to_bson(m),
    }
}

