mod builder;
/// The documents module defines an intermediary document format that milli uses for indexation, and
/// provides an API to easily build and read such documents.
///
/// The `DocumentBuilder` interface allows to write batches of documents to a writer, that can
/// later be read by milli using the `DocumentsReader` interface.
mod reader;
mod serde;

use std::{fmt, io};

use ::serde::{Deserialize, Serialize};
use bimap::BiHashMap;
pub use builder::DocumentsBuilder;
pub use reader::DocumentsReader;

use crate::FieldId;

type AdditionIndex = BiHashMap<FieldId, String>;

#[derive(Debug, Serialize, Deserialize)]
struct DocumentsMetadata {
    count: usize,
    index: AdditionIndex,
}

pub struct ByteCounter<W> {
    count: usize,
    writer: W,
}

impl<W> ByteCounter<W> {
    fn new(writer: W) -> Self {
        Self { count: 0, writer }
    }
}

impl<W: io::Write> io::Write for ByteCounter<W> {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        let count = self.writer.write(buf)?;
        self.count += count;
        Ok(count)
    }

    fn flush(&mut self) -> io::Result<()> {
        self.writer.flush()
    }
}

#[derive(Debug)]
pub enum Error {
    InvalidDocumentFormat,
    Custom(String),
    JsonError(serde_json::Error),
    Serialize(bincode::Error),
    Io(io::Error),
    DocumentTooLarge,
}

impl From<io::Error> for Error {
    fn from(other: io::Error) -> Self {
        Self::Io(other)
    }
}

impl From<bincode::Error> for Error {
    fn from(other: bincode::Error) -> Self {
        Self::Serialize(other)
    }
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Error::Custom(s) => write!(f, "Unexpected serialization error: {}", s),
            Error::InvalidDocumentFormat => f.write_str("Invalid document addition format."),
            Error::JsonError(err) => write!(f, "Couldn't serialize document value: {}", err),
            Error::Io(e) => e.fmt(f),
            Error::DocumentTooLarge => f.write_str("Provided document is too large (>2Gib)"),
            Error::Serialize(e) => e.fmt(f),
        }
    }
}

impl std::error::Error for Error {}

/// Macro used to generate documents, with the same syntax as `serde_json::json`
#[cfg(test)]
macro_rules! documents {
    ($data:tt) => {{
        let documents = serde_json::json!($data);
        let mut writer = std::io::Cursor::new(Vec::new());
        let mut builder =
            crate::documents::DocumentsBuilder::new(&mut writer).unwrap();
        builder.add_documents(documents).unwrap();
        builder.finish().unwrap();

        writer.set_position(0);

        crate::documents::DocumentsReader::from_reader(writer).unwrap()
    }};
}

#[cfg(test)]
mod test {
    use serde_json::{json, Value};

    use super::*;

    #[test]
    fn create_documents_no_errors() {
        let json = json!({
            "number": 1,
            "string": "this is a field",
            "array": ["an", "array"],
            "object": {
                "key": "value",
            },
            "bool": true
        });

        let mut v = Vec::new();
        let mut cursor = io::Cursor::new(&mut v);

        let mut builder = DocumentsBuilder::new(&mut cursor).unwrap();

        builder.add_documents(json).unwrap();

        builder.finish().unwrap();

        let mut documents =
            DocumentsReader::from_reader(io::Cursor::new(cursor.into_inner())).unwrap();

        assert_eq!(documents.index().iter().count(), 5);

        let reader = documents.next_document_with_index().unwrap().unwrap();

        assert_eq!(reader.1.iter().count(), 5);
        assert!(documents.next_document_with_index().unwrap().is_none());
    }

    #[test]
    fn test_add_multiple_documents() {
        let doc1 = json!({
            "bool": true,
        });
        let doc2 = json!({
            "toto": false,
        });

        let mut v = Vec::new();
        let mut cursor = io::Cursor::new(&mut v);

        let mut builder = DocumentsBuilder::new(&mut cursor).unwrap();

        builder.add_documents(doc1).unwrap();
        builder.add_documents(doc2).unwrap();

        builder.finish().unwrap();

        let mut documents =
            DocumentsReader::from_reader(io::Cursor::new(cursor.into_inner())).unwrap();

        assert_eq!(documents.index().iter().count(), 2);

        let reader = documents.next_document_with_index().unwrap().unwrap();

        assert_eq!(reader.1.iter().count(), 1);
        assert!(documents.next_document_with_index().unwrap().is_some());
        assert!(documents.next_document_with_index().unwrap().is_none());
    }

    #[test]
    fn add_documents_array() {
        let docs = json!([
            { "toto": false },
            { "tata": "hello" },
        ]);

        let mut v = Vec::new();
        let mut cursor = io::Cursor::new(&mut v);

        let mut builder = DocumentsBuilder::new(&mut cursor).unwrap();

        builder.add_documents(docs).unwrap();

        builder.finish().unwrap();

        let mut documents =
            DocumentsReader::from_reader(io::Cursor::new(cursor.into_inner())).unwrap();

        assert_eq!(documents.index().iter().count(), 2);

        let reader = documents.next_document_with_index().unwrap().unwrap();

        assert_eq!(reader.1.iter().count(), 1);
        assert!(documents.next_document_with_index().unwrap().is_some());
        assert!(documents.next_document_with_index().unwrap().is_none());
    }

    #[test]
    fn add_invalid_document_format() {
        let mut v = Vec::new();
        let mut cursor = io::Cursor::new(&mut v);

        let mut builder = DocumentsBuilder::new(&mut cursor).unwrap();

        let docs = json!([[
            { "toto": false },
            { "tata": "hello" },
        ]]);

        assert!(builder.add_documents(docs).is_err());

        let docs = json!("hello");

        assert!(builder.add_documents(docs).is_err());
    }

    #[test]
    fn test_nested() {
        let mut docs = documents!([{
            "hello": {
                "toto": ["hello"]
            }
        }]);

        let (_index, doc) = docs.next_document_with_index().unwrap().unwrap();

        let nested: Value = serde_json::from_slice(doc.get(0).unwrap()).unwrap();
        assert_eq!(nested, json!({ "toto": ["hello"] }));
    }
}
