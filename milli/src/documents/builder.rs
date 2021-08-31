use std::io::{self, Write};

use byteorder::{BigEndian, WriteBytesExt};
use bimap::BiHashMap;
use serde::ser::Serialize;

use super::{Error, ByteCounter, DocumentsMetadata};
use super::serde::DocumentsSerilializer;
use crate::FieldId;

pub struct DocumentsBuilder<W> {
    serializer: DocumentsSerilializer<W>,
}

impl<W: io::Write + io::Seek> DocumentsBuilder<W> {
    pub fn new(writer: W, index: BiHashMap<FieldId, String>) -> Result<Self, Error> {
        let mut writer = ByteCounter::new(writer);

        // add space to write the offset of the metadata at the end of the writer
        writer.write_u64::<BigEndian>(0)?;

        let serializer = DocumentsSerilializer {
            writer,
            buffer: Vec::new(),
            index,
            count: 0,
            allow_seq: true,
        };

        Ok(Self { serializer })
    }

    pub fn count(&self) -> usize {
        self.serializer.count
    }

    /// This method must be called after the document addition is terminated. It will put the
    /// metadata at the end of the file, and write the metadata offset at the beginning on the
    /// file.
    pub fn finish(self) -> Result<(), Error> {
        let DocumentsSerilializer {
            writer:
                ByteCounter {
                    mut writer,
                    count: offset,
                },
            index,
            count,
            ..
        } = self.serializer;

        let meta = DocumentsMetadata {
            count,
            index,
        };

        bincode::serialize_into(&mut writer, &meta)?;

        writer.seek(io::SeekFrom::Start(0))?;
        writer.write_u64::<BigEndian>(offset as u64)?;

        writer.flush()?;

        Ok(())
    }

    /// Adds a document that can be serilized. The internal index is updated with the fields found
    /// in the documents;
    pub fn add_documents<T: Serialize>(&mut self, document: T) -> Result<(), Error> {
        document.serialize(&mut self.serializer)?;
        Ok(())
    }

    /// Adds a raw document. This internal index **is not** updated, so it is expected that the
    /// index provided in the constructor is correct.
    pub fn add_raw_document(&mut self, document: impl AsRef<[u8]>) -> Result<(), Error> {
        let document = document.as_ref();
        self.serializer
            .writer
            .write_u32::<BigEndian>(document.len() as u32)?;
        self.serializer.writer.write_all(document)?;
        self.serializer.count += 1;
        Ok(())
    }
}
