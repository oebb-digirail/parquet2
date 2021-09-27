use crate::error::Result;
use crate::page::DataPageHeader;
use crate::parquet_bridge::Compression;
use crate::{
    compression::create_codec,
    page::{CompressedDataPage, DataPage},
};

/// Compresses a [`DataPage`] into a [`CompressedDataPage`].
pub fn compress(
    page: DataPage,
    mut compressed_buffer: Vec<u8>,
    compression: Compression,
) -> Result<CompressedDataPage> {
    let DataPage {
        mut buffer,
        header,
        dictionary_page,
        descriptor,
    } = page;
    let uncompressed_page_size = buffer.len();
    let codec = create_codec(&compression)?;
    if let Some(mut codec) = codec {
        match &header {
            DataPageHeader::V1(_) => {
                codec.compress(&buffer, &mut compressed_buffer)?;
            }
            DataPageHeader::V2(header) => {
                let levels_byte_length = (header.repetition_levels_byte_length
                    + header.definition_levels_byte_length)
                    as usize;
                compressed_buffer.extend_from_slice(&buffer[..levels_byte_length]);
                codec.compress(&buffer[levels_byte_length..], &mut compressed_buffer)?;
            }
        };
    } else {
        std::mem::swap(&mut buffer, &mut compressed_buffer);
    };
    Ok(CompressedDataPage::new(
        header,
        compressed_buffer,
        compression,
        uncompressed_page_size,
        dictionary_page,
        descriptor,
    ))
}
