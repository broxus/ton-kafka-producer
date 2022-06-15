use std::collections::BTreeMap;
use std::hash::Hash;
use std::str::FromStr;

use anyhow::Result;
use ton_indexer::utils::*;
use ton_types::UInt256;

pub fn parse_archive(data: Vec<u8>) -> Result<Vec<(ton_block::BlockIdExt, ParsedEntry)>> {
    let mut reader = ArchivePackageViewReader::new(&data)?;

    let mut map: BTreeMap<ton_block::BlockIdExt, PartiallyParsedEntry> = Default::default();

    while let Some(entry) = reader.read_next()? {
        let entry_id = PackageEntryId::from_filename(entry.name)?;
        let is_link = matches!(&entry_id, PackageEntryId::ProofLink(_));

        match PackageEntryId::from_filename(entry.name)? {
            PackageEntryId::Block(id) => {
                let mut parsed_entry = map.entry(id.clone()).or_default();
                let block = BlockStuff::deserialize_checked(id, entry.data)?;
                parsed_entry.block_stuff = Some((block, entry.data.to_vec()));
            }
            PackageEntryId::Proof(id) | PackageEntryId::ProofLink(id) => {
                let mut parsed_entry = map.entry(id.clone()).or_default();
                let proof = BlockProofStuff::deserialize(id, entry.data, is_link)?;
                parsed_entry.block_proof_stuff = Some(proof);
            }
        }
    }

    let result = map
        .into_iter()
        .map(|(key, entry)| match entry.block_stuff {
            Some(block_stuff) => Ok((
                key,
                ParsedEntry {
                    block_stuff,
                    block_proof_stuff: entry.block_proof_stuff,
                },
            )),
            None => Err(ArchiveError::MissingBlockData(key)),
        })
        .collect::<Result<Vec<_>, _>>()?;

    Ok(result)
}

#[derive(Clone)]
pub struct ParsedEntry {
    pub block_stuff: (BlockStuff, Vec<u8>),
    pub block_proof_stuff: Option<BlockProofStuff>,
}

#[derive(Default)]
struct PartiallyParsedEntry {
    block_stuff: Option<(BlockStuff, Vec<u8>)>,
    block_proof_stuff: Option<BlockProofStuff>,
}

struct ArchivePackageViewReader<'a> {
    data: &'a [u8],
    offset: usize,
}

impl<'a> ArchivePackageViewReader<'a> {
    fn new(data: &'a [u8]) -> Result<Self> {
        let mut offset = 0;
        read_package_header(data, &mut offset)?;
        Ok(Self { data, offset })
    }

    fn read_next(&mut self) -> Result<Option<ArchivePackageEntryView<'a>>> {
        ArchivePackageEntryView::read_from_view(self.data, &mut self.offset)
    }
}

fn read_package_header(buf: &[u8], offset: &mut usize) -> Result<()> {
    if buf.len() < *offset + 4 {
        return Err(ArchivePackageError::UnexpectedArchiveEof.into());
    }

    let magic = u32::from_le_bytes([
        buf[*offset],
        buf[*offset + 1],
        buf[*offset + 2],
        buf[*offset + 3],
    ]);
    *offset += 4;

    if magic == PKG_HEADER_MAGIC {
        Ok(())
    } else {
        Err(ArchivePackageError::InvalidArchiveHeader.into())
    }
}

struct ArchivePackageEntryView<'a> {
    pub name: &'a str,
    pub data: &'a [u8],
}

impl<'a> ArchivePackageEntryView<'a> {
    fn read_from_view(buf: &'a [u8], offset: &mut usize) -> Result<Option<Self>> {
        if buf.len() < *offset + 8 {
            return Ok(None);
        }

        if u16::from_le_bytes([buf[*offset], buf[*offset + 1]]) != ENTRY_HEADER_MAGIC {
            return Err(ArchivePackageError::InvalidArchiveEntryHeader.into());
        }
        *offset += 2;

        let filename_size = u16::from_le_bytes([buf[*offset], buf[*offset + 1]]) as usize;
        *offset += 2;

        let data_size = u32::from_le_bytes([
            buf[*offset],
            buf[*offset + 1],
            buf[*offset + 2],
            buf[*offset + 3],
        ]) as usize;
        *offset += 4;

        if buf.len() < *offset + filename_size + data_size {
            return Err(ArchivePackageError::UnexpectedEntryEof.into());
        }

        let name = std::str::from_utf8(&buf[*offset..*offset + filename_size])?;
        *offset += filename_size;

        let data = &buf[*offset..*offset + data_size];
        *offset += data_size;

        Ok(Some(Self { name, data }))
    }
}

const PKG_HEADER_MAGIC: u32 = 0xae8fdd01;
const ENTRY_HEADER_MAGIC: u16 = 0x1e8b;

#[derive(thiserror::Error, Debug)]
enum ArchivePackageError {
    #[error("Invalid archive header")]
    InvalidArchiveHeader,
    #[error("Unexpected archive eof")]
    UnexpectedArchiveEof,
    #[error("Invalid archive entry header")]
    InvalidArchiveEntryHeader,
    #[error("Unexpected entry eof")]
    UnexpectedEntryEof,
}

#[derive(Debug, thiserror::Error)]
enum ArchiveError {
    #[error("Block data is missing for block {0}")]
    MissingBlockData(ton_block::BlockIdExt),
}

#[derive(Debug, Hash, Eq, PartialEq)]
enum PackageEntryId<I> {
    Block(I),
    Proof(I),
    ProofLink(I),
}

impl PackageEntryId<ton_block::BlockIdExt> {
    fn from_filename(filename: &str) -> Result<Self> {
        let block_id_pos = match filename.find('(') {
            Some(pos) => pos,
            None => return Err(PackageEntryIdError::InvalidFileName.into()),
        };

        let (prefix, block_id) = filename.split_at(block_id_pos);

        Ok(match prefix {
            PACKAGE_ENTRY_BLOCK => Self::Block(parse_block_id(block_id)?),
            PACKAGE_ENTRY_PROOF => Self::Proof(parse_block_id(block_id)?),
            PACKAGE_ENTRY_PROOF_LINK => Self::ProofLink(parse_block_id(block_id)?),
            _ => return Err(PackageEntryIdError::InvalidFileName.into()),
        })
    }
}

fn parse_block_id(filename: &str) -> Result<ton_block::BlockIdExt> {
    let mut parts = filename.split(':');

    let shard_id = match parts.next() {
        Some(part) => part,
        None => return Err(PackageEntryIdError::ShardIdNotFound.into()),
    };

    let mut shard_id_parts = shard_id.split(',');
    let workchain_id = match shard_id_parts
        .next()
        .and_then(|part| part.strip_prefix('('))
    {
        Some(part) => i32::from_str(part)?,
        None => return Err(PackageEntryIdError::WorkchainIdNotFound.into()),
    };

    let shard_prefix_tagged = match shard_id_parts.next() {
        Some(part) => u64::from_str_radix(part, 16)?,
        None => return Err(PackageEntryIdError::ShardPrefixNotFound.into()),
    };

    let seq_no = match shard_id_parts
        .next()
        .and_then(|part| part.strip_suffix(')'))
    {
        Some(part) => u32::from_str(part)?,
        None => return Err(PackageEntryIdError::SeqnoNotFound.into()),
    };

    let shard_id = ton_block::ShardIdent::with_tagged_prefix(workchain_id, shard_prefix_tagged)?;

    let root_hash = match parts.next() {
        Some(part) => UInt256::from_str(part)?,
        None => return Err(PackageEntryIdError::RootHashNotFound.into()),
    };

    let file_hash = match parts.next() {
        Some(part) => UInt256::from_str(part)?,
        None => return Err(PackageEntryIdError::FileHashNotFound.into()),
    };

    Ok(ton_block::BlockIdExt {
        shard_id,
        seq_no,
        root_hash,
        file_hash,
    })
}

const PACKAGE_ENTRY_BLOCK: &str = "block_";
const PACKAGE_ENTRY_PROOF: &str = "proof_";
const PACKAGE_ENTRY_PROOF_LINK: &str = "prooflink_";

#[derive(thiserror::Error, Debug)]
enum PackageEntryIdError {
    #[error("Invalid filename")]
    InvalidFileName,
    #[error("Shard id not found")]
    ShardIdNotFound,
    #[error("Workchain id not found")]
    WorkchainIdNotFound,
    #[error("Shard prefix not found")]
    ShardPrefixNotFound,
    #[error("Seqno not found")]
    SeqnoNotFound,
    #[error("Root hash not found")]
    RootHashNotFound,
    #[error("File hash not found")]
    FileHashNotFound,
}
