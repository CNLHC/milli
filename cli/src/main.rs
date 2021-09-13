use std::fs::File;
use std::io::{stdin, Cursor, Read};
use std::{path::PathBuf, str::FromStr};

use byte_unit::Byte;
use eyre::Result;
use indicatif::{MultiProgress, ProgressBar, ProgressStyle};
use milli::update::UpdateIndexingStep::{
    ComputeIdsAndMergeDocuments, IndexDocuments, MergeDataIntoFinalDatabase, RemapDocumentAddition,
};
use serde_json::{Map, Value};
use structopt::StructOpt;

#[cfg(target_os = "linux")]
#[global_allocator]
static ALLOC: jemallocator::Jemalloc = jemallocator::Jemalloc;

#[derive(Debug, StructOpt)]
#[structopt(name = "example", about = "An example of StructOpt usage.")]
struct Cli {
    #[structopt(short, long)]
    index_path: PathBuf,
    #[structopt(short = "s", long, default_value = "100GiB")]
    index_size: Byte,
    /// Verbose mode (-v, -vv, -vvv, etc.)
    #[structopt(short, long, parse(from_occurrences))]
    verbose: usize,
    #[structopt(subcommand)]
    subcommand: Command,
}

#[derive(Debug, StructOpt)]
enum Command {
    DocumentAddition(DocumentAddition),
    Search(Search),
    SettingsUpdate(SettingsUpdate),
}

fn setup(opt: &Cli) -> eyre::Result<()> {
    color_eyre::install()?;
    stderrlog::new()
        .verbosity(opt.verbose)
        .show_level(false)
        .timestamp(stderrlog::Timestamp::Off)
        .init()?;
    Ok(())
}

fn main() -> Result<()> {
    let command = Cli::from_args();

    setup(&command)?;

    let mut options = heed::EnvOpenOptions::new();
    options.map_size(command.index_size.get_bytes() as usize);
    let index = milli::Index::new(options, command.index_path)?;

    match command.subcommand {
        Command::DocumentAddition(addition) => addition.perform(index)?,
        Command::Search(search) => search.perform(index)?,
        Command::SettingsUpdate(update) => update.perform(index)?,
    }

    Ok(())
}

#[derive(Debug)]
enum DocumentAdditionFormat {
    Csv,
    Json,
    Jsonl,
}

impl FromStr for DocumentAdditionFormat {
    type Err = eyre::Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "csv" => Ok(Self::Csv),
            "jsonl" => Ok(Self::Jsonl),
            "json" => Ok(Self::Json),
            other => eyre::bail!("invalid format: {}", other),
        }
    }
}

#[derive(Debug, StructOpt)]
struct DocumentAddition {
    #[structopt(short, long, default_value = "json")]
    format: DocumentAdditionFormat,
    /// Path of the update file, if not present, will read from stdin.
    #[structopt(short, long)]
    path: Option<PathBuf>,
    /// Wheter to generate missing document ids.
    #[structopt(short, long)]
    autogen_docids: bool,
    /// Whether to update or replace the documents if they already exist.
    #[structopt(short, long)]
    update_documents: bool,
}

impl DocumentAddition {
    fn perform(&self, index: milli::Index) -> Result<()> {
        let reader: Box<dyn Read> = match self.path {
            Some(ref path) => {
                let file = File::open(path)?;
                Box::new(file)
            }
            None => Box::new(stdin()),
        };

        println!("parsing documents...");
        let documents = match self.format {
            DocumentAdditionFormat::Csv => documents_from_csv(reader)?,
            DocumentAdditionFormat::Json => documents_from_json(reader)?,
            DocumentAdditionFormat::Jsonl => documents_from_jsonl(reader)?,
        };

        let reader = milli::documents::DocumentsReader::from_reader(Cursor::new(documents))?;
        println!("Adding {} documents to the index.", reader.len());

        let mut txn = index.env.write_txn()?;
        let mut addition = milli::update::IndexDocuments::new(&mut txn, &index, 0);

        if self.update_documents {
            addition.index_documents_method(milli::update::IndexDocumentsMethod::UpdateDocuments);
        }

        addition.log_every_n(100);

        if self.autogen_docids {
            addition.enable_autogenerate_docids()
        }

        let mut bars = Vec::new();
        let progesses = MultiProgress::new();
        for _ in 0..4 {
            let bar = ProgressBar::hidden();
            let bar = progesses.add(bar);
            bars.push(bar);
        }

        std::thread::spawn(move || {
            progesses.join().unwrap();
        });

        let result = addition.execute(reader, |step, _| indexing_callback(step, &bars))?;

        txn.commit()?;

        println!("result {:?}", result);
        Ok(())
    }
}

fn indexing_callback(step: milli::update::UpdateIndexingStep, bars: &[ProgressBar]) {
    let step_index = step.step();
    let bar = &bars[step_index];
    if step_index > 0 {
        let prev = &bars[step_index - 1];
        if !prev.is_finished() {
            prev.disable_steady_tick();
            prev.finish_at_current_pos();
        }
    }

    let style = ProgressStyle::default_bar()
        .template("[eta: {eta_precise}] {bar:40.cyan/blue} {pos:>7}/{len:7} {msg}")
        .progress_chars("##-");

    match step {
        RemapDocumentAddition { documents_seen } => {
            bar.set_style(ProgressStyle::default_spinner());
            bar.set_message(format!("remaped {} documents so far.", documents_seen));
        }
        ComputeIdsAndMergeDocuments { documents_seen, total_documents } => {
            bar.set_style(style);
            bar.set_length(total_documents as u64);
            bar.set_message("Merging documents...");
            bar.set_position(documents_seen as u64);
        }
        IndexDocuments { documents_seen, total_documents } => {
            bar.set_style(style);
            bar.set_length(total_documents as u64);
            bar.set_message("Indexing documents...");
            bar.set_position(documents_seen as u64);
        }
        MergeDataIntoFinalDatabase { databases_seen, total_databases } => {
            bar.set_style(style);
            bar.set_length(total_databases as u64);
            bar.set_message("Merging databases...");
            bar.set_position(databases_seen as u64);
        }
    }
    bar.enable_steady_tick(200);
}

fn documents_from_jsonl(reader: impl Read) -> Result<Vec<u8>> {
    let mut writer = Cursor::new(Vec::new());
    let mut documents =
        milli::documents::DocumentsBuilder::new(&mut writer, bimap::BiHashMap::new())?;

    let values = serde_json::Deserializer::from_reader(reader)
        .into_iter::<serde_json::Map<String, serde_json::Value>>();
    for document in values {
        let document = document?;
        documents.add_documents(document)?;
    }
    documents.finish()?;

    println!("finished conversion");

    Ok(writer.into_inner())
}

fn documents_from_json(reader: impl Read) -> Result<Vec<u8>> {
    let mut writer = Cursor::new(Vec::new());
    let mut documents =
        milli::documents::DocumentsBuilder::new(&mut writer, bimap::BiHashMap::new())?;

    let json: serde_json::Value = serde_json::from_reader(reader)?;
    documents.add_documents(json)?;
    documents.finish()?;

    Ok(writer.into_inner())
}

fn documents_from_csv(reader: impl Read) -> Result<Vec<u8>> {
    let mut writer = Cursor::new(Vec::new());
    let mut documents =
        milli::documents::DocumentsBuilder::new(&mut writer, bimap::BiHashMap::new())?;

    let mut records = csv::Reader::from_reader(reader);
    let iter = records.deserialize::<Map<String, Value>>();

    for doc in iter {
        let doc = doc?;
        documents.add_documents(doc)?;
    }

    documents.finish()?;

    Ok(writer.into_inner())
}

#[derive(Debug, StructOpt)]
struct Search {
    query: Option<String>,
    #[structopt(short, long)]
    filter: Option<String>,
    #[structopt(short, long)]
    offset: Option<usize>,
    #[structopt(short, long)]
    limit: Option<usize>,
}

impl Search {
    fn perform(&self, index: milli::Index) -> Result<()> {
        let txn = index.env.read_txn()?;
        let mut search = index.search(&txn);

        if let Some(ref query) = self.query {
            search.query(query);
        }

        if let Some(ref filter) = self.filter {
            let condition = milli::FilterCondition::from_str(&txn, &index, filter)?;
            search.filter(condition);
        }

        if let Some(offset) = self.offset {
            search.offset(offset);
        }

        if let Some(limit) = self.limit {
            search.limit(limit);
        }

        let result = search.execute()?;

        let fields_ids_map = index.fields_ids_map(&txn)?;
        let displayed_fields =
            index.displayed_fields_ids(&txn)?.unwrap_or_else(|| fields_ids_map.ids().collect());
        let documents = index.documents(&txn, result.documents_ids)?;
        let mut jsons = Vec::new();
        for (_, obkv) in documents {
            let json = milli::obkv_to_json(&displayed_fields, &fields_ids_map, obkv)?;
            jsons.push(json);
        }

        let hits = serde_json::to_string_pretty(&jsons)?;

        println!("{}", hits);

        Ok(())
    }
}

#[derive(Debug, StructOpt)]
struct SettingsUpdate {
    #[structopt(short, long)]
    filterable_attributes: Option<Vec<String>>,
}

impl SettingsUpdate {
    fn perform(&self, index: milli::Index) -> eyre::Result<()> {
        let mut txn = index.env.write_txn()?;

        let mut update = milli::update::Settings::new(&mut txn, &index, 0);
        update.log_every_n(100);

        if let Some(ref filterable_attributes) = self.filterable_attributes {
            if !filterable_attributes.is_empty() {
                update.set_filterable_fields(filterable_attributes.iter().cloned().collect());
            } else {
                update.reset_filterable_fields();
            }
        }

        let mut bars = Vec::new();
        let progesses = MultiProgress::new();
        for _ in 0..4 {
            let bar = ProgressBar::hidden();
            let bar = progesses.add(bar);
            bars.push(bar);
        }

        std::thread::spawn(move || {
            progesses.join().unwrap();
        });

        update.execute(|step, _| indexing_callback(step, &bars))?;

        txn.commit()?;
        Ok(())
    }
}
