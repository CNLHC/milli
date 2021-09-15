use std::fs::{create_dir_all, remove_dir_all, File};
use std::path::Path;

use criterion::BenchmarkId;
use heed::EnvOpenOptions;
use milli::update::{IndexDocumentsMethod, Settings, UpdateBuilder, UpdateFormat};
use milli::{FilterCondition, Index};

pub struct Conf<'a> {
    /// where we are going to create our database.mmdb directory
    /// each benchmark will first try to delete it and then recreate it
    pub database_name: &'a str,
    /// the dataset to be used, it must be an uncompressed csv
    pub dataset: &'a str,
    /// The format of the dataset
    pub dataset_format: UpdateFormat,
    pub group_name: &'a str,
    pub queries: &'a [&'a str],
    /// here you can change which criterion are used and in which order.
    /// - if you specify something all the base configuration will be thrown out
    /// - if you don't specify anything (None) the default configuration will be kept
    pub criterion: Option<&'a [&'a str]>,
    /// the last chance to configure your database as you want
    pub configure: fn(&mut Settings),
    pub filter: Option<&'a str>,
    pub sort: Option<Vec<&'a str>>,
    /// enable or disable the optional words on the query
    pub optional_words: bool,
    /// primary key, if there is None we'll auto-generate docids for every documents
    pub primary_key: Option<&'a str>,
}

impl Conf<'_> {
    pub const BASE: Self = Conf {
        database_name: "benches.mmdb",
        dataset_format: UpdateFormat::Csv,
        dataset: "",
        group_name: "",
        queries: &[],
        criterion: None,
        configure: |_| (),
        filter: None,
        sort: None,
        optional_words: true,
        primary_key: None,
    };
}

pub fn base_setup(conf: &Conf) -> Index {
    match remove_dir_all(&conf.database_name) {
        Ok(_) => (),
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => (),
        Err(e) => panic!("{}", e),
    }
    create_dir_all(&conf.database_name).unwrap();

    let mut options = EnvOpenOptions::new();
    options.map_size(100 * 1024 * 1024 * 1024); // 100 GB
    options.max_readers(10);
    let index = Index::new(options, conf.database_name).unwrap();

    let update_builder = UpdateBuilder::new(0);
    let mut wtxn = index.write_txn().unwrap();
    let mut builder = update_builder.settings(&mut wtxn, &index);

    if let Some(primary_key) = conf.primary_key {
        builder.set_primary_key(primary_key.to_string());
    }

    if let Some(criterion) = conf.criterion {
        builder.reset_filterable_fields();
        builder.reset_criteria();
        builder.reset_stop_words();

        let criterion = criterion.iter().map(|s| s.to_string()).collect();
        builder.set_criteria(criterion);
    }

    (conf.configure)(&mut builder);

    builder.execute(|_, _| ()).unwrap();
    wtxn.commit().unwrap();

    let update_builder = UpdateBuilder::new(0);
    let mut wtxn = index.write_txn().unwrap();
    let mut builder = update_builder.index_documents(&mut wtxn, &index);
    if let None = conf.primary_key {
        builder.enable_autogenerate_docids();
    }
    builder.update_format(conf.dataset_format);
    builder.index_documents_method(IndexDocumentsMethod::ReplaceDocuments);
    let reader = File::open(conf.dataset)
        .expect(&format!("could not find the dataset in: {}", conf.dataset));
    builder.execute(reader, |_, _| ()).unwrap();
    wtxn.commit().unwrap();

    index
}

pub fn run_benches(c: &mut criterion::Criterion, confs: &[Conf]) {
    for conf in confs {
        let index = base_setup(conf);

        let file_name = Path::new(conf.dataset).file_name().and_then(|f| f.to_str()).unwrap();
        let name = format!("{}: {}", file_name, conf.group_name);
        let mut group = c.benchmark_group(&name);

        for &query in conf.queries {
            group.bench_with_input(BenchmarkId::from_parameter(query), &query, |b, &query| {
                b.iter(|| {
                    let rtxn = index.read_txn().unwrap();
                    let mut search = index.search(&rtxn);
                    search.query(query).optional_words(conf.optional_words);
                    if let Some(filter) = conf.filter {
                        let filter = FilterCondition::from_str(&rtxn, &index, filter).unwrap();
                        search.filter(filter);
                    }
                    if let Some(sort) = &conf.sort {
                        let sort = sort.iter().map(|sort| sort.parse().unwrap()).collect();
                        search.sort_criteria(sort);
                    }
                    let _ids = search.execute().unwrap();
                });
            });
        }
        group.finish();

        index.prepare_for_closing().wait();
    }
}
