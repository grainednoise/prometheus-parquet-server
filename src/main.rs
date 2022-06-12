use std::fs;
use std::net::SocketAddr;
use std::ops::Deref;
use std::path::PathBuf;
use std::process::exit;
use std::sync::Arc;

use axum::{
    extract::Extension,
    http::StatusCode,
    Json,
    response::IntoResponse, Router,
    routing::{get, post},
};
use axum::extract::{ContentLengthLimit, Form, Query};
use axum::http::header;
use axum::response::Response;
use chrono::prelude::*;
use clap::Parser;
use color_eyre::eyre::{eyre, Result as EyreResult};
use serde::{Serialize, Serializer};
use serde::ser::SerializeSeq;
use tower_http::trace::TraceLayer;

use crate::labels::LabelValueMap;
use crate::metricstore::process_file;
use crate::query::{QueryResult, run_query};
use crate::timeseries::{LabelledTimeSeries, LabelledTimeSeriesWithOffset, TimeSeriesCollection};
use crate::timevectors::RegularTimeRange;
use crate::web::{InstantQuery, RangeQuery};

mod stringuniverse;
mod timeseries;
mod config;
mod labels;
mod query;
mod metricstore;
mod timevectors;
mod web;

// use promql::Node::String;


fn main_func() {
    println!("start");

    let name = PathBuf::from("testdata/performancedata.parquet.zip");
    let tags = PathBuf::from("testdata/performancedata.parquet.zip.yaml");

    let all_time_series = Arc::new(match process_file(&name, &tags) {
        Ok(all_time_series) => all_time_series,
        Err(err) => {
            println!("Got error: {err}");
            exit(1);
        }
    });

    println!("Read {} timeseries, range={}", all_time_series.len(), all_time_series.time_range());

    // for item in all_time_series.by_name("webmango_request_duration_bucket") {
    //     println!("Filtered {}", item);
    // }

    // timing="transport"

//     let ast = parse(b"
// 	sum(1 - something_used{env=\"production\"} / something_total) by (instance)
// 	and ignoring (instance)
// 	sum(rate(some_queries{instance=~\"localhost\\\\d+\"} [5m])) > 100
// ", false).unwrap(); // or show user that their query is invalid

    let range = Arc::new(RegularTimeRange::new(0.0, 7000.0, 15.0, None));

    // run_query("webmango_request_duration_bucket{timing=\"transport\"}", all_time_series.clone(), range.clone()).unwrap(); // or show user that their query is invalid
    // run_query("webmango_request_duration_bucket{timing=\"transport\"}[5m]", all_time_series.clone()).unwrap(); // or show user that their query is invalid
    // let result = run_query("rate(webmango_request_duration_bucket{timing=\"transport\"}[5m])", all_time_series.clone(), range.clone());
    // let result = run_query("sum by (Le) (rate(webmango_request_duration_bucket{timing=\"transport\"}[5m]))", all_time_series.clone(), range.clone());
    // let result = run_query("sum by (Le) (rate(webmango_request_duration_bucket{}[5m]))", all_time_series.clone(), range.clone());
    // let result = run_query("histogram_quantile(0.95, sum by (Le) (rate(webmango_request_duration_bucket{timing=\"transport\"}[5m])))", all_time_series.clone(), range.clone()); // or show user that their query is invalid
    let result = run_query("histogram_quantile(0.95, sum by (Le, timing) (rate(webmango_request_duration_bucket{}[5m])))", all_time_series.clone(), range.clone()); // or show user that their query is invalid
    // let result = run_query("histogram_quantile(0.95, sum by (Le, timing) (webmango_request_duration_bucket{}))", all_time_series.clone(), range.clone()); // or show user that their query is invalid

    println!("Result: {result:?}");
}



fn read_all(name: &PathBuf, tags: &PathBuf, fixed_start_time: Option<f64>) -> Arc<TimeSeriesCollection> {
    let mut res = match process_file(name, tags) {
        Ok(all_time_series) => all_time_series,
        Err(err) => {
            println!("Got error: {err}");
            exit(1);
        }
    };

    if let Some(target_start_time) = fixed_start_time {
        res.set_query_start_time(target_start_time);
    }

    Arc::new(res)
}

const FAVICON_EMBED: &[u8] = include_bytes!("../assets/favicon.ico");

#[derive(Parser, Debug)]
#[clap(author, version, about, long_about = None)]
struct Args {
    directory: PathBuf,
    #[clap(short, long, help="Shift start of metrics to most recent local midnight")]
    shift_to_midnight: bool,
}

const STANDARD_FILE_SUFFIX: &str = ".parquet.zip";

impl Args {
    pub fn get_parquet_zip(&self) -> EyreResult<PathBuf> {
        let mut base_path = make_path_absolute(&self.directory)?;

        if base_path.is_file() {
            return Ok(base_path)
        }

        let mut found = Vec::new();

        for item in fs::read_dir(&base_path)? {
            if let Some(name) = item?.file_name().to_str() {
                if name.ends_with(".parquet.zip") {
                    found.push(name.to_string())
                }
            }
        }

        match found.len() {
            0 => Err(eyre!("No files end with {STANDARD_FILE_SUFFIX}")),
            1 => {
                base_path.push(found.pop().unwrap());
                Ok(base_path)
            }
            n => Err(eyre!("Too many ({n}) files end with {STANDARD_FILE_SUFFIX}"))
        }
    }

    pub fn get_tag_mapping_yaml(&self) -> EyreResult<PathBuf> {
        let base = self.get_parquet_zip()?;
        let mut raw = base.as_os_str().to_owned();
        raw.push(".yaml");
        let result = PathBuf::from(raw);

        if !result.is_file() {
            return Err(eyre!("No such file: {base:?}"))
        }

        Ok(result)
    }
}

pub fn make_path_absolute(pb: &PathBuf) -> EyreResult<PathBuf> {
    if pb.is_absolute() {
        Ok(pb.clone())
    }
    else {
        let mut result = PathBuf::from(std::env::current_dir()?);
        result.push(pb);
        Ok(result)
    }
}


#[tokio::main]
async fn main() {
    let args: Args = Args::parse();

    println!("directory {:?}!", args.directory);
    println!("shift_to_midnight {}!", args.shift_to_midnight);

    let data_file = match args.get_parquet_zip() {
        Ok(data_file) => data_file,
        Err(err) => {
            println!("Error getting data file name: {err}");
            exit(2)
        }
    };

    println!("parquet {:?}!", data_file);

    let tags_mapping_file = match args.get_tag_mapping_yaml() {
        Ok(tags_fapping_file) => tags_fapping_file,
        Err(err) => {
            println!("Error getting tags mapping file name: {err}");
            exit(2)
        }
    };

    println!("tags {:?}!", tags_mapping_file);

    if std::env::var_os("RUST_LOG").is_none() {
        std::env::set_var(
            "RUST_LOG",
            "prometheus_parquet_server=debug,tower_http=debug",
        )
    }

    // initialize tracing
    tracing_subscriber::fmt::init();

    let fixed_start_time = if args.shift_to_midnight {
        let local: DateTime<Local> = Local::now();
        println!("Local time {local}");
        let midnight = local
            .with_hour(0).unwrap()
            .with_minute(0).unwrap()
            .with_second(0).unwrap()
            .with_nanosecond(0).unwrap();
        println!("Midnight local time {midnight}");

        let epoch_midnight = midnight.timestamp_millis() as f64 / 1000.0;

        println!("Midnight epoch {epoch_midnight}");
        Some(epoch_midnight)
    }
    else {
        None
    };

    let tsc = read_all(&data_file, &tags_mapping_file, fixed_start_time);

    // build our application with a route
    let app = Router::new()
        .route("/", get(root))
        .route("/api/v1/labels", get(all_labels).post(all_labels_post))
        .route("/api/v1/query_range", get(query_range_get).post(query_range_post))
        .route("/api/v1/query", post(query_plain_post))
        .route("/api/v1/label/:label_name/values", get(get_label_values))
        .route("/favicon.ico", get(favicon))
        .layer(TraceLayer::new_for_http())
        .layer(Extension(tsc));

    // run our app with hyper
    // `axum::Server` is a re-export of `hyper::Server`
    let addr = SocketAddr::from(([0, 0, 0, 0], 3003));
    tracing::info!("listening on {}", addr);
    axum::Server::bind(&addr)
        .serve(app.into_make_service())
        .await
        .unwrap();
}

// basic handler that responds with a static string
async fn root(timeseries: Extension<Arc<TimeSeriesCollection>>) -> String {
    let u = timeseries.0.len();
    format!("Got {} time series", u)
}

async fn favicon() -> impl IntoResponse {
    (StatusCode::OK, [(header::CONTENT_TYPE, "image/x-icon")], FAVICON_EMBED)
}

#[derive(Serialize)]
struct PromQueryResult<D: Serialize> {
    #[serde(rename = "resultType")]
    result_type: &'static str,
    result: D
}

impl <D: Serialize> PromQueryResult<D> {
    fn new(result: D, result_type: &'static str) -> Self {
        PromQueryResult {
            result_type,
            result
        }
    }
}


#[derive(Serialize)]
struct PromSuccess<D: Serialize> {
    status: &'static str,
    data: D
}

impl <D: Serialize> PromSuccess<D> {
    fn new(data: D) -> Self {
        PromSuccess {
            status: "success",
            data
        }
    }
}

#[derive(Serialize)]
struct PromFailure {
    status: &'static str,
    #[serde(rename = "errorType")]
    error_type: String,
    error: String
}

impl PromFailure {
    fn new(error_type: String, error: String) -> Self {
        PromFailure {
            status: "error",
            error_type,
            error
        }
    }
}

#[derive(Debug, Serialize)]
struct TypedResult<T: Serialize> {
    result_type: &'static str,
    result: T
}

struct QueryResultWithOffset {
    offset: f64,
    data: Vec<LabelledTimeSeries>,
}

impl QueryResultWithOffset {
    pub fn new(offset: f64, data: Vec<LabelledTimeSeries>) -> Self {
        QueryResultWithOffset {
            offset,
            data
        }
    }
}

impl Serialize for QueryResultWithOffset {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error> where S: Serializer {
        let mut seq = serializer.serialize_seq(Some(self.data.len()))?;
        for lts in self.data.iter() {
            let lts_with_offset = LabelledTimeSeriesWithOffset::new(self.offset, lts);
            seq.serialize_element(&lts_with_offset)?;
        }
        seq.end()
    }
}




async fn all_labels(timeseries: Extension<Arc<TimeSeriesCollection>>) -> impl IntoResponse {
    let labels = timeseries.0.all_labels();
    let result = PromSuccess::new(labels);
    Json(result)
}

async fn all_labels_post(timeseries: Extension<Arc<TimeSeriesCollection>>, body: ContentLengthLimit<String, 4096>) -> impl IntoResponse {
    tracing::debug!("All labels body: {:?}", body);
    let labels = timeseries.0.all_labels();
    let result = PromSuccess::new(labels);
    Json(result)
}

async fn query_range_get(timeseries: Extension<Arc<TimeSeriesCollection>>, query: Query<RangeQuery>) -> Response {
    tracing::debug!("Query: {:?}", query);
    let all_time_series = timeseries.0.clone();
    query_range_generic(all_time_series, &query.deref())
}

async fn query_range_post(timeseries: Extension<Arc<TimeSeriesCollection>>, form: Form<RangeQuery>) -> Response {
    tracing::debug!("Query: {:?}", form);
    let all_time_series = timeseries.0.clone();
    query_range_generic(all_time_series, &form.deref())
}

fn query_range_generic(all_time_series: Arc<TimeSeriesCollection>, query: &RangeQuery) -> Response {
    let offset = all_time_series.query_time_offset().unwrap_or(0.0);

    let range = RegularTimeRange::new(
        query.start.clone().into(),
        query.end.into(),
        query.step.into(),
        all_time_series.query_time_offset());

    let range = Arc::new(range);

    let result = run_query(query.query.as_str(), all_time_series, range); // or show user that their query is invalid

    let qr = match result {
        Ok(qr) => qr,
        Err(e) => return (StatusCode::BAD_REQUEST, Json(PromFailure::new("queryError".to_string(), e.to_string()))).into_response()
    };

    let pr = match qr {
        QueryResult::OriginalCollection {..} => Err(eyre!("no query")),
        QueryResult::Unprocessed {..} => Err(eyre!("no processing")),
        QueryResult::UnprocessedVector {..} => Err(eyre!("no processing, vectorized")),
        QueryResult::Processed{time_series, ..} => Ok(time_series),
        QueryResult::ProcessedVector {..} => Err(eyre!("Cannot return a vectorized result")),
        QueryResult::Scalar(_) => Err(eyre!("scalar result")),
    };


    match pr {
        Ok(pr) => {
            let pr_offset = QueryResultWithOffset::new(offset, pr);
            Json(PromSuccess::new(PromQueryResult::new(pr_offset, "matrix"))).into_response()
        }
        Err(err) =>
            (StatusCode::BAD_REQUEST, Json(PromFailure::new("querySemantics".to_string(), err.to_string()))).into_response()
    }
}

async fn query_plain_post(_timeseries: Extension<Arc<TimeSeriesCollection>>, form: Form<InstantQuery>) -> Response {
    tracing::debug!("Query: {:?}", form);
    if form.query == "1+1" {
        return Json(PromSuccess::new(PromQueryResult::new(2.0_f64, "matrix"))).into_response()

    }
    (StatusCode::BAD_REQUEST, Json(PromFailure::new("querySemantics".to_string(), "sorry".to_string()))).into_response()
}

async fn get_label_values(timeseries: Extension<Arc<TimeSeriesCollection>>, axum::extract::Path(label_name): axum::extract::Path<String>) -> Response {
    tracing::debug!("Label name: {:?}", label_name);
    if label_name == "__name__" {
        let names = timeseries.0.all_time_series_names();
        tracing::debug!("Found: {:?}", names);
        let result = PromSuccess::new(names);
        return Json(result).into_response()

    }
    (StatusCode::BAD_REQUEST, Json(PromFailure::new("querySemantics".to_string(), "sorry".to_string()))).into_response()
}