use std::collections::HashMap;
use std::fmt::{Debug, Display, Formatter};
use std::fs::File;
use std::io::{Error, Read};
use std::path::{Path, PathBuf};
use std::sync::Arc;

use color_eyre::eyre::{eyre, Result as EyreResult};
use itertools::{Either, enumerate, Itertools};
use lazy_static::lazy_static;
use parquet::basic::Type;
use parquet::file::reader::{FileReader, SerializedFileReader};
use parquet::file::serialized_reader::SliceableCursor;
use parquet::record::{Field, Row, RowAccessor};
use regex::Regex;
use zip::ZipArchive;

use crate::config::read_mapping;
use crate::labels::{LabelCode, LabelValueMap, make_metric_name_legal, NAME_LABEL};
use crate::metricstore::TaggedMetricType::{Histogram, MultiColumn, SingleColumn, Unknown};
use crate::timeseries::{LabelledTimeSeries, TimeSeries, TimeSeriesCollection};
use crate::timevectors::VecTimeRange;

lazy_static! {
    static ref BUCKET_NAME: Regex = Regex::new(r"^Le((\d*(\.\d*)?([eE][-+]?\d+)?)|\+[Ii]nf)$").unwrap();
}


pub struct MetricStore {
    zip_archive: ZipArchive<File>,
    metrics: HashMap<String, SerializedFileReader<SliceableCursor>>,
}


impl MetricStore {
    pub fn new(name: &Path) -> EyreResult<Self> {
        let zipfile = File::open(name)?;

        Ok(MetricStore {
            zip_archive: ZipArchive::new(zipfile)?,
            metrics: HashMap::new()
        })
    }

    fn raw_names(&self) -> impl Iterator<Item = &str> {
        self.zip_archive.file_names()
    }

    fn construct_reader(&mut self, name: &str) -> Result<SerializedFileReader<SliceableCursor>, Error> {
        let mut zf = self.zip_archive.by_name(name)?;
        let mut raw_data = Vec::<u8>::with_capacity(zf.size() as usize);
        zf.read_to_end(&mut raw_data)?;
        let cursor = SliceableCursor::new(raw_data);
        let rdr = SerializedFileReader::new(cursor)?;
        Ok(rdr)
    }


    fn get_reader(&mut self, name: &str) -> EyreResult<Option<&SerializedFileReader<SliceableCursor>>> {
        if !self.metrics.contains_key(name) {
            match self.construct_reader(name) {
                Ok(rdr) => {
                    self.metrics.insert(name.to_string(), rdr);
                }
                Err(e) => {
                    return Err(eyre!("failed to construct reader: {}", e))
                }
            }
        }

        Ok(self.metrics.get(name))
    }

    fn get_preliminary_metric_types(&self, mapping: HashMap<String, LabelValueMap>) -> Vec<TaggedMetric> {
        let mut mappings = Vec::<TaggedMetric>::new();

        for name in self.raw_names() {
            // println!("  {name}");

            let (prefix, cleaned) = split_and_clean(name);

            match mapping.get(prefix) {
                None => {
                    println!("Skipping {name}, no match")
                }
                Some(map) => {
                    let mut labelmap_with_name = map.clone();
                    labelmap_with_name.set_value(NAME_LABEL, &cleaned);
                    let tmt = TaggedMetric {
                        metric_type: Unknown,
                        time_column: None,
                        fixed_labels: labelmap_with_name,
                        varying_labels: None,
                        full_zip_name: name.to_string()
                    };

                    mappings.push(tmt);
                }
            };
        }
        mappings
    }

    fn refine_single_metric(&mut self, tagged_metric: &mut TaggedMetric) -> EyreResult<()> {
        let name = tagged_metric.full_zip_name.as_str();

        let rdr = match self.get_reader(name)? {
            None => {
                return Err(eyre!("Cannot find reader for {name}"))
            }
            Some(r) => r
        };

        let md = rdr.metadata();

        // let fmd = md.file_metadata().key_value_metadata().as_ref().unwrap();
        // for kv in fmd {
        //     println!("KeyValue {kv:?}");
        // }

        let row_groups = md.num_row_groups();
        // println!("Row groups {row_groups}");
        if row_groups != 1 {
            return Err(eyre!("Expected exactly 1 row group"))
        }

        let row_group = md.row_group(0);
        // println!("ROW {row_group:?}");

        let mut possible_value_columns = Vec::<&str>::new();
        let mut possible_time_columns = Vec::<&str>::new();
        let mut possible_tag_columns = Vec::<&str>::new();

        for col in row_group.columns() {
            // println!("ROW {col:?}");
            let column_name = col.column_descr().name();
            match col.column_type() {
                Type::BOOLEAN => {
                    // println!("BOOLEAN for {column_name}");
                    return Err(eyre!("Booleans are beyond me, column = {column_name}"))
                }

                Type::DOUBLE | Type::FLOAT | Type::INT32 | Type::INT64 | Type::INT96  => {
                    // println!("TIME OR VALUE for {column_name}");
                    if column_name == "time" || column_name == "timestamp" || column_name == "__time__" {
                        // println!("TIME for {column_name}");
                        possible_time_columns.push(column_name);
                    }
                    else {
                        // println!("VALUE for {column_name}");
                        possible_value_columns.push(column_name);
                    }
                }
                Type::BYTE_ARRAY | Type::FIXED_LEN_BYTE_ARRAY => {
                    //A string value, can only be a tag
                    // println!("LABEL for {column_name}");
                    possible_tag_columns.push(column_name);
                }
            }
        }

        let time_col = match possible_time_columns.len() {
            0 => return Err(eyre!("No possible time columns")),
            1 => { possible_time_columns[0].to_string() },
            cnt=> return Err(eyre!("Too many possible time columns: {cnt}"))
        };

        let metric_type = TaggedMetricType::from_value_columns(&possible_value_columns)?;

        // println!("Time column: {time_col}");
        // // println!("Possible value columns: {possible_value_columns:?}");
        // println!("Possible tag columns: {possible_tag_columns:?}");
        //
        // println!("Metric type: {metric_type:?}");


        tagged_metric.time_column = Some(time_col);
        tagged_metric.metric_type = metric_type;

        let lvm = &mut tagged_metric.fixed_labels;

        let mut tags: Vec<_> = possible_tag_columns.iter().map(|c| lvm.register_label(*c)).collect();
        tags.sort();
        tagged_metric.varying_labels = Some(tags);

        Ok(())
    }
}
const PARQUET_SUFFIX: &str = ".parquet";

fn clean_metric_name(name: &str) -> String {
    let stripped = if name.ends_with(PARQUET_SUFFIX) {
        let new_length = name.len() - PARQUET_SUFFIX.len();
        &name[..new_length]
    }
    else {
        name
    };

    make_metric_name_legal(stripped)
}

///This is a very cumbersome way of getting a number out of a Row, and don't I know it.
/// Unfortunately, there seems to be no direct way to access the Field in a Row
pub fn get_f64(row: &Row, index: usize) -> EyreResult<f64> {
    if let Ok(val) = row.get_double(index) {
        return Ok(val)
    }

    for (idx, (col, field)) in enumerate(row.get_column_iter()) {
        // println!("{idx} :: {col} :: {field}");
        if idx == index {
            let val = match field {
                Field::Null => f64::NAN,
                Field::Byte(v) => *v as f64,
                Field::Short(v) => *v as f64,
                Field::Int(v) => *v as f64,
                Field::Long(v) => *v as f64,
                Field::UByte(v) => *v as f64,
                Field::UShort(v) => *v as f64,
                Field::UInt(v) => *v as f64,
                Field::ULong(v) => *v as f64,
                Field::Float(v) => *v as f64,
                Field::Double(v) => *v,
                _ => return Err(eyre!("Incompatible field type: {field} {col}"))
            };

            return Ok(val)
        }
    }

    Err(eyre!("No column for index: {index}"))
}

fn contains_str(haystack: &Vec<String>, needle: &str) -> bool {
    haystack.iter().any(|h| h == needle)
}


trait TimeSeriesBuilder : Debug {
    fn feed(&mut self, time_stamp: f64, row: &Row) -> EyreResult<()>;
    fn generate_next(&mut self) -> EyreResult<Option<LabelledTimeSeries>>;
}

#[derive(Debug)]
struct DummyTimeSeriesBuilder {

}


impl TimeSeriesBuilder for DummyTimeSeriesBuilder {
    fn feed(&mut self, time_stamp: f64, row: &Row) -> EyreResult<()>{
        println!("Feed {time_stamp} - {row}");
        Ok(())
    }


    fn generate_next(&mut self) -> EyreResult<Option<LabelledTimeSeries>> {
        Ok(None)
    }
}

#[derive(Debug)]
struct SingleValueTimeSeriesBuilder {
    label_value_map: Option<LabelValueMap>,
    column_index: usize,
    time_stamps: Option<Vec<f64>>,
    values: Option<Vec<f64>>
}

impl SingleValueTimeSeriesBuilder {
    fn new(label_value_map: LabelValueMap, column_index: usize) -> Self {
        SingleValueTimeSeriesBuilder {
            label_value_map: Some(label_value_map),
            column_index,
            time_stamps: Some(Vec::new()),
            values: Some(Vec::new())
        }
    }
}

impl TimeSeriesBuilder for SingleValueTimeSeriesBuilder {
    fn feed(&mut self, time_stamp: f64, row: &Row) -> EyreResult<()> {
        let time_stamps = self.time_stamps.as_mut().unwrap();
        time_stamps.push(time_stamp);

        let value = get_f64(row, self.column_index)?;
        let values = self.values.as_mut().unwrap();
        values.push(value);

        Ok(())
    }

    fn generate_next(&mut self) -> EyreResult<Option<LabelledTimeSeries>> {
        match self.label_value_map.take() {
            None => Ok(None),
            Some(lvm) => {
                let stamps = VecTimeRange::from_values(self.time_stamps.take().unwrap())?;
                let values = self.values.take().unwrap();

                let ts = TimeSeries::new(Arc::new(stamps), Arc::new(values))?;

                Ok(Some(LabelledTimeSeries::new(lvm, ts)))
            }
        }
    }
}


#[derive(Debug)]
struct MultiValueTimeSeriesBuilder {
    label_value_maps: Vec<LabelValueMap>,
    column_indexes: Vec<usize>,
    time_stamps: Option<Vec<f64>>,
    values: Vec<Vec<f64>>,
    shared_time_stamps: Option<Arc<VecTimeRange>>
}

impl MultiValueTimeSeriesBuilder {
    fn new(label_value_maps: Vec<LabelValueMap>, column_indexes: Vec<usize>) -> Self {
        let length = column_indexes.len();
        MultiValueTimeSeriesBuilder {
            label_value_maps,
            column_indexes,
            time_stamps: Some(Vec::new()),
            values: vec![Vec::<f64>::new(); length],
            shared_time_stamps: None
        }
    }
}
//MultiColumn
impl TimeSeriesBuilder for MultiValueTimeSeriesBuilder {
    fn feed(&mut self, time_stamp: f64, row: &Row) -> EyreResult<()> {
        let time_stamps = self.time_stamps.as_mut().unwrap();
        time_stamps.push(time_stamp);

        for (idx, col_idx) in enumerate(&self.column_indexes) {
            let value = get_f64(row, *col_idx)?;
            let series = &mut self.values[idx];
            series.push(value);
        }

        Ok(())
    }

    fn generate_next(&mut self) -> EyreResult<Option<LabelledTimeSeries>> {
        let lvm = match self.label_value_maps.pop() {
            None => return Ok(None),
            Some(lvm) => lvm
        };

        let values = self.values.pop().unwrap();

        let stamps = match self.shared_time_stamps {
            None => {
                let stamps = self.time_stamps.take().unwrap();
                let stamps = VecTimeRange::from_values(stamps)?;
                let stamps = Arc::new(stamps);
                self.shared_time_stamps = Some(stamps.clone());
                stamps
            }
            Some(ref stamps) => stamps.clone()
        };

        let ts = TimeSeries::new(stamps, Arc::new(values))?;

        Ok(Some(LabelledTimeSeries::new(lvm, ts)))
    }
}


#[derive(Debug)]
enum TaggedMetricType {
    Unknown,
    SingleColumn(String),
    MultiColumn(Vec<String>),
    Histogram {
        buckets: Vec<String>,
        alternate_columns: Vec<String>
    },
    // There is no Summary yet, as there is no Parquet data model for that yet.
    // In fact, there may never be, as they're so much less useful than Histograms.
}


const LE_INF_BUCKET: &'static str = "Le+Inf";
const COUNT_COLUMN: &'static str = "count";

impl TaggedMetricType {
    fn is_non_bucket_histogram_column(column_name: &str) -> bool {
        match column_name {
            "count" | "sum" | "max" => true,
            _ => false
        }
    }

    fn can_be_histogram(column_names: &Vec<&str>) -> EyreResult<bool> {
        let all_histogram_columns = column_names.iter().all(|c|
            BUCKET_NAME.is_match(*c) || TaggedMetricType::is_non_bucket_histogram_column(*c));

        if all_histogram_columns {
            return Ok(true)
        }

        let contains_buckets = column_names.iter().any(|c| BUCKET_NAME.is_match(*c));
        if !contains_buckets {
            return Ok(false)
        }

        Err(eyre!("This is a mixture of columns that is not a full histogram nor a simple set of independent columns : {column_names:?}"))
    }

    fn from_value_columns(column_names: &Vec<&str>) -> EyreResult<Self> {
        match column_names.len() {
            0 => Err(eyre!("No value columns")),
            1 => Ok(SingleColumn(column_names[0].to_string())),
            _ => Ok(
                if TaggedMetricType::can_be_histogram(column_names)? {
                    let (buckets, alternate_columns): (Vec<_>, Vec<_>) =
                        column_names
                            .iter()
                            .partition_map(|c|
                                if BUCKET_NAME.is_match(*c) {
                                    Either::Left(c.to_string())
                                } else {
                                    Either::Right(c.to_string())
                                }
                            );
                    Histogram{buckets, alternate_columns}
                }
                else {
                    MultiColumn(column_names.iter().map(|s| s.to_string()).collect())
                }
            )
        }
    }

    fn create_metric_builder(&self, label_value_map: &LabelValueMap, name_to_column_index: &HashMap<String, usize>) -> Box<dyn TimeSeriesBuilder> {
        match self {
            SingleColumn(col_name) => {
                let col_index = name_to_column_index.get(col_name.as_str()).unwrap();

                Box::new(SingleValueTimeSeriesBuilder::new(label_value_map.clone(), *col_index))
            }

            MultiColumn(col_names) => {
                let mut label_value_maps = Vec::<LabelValueMap>::with_capacity(col_names.len());
                let mut column_indexes = Vec::<usize>::with_capacity(col_names.len());
                for col_name in col_names {
                    let mut new_lvm = label_value_map.clone();

                    if col_name != "value" {
                        new_lvm.add_suffix_to_metric(col_name);
                    };

                    label_value_maps.push(new_lvm);

                    let index = name_to_column_index.get(col_name).unwrap();
                    column_indexes.push(*index);
                }

                Box::new(MultiValueTimeSeriesBuilder::new(label_value_maps, column_indexes))
            }

            Histogram { buckets, alternate_columns} => {
                let generate_linf_from_count = !contains_str(buckets, LE_INF_BUCKET) && contains_str(alternate_columns, "count");
                let generate_count_from_linf = contains_str(buckets, LE_INF_BUCKET) && !contains_str(alternate_columns, "count");

                let total_capacity = buckets.len() + alternate_columns.len() + (generate_linf_from_count || generate_count_from_linf) as usize;

                let mut label_value_maps = Vec::<LabelValueMap>::with_capacity(total_capacity);
                let mut column_indexes = Vec::<usize>::with_capacity(total_capacity);

                let mut bucket_lvm = label_value_map.clone();
                bucket_lvm.add_suffix_to_metric("bucket");
                let le_code = bucket_lvm.register_label("Le");

                for col_name in buckets {
                    let mut new_lvm = bucket_lvm.clone();
                    new_lvm.set_value_by_code(le_code, &col_name[2..]);

                    label_value_maps.push(new_lvm);

                    let index = name_to_column_index.get(col_name).unwrap();
                    column_indexes.push(*index);
                }

                if generate_linf_from_count {
                    let mut new_lvm = bucket_lvm.clone();
                    new_lvm.set_value_by_code(le_code, &LE_INF_BUCKET[2..]);

                    label_value_maps.push(new_lvm);

                    let index = name_to_column_index.get(COUNT_COLUMN).unwrap();
                    column_indexes.push(*index);
                }

                for col_name in alternate_columns {
                    let mut new_lvm = label_value_map.clone();
                    new_lvm.add_suffix_to_metric(col_name);

                    label_value_maps.push(new_lvm);

                    let index = name_to_column_index.get(col_name).unwrap();
                    column_indexes.push(*index);
                }

                if generate_count_from_linf {
                    let mut new_lvm = label_value_map.clone();
                    new_lvm.add_suffix_to_metric(COUNT_COLUMN);

                    label_value_maps.push(new_lvm);

                    let index = name_to_column_index.get(LE_INF_BUCKET).unwrap();
                    column_indexes.push(*index);
                }

                Box::new(MultiValueTimeSeriesBuilder::new(label_value_maps, column_indexes))
            }

            Unknown => Box::new(DummyTimeSeriesBuilder {})
        }
    }
}

impl Display for TaggedMetricType {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Unknown => write!(f, "Unknown"),
            SingleColumn(sc) => write!(f, "SingleColumn[{sc}]"),
            MultiColumn(mc) => write!(f, "MultiColumn[{mc:?}]"),
            Histogram { .. } => write!(f, "Histogram"),
        }
    }
}


#[derive(Debug)]
struct TaggedMetric {
    time_column: Option<String>,
    metric_type: TaggedMetricType,
    fixed_labels: LabelValueMap,
    varying_labels: Option<Vec<LabelCode>>,
    full_zip_name: String
}


pub fn process_file(name: &PathBuf, tags: &PathBuf) -> EyreResult<TimeSeriesCollection> {
    let mut all_timeseries = TimeSeriesCollection::new();
    let mapping = read_mapping(tags)?;

    let mut metric_store = MetricStore::new(name)?;

    let mut mappings = metric_store.get_preliminary_metric_types(mapping);

    for mut mp in mappings.iter_mut() {
        match process_all_timeseries_from_metric(&mut mp, &mut metric_store, &mut all_timeseries) {
            Ok(_) => {
                println!("Processed {} - {}", mp.full_zip_name, mp.metric_type);
            }
            Err(err) => {
                println!("Failed to process {:?} - {}: {}", mp.metric_type, mp.full_zip_name, err);
            }
        }
    }

    Ok(all_timeseries)
}


fn process_all_timeseries_from_metric(mut mp: &mut &mut TaggedMetric, metric_store: &mut MetricStore, all_timeseries: &mut TimeSeriesCollection) ->EyreResult<()> {
    // println!("Mapping {:?}", mp.full_zip_name);
    metric_store.refine_single_metric(&mut mp)?;

    let mut identified_label_values = HashMap::<Vec<LabelCode>, Box<dyn TimeSeriesBuilder>>::new();

    let lvm = &mut mp.fixed_labels;

    let mut name_to_column_index = HashMap::<String, usize>::new();
    let rdr = metric_store.get_reader(&mp.full_zip_name)?.unwrap();
    let rg = rdr.get_row_group(0).unwrap();
    let meta = rg.metadata();
    for (idx, col) in enumerate(meta.columns()) {
        name_to_column_index.insert(col.column_descr().name().to_string(), idx);
    }

    let varying_labels = mp.varying_labels.as_ref().unwrap();
    let mut label_column_map: Vec<(LabelCode, usize)> = Vec::new();

    for vl in varying_labels {
        let col_idx = lvm.process_for_label_index(*vl, |name| name_to_column_index.get(name).unwrap()).unwrap();
        label_column_map.push((*vl, *col_idx));
    }

    let mut current_label_values = Vec::<LabelCode>::with_capacity(label_column_map.len());

    let time_index = name_to_column_index[mp.time_column.as_ref().unwrap()];

    let rows = rdr.get_row_iter(None)?;
    for row in rows {
        // println!(" Row {row}");
        let time = row.get_double(time_index).unwrap();

        current_label_values.clear();

        for (_, col) in &label_column_map {
            let label_value = row.get_string(*col)?;
            let value_code = lvm.register_value(label_value.as_str());
            current_label_values.push(value_code);
        }
        let xx = identified_label_values.get_mut(&current_label_values);

        match xx {
            Some(yy) => {
                // println!("Found");
                yy.feed(time, &row)?;
            }
            None => {
                let mut new_lvm = lvm.clone();
                for (label_code, value_code) in std::iter::zip(varying_labels, &current_label_values) {
                    new_lvm.set_value_code_by_key_code(*label_code, *value_code)
                }
                // println!("Created: {new_lvm}");
                let mut yy = mp.metric_type.create_metric_builder(&new_lvm, &name_to_column_index);
                yy.feed(time, &row)?;
                identified_label_values.insert(current_label_values.clone(), yy);
            }
        }
    }

    for il in identified_label_values.values_mut() {
        // println!("More time series! {il:?}");
        loop {
            match il.generate_next()? {
                None => break,
                Some(ts) => {
                    // println!("Generated one: {ts}");
                    all_timeseries.add(ts)?;
                }
            }
        }
    }

    Ok(())
}


fn split_and_clean(name: &str) -> (&str, String) {
    let path_and_name: Vec<&str> = name.rsplitn(2, "/").collect();
    let (prefix, suffix) = match path_and_name.len() {
        1 => ("__root__", path_and_name[0]),
        2 => (path_and_name[1], path_and_name[0]),
        _ => panic!("Split failed")
    };

    let cleaned = clean_metric_name(suffix);
    // println!("     {prefix} - {suffix} - {cleaned}");
    (prefix, cleaned)
}

#[cfg(test)]
mod test {
    use crate::metricstore::BUCKET_NAME;

    #[test]
    fn test_le_regex_int() {
        let re = &BUCKET_NAME;
        let res = re.find("Le0");
        assert!(res.is_some());

        let capt = re.captures("Le0");
        assert!(capt.is_some());
        let capt1 = capt.unwrap().get(1);
        assert!(capt1.is_some());
        assert_eq!(capt1.unwrap().as_str(), "0");
    }

    #[test]
    fn test_le_regex_float() {
        let re = &BUCKET_NAME;
        let res = re.find("Le0.0");
        assert!(res.is_some());

        let capt = re.captures("Le0.0");
        assert!(capt.is_some());
        let capt1 = capt.unwrap().get(1);
        assert!(capt1.is_some());
        assert_eq!(capt1.unwrap().as_str(), "0.0");
    }

    #[test]
    fn test_le_regex_float_exponent() {
        let re = &BUCKET_NAME;
        let res = re.find("Le1.0E-4");
        assert!(res.is_some());

        let capt = re.captures("Le1.0E-4");
        assert!(capt.is_some());
        let capt1 = capt.unwrap().get(1);
        assert!(capt1.is_some());
        assert_eq!(capt1.unwrap().as_str(), "1.0E-4");
    }

    #[test]
    fn test_le_regex_float_inf() {
        let re = &BUCKET_NAME;
        let res = re.find("Le+Inf");
        assert!(res.is_some());

        let capt = re.captures("Le+Inf");
        assert!(capt.is_some());
        let capt1 = capt.unwrap().get(1);
        assert!(capt1.is_some());
        assert_eq!(capt1.unwrap().as_str(), "+Inf");
    }
}