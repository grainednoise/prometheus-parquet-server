use std::collections::HashMap;
use std::fmt::{Display, Formatter};
use std::ops::Deref;
use std::sync::Arc;

use color_eyre::eyre::eyre;
use itertools::Itertools;
use promql::*;
use regex::Regex;

use crate::{EyreResult, LabelValueMap};
use crate::labels::{LabelAndValueUniverse, LabelCode};
use crate::QueryResult::{OriginalCollection, Processed, ProcessedVector, Scalar, Unprocessed, UnprocessedVector};
use crate::timeseries::{LabelledTimeSeries, TimeSeries, TimeSeriesCollection};
use crate::timevectors::{RegularTimeRange, TimeRange};

pub trait LabelFilterable {
    fn filter_by_labels(&self, matchers: &Vec<&LabelMatcher>) -> EyreResult<Vec<LabelledTimeSeries>>;

    fn resampled_by(&self) -> Option<&RegularTimeRange> {
        None
    }

    fn offset(&self) -> i32 {
        0
    }
}

impl LabelFilterable for Vec<LabelledTimeSeries> {
    fn filter_by_labels(&self, matchers: &Vec<&LabelMatcher>) -> EyreResult<Vec<LabelledTimeSeries>> {
        let res = self
            .iter()
            .filter(|lts| { lts.label_value_map().matches_labels(matchers)})
            .cloned()
            .collect::<Vec<_>>();

        Ok(res)
    }
}

#[derive(Debug, Clone)]
pub enum QueryResult {
    OriginalCollection{time_series: Arc<TimeSeriesCollection>, target_time_range: Arc<RegularTimeRange>},
    Unprocessed{time_series: Vec<LabelledTimeSeries>, target_time_range: Arc<RegularTimeRange>, offset: Option<usize>},
    UnprocessedVector{time_series: Vec<LabelledTimeSeries>, target_time_range: Arc<RegularTimeRange>, over_range: usize, offset: Option<usize>},
    Processed{time_series: Vec<LabelledTimeSeries>, target_time_range: Arc<RegularTimeRange>},
    ProcessedVector{time_series: Vec<LabelledTimeSeries>, target_time_range: Arc<RegularTimeRange>, over_range: usize},
    Scalar(f64),
}

impl QueryResult {
    pub fn from_original_collection(time_series: Arc<TimeSeriesCollection>, target_time_range: Arc<RegularTimeRange>) -> Self {
        OriginalCollection { time_series, target_time_range }
    }

    pub fn from_processed(time_series: Vec<LabelledTimeSeries>, target_time_range: Arc<RegularTimeRange>) -> Self {
        Processed { time_series, target_time_range }
    }

    pub fn from_scalar(scalar: f64) -> Self {
        Scalar(scalar)
    }

    pub fn create_as_unprocessed(time_series: Vec<LabelledTimeSeries>, target_time_range: Arc<RegularTimeRange>, over_range: Option<usize>, offset: Option<usize>) -> Self {
        match over_range {
            None => Unprocessed {time_series, target_time_range, offset},
            Some(over_range) => UnprocessedVector {time_series, target_time_range, over_range, offset},
        }
    }

    fn filter_by_labels(&self, matchers: &Vec<&LabelMatcher>, over_range: Option<usize>, new_offset:  Option<usize>) -> EyreResult<Self> {
        let result = match self {
            OriginalCollection { time_series, target_time_range } => {
                let filtered = time_series.filter_by_labels(matchers)?;
                QueryResult::create_as_unprocessed(filtered, target_time_range.clone(), over_range, new_offset)
            }
            Unprocessed{ time_series, target_time_range, offset} => {
                let filtered = time_series.filter_by_labels(matchers)?;
                let verified_offset = match (new_offset, offset) {
                    (None, None) => None,
                    (Some(v1), None) => Some(v1),
                    (None, Some(v2)) => Some(*v2),
                    (Some(_), Some(_)) => return Err(eyre!("Trying to apply multiple offsets to series"))
                };

                QueryResult::create_as_unprocessed(filtered, target_time_range.clone(), over_range, verified_offset)
            }

            Processed { time_series, target_time_range } => {
                let filtered = time_series.filter_by_labels(matchers)?;

                match over_range {
                    Some(_) => return Err(eyre!("Cannot vectorize a re-sampled series")),
                    None => Processed { time_series: filtered, target_time_range: target_time_range.clone() }
                }
            }

            UnprocessedVector {..} => return Err(eyre!("Cannot re-filter a vectorized raw series")),
            ProcessedVector {..} => return Err(eyre!("Cannot re-filter a vectorized resampled series")),

            Scalar(_) => {
                return Err(eyre!("A scalar cannot be filtered"))
            }
        };

        Ok(result)
    }

    pub fn resampled(self) -> EyreResult<Self> {
        match self {
            OriginalCollection { .. } => return Err(eyre!("Cannot resample all data")),
            Processed { .. } => Ok(self),
            Scalar(_) => Ok(self),
            Unprocessed{time_series: data, target_time_range, offset} => {
                let offset = offset.unwrap_or(0);
                let new_series = data.iter()
                    .map(|ts| ts.resampled(target_time_range.clone(), offset))
                    .collect::<Vec<_>>();

                Ok(Processed { time_series: new_series, target_time_range: target_time_range.clone() })
            }
            UnprocessedVector {..} | ProcessedVector {..} => return Err(eyre!("Cannot resample a vectorized time series"))
        }
    }
}

impl Display for QueryResult {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "QueryResultType(")?;

        match self {
            OriginalCollection { time_series, .. } => {
                write!(f, "filtered, having {} time series", time_series.len())?;
            }

            Unprocessed { time_series, target_time_range: _target_time_range, offset } => {
                match offset {
                    None => write!(f, "filtered, having {} time series", time_series.len())?,
                    Some(offset) =>  write!(f, "filtered, having {} time series, offset {}", time_series.len(), offset)?
                }
            }

            UnprocessedVector { time_series, target_time_range: _target_time_range, over_range, offset} => {
                match offset {
                    None => write!(f, "filtered, having {} time series, vectorized to range {}", time_series.len(), over_range)?,
                    Some(offset) => write!(f, "filtered, having {} time series, vectorized to range {}, offset {}", time_series.len(), over_range, offset)?
                }
            }

            Processed { time_series, .. } => {
                let elements = time_series.first().map(|v| v.time_series().len()).unwrap_or(0);

                write!(f, "filtered and resampled, having {} time series of {} elements", time_series.len(), elements)?;

                if let Some(label_code)= time_series.first().map(|v| v.label_value_map().metric_name_code()).flatten() {
                    LabelAndValueUniverse::global().value_universe()
                        .process_for_index(label_code, |s| write!(f, " first name '{}'", s))
                        .unwrap_or(Ok(()))?;
                }
            }

            ProcessedVector { time_series, target_time_range: _target_time_range, over_range} => {
                write!(f, "filtered/resampled, having {} time series, vectorized to range {}", time_series.len(), over_range)?
            }

            Scalar(scalar) => {
                write!(f, "Scalar({scalar})")?;
            }
        }

        write!(f, ")")
    }
}


pub enum LabelMatchOperator {
    NoMatch,
    Equals(String),
    NotEquals(String),
    RegexMatch(Regex),
    NotRegexMatch(Regex)
}

impl LabelMatchOperator {
    pub fn matches(&self, value: &str) -> bool {
        match self {
            LabelMatchOperator::NoMatch => false,
            LabelMatchOperator::Equals(eq_value) => eq_value == value,
            LabelMatchOperator::NotEquals(neq_value) => neq_value != value,
            LabelMatchOperator::RegexMatch(pat) => pat.is_match(value),
            LabelMatchOperator::NotRegexMatch(not_pat) => !not_pat.is_match(value),
        }
    }
}

pub struct LabelMatcher {
    pub label_code: LabelCode,
    pub operator: LabelMatchOperator
}

impl LabelMatcher {
    pub fn from_labelmatch(label_match: &LabelMatch) -> EyreResult<Self> {
        let universe = LabelAndValueUniverse::global();
        let label_code = match universe.label_universe().code_for(&label_match.name) {
            None => return Ok(LabelMatcher {
                label_code: 0,
                operator: LabelMatchOperator::NoMatch
            }),

            Some(code) => code
        };

        let operator = match label_match.op {
            LabelMatchOp::Eq => LabelMatchOperator::Equals(label_match.value.clone()),
            LabelMatchOp::Ne => LabelMatchOperator::NotEquals(label_match.value.clone()),
            LabelMatchOp::REq => LabelMatchOperator::RegexMatch(Regex::new(&label_match.value)?),
            LabelMatchOp::RNe => LabelMatchOperator::NotRegexMatch(Regex::new(&label_match.value)?)
        };

        Ok(LabelMatcher {
            label_code,
            operator
        })
    }
}


pub trait LabelMatchMatcher {
    fn matches_labels(&self, matchers: &Vec<&LabelMatcher>) -> bool;
}


fn generate_matchers<'a, I:Iterator<Item=&'a LabelMatch>>(iter: I) -> EyreResult<Vec<LabelMatcher>> {
    let mut result = Vec::new();
    for l in iter {
        let converted = LabelMatcher::from_labelmatch(l)?;
        result.push(converted)
    }

    Ok(result)
}

fn process_function(name: &str, args: &Vec<Node>, aggregation: &Option<AggregationMod>, query_result: QueryResult) ->  EyreResult<QueryResult> {
    // println!("Function\n    name={name}\n    args={args:?}\n    aggregation={aggregation:?}");

    match name {
        "rate" => handle_rate_function(args, aggregation, query_result),
        "irate" => handle_irate_function(args, aggregation, query_result),
        "sum" =>  handle_sum_function(args, aggregation, query_result),
        "histogram_quantile" => handle_histogram_quantile_function(args, aggregation, query_result),

        other => {
            Err(eyre!("Cannot handle function '{other}'"))
        }
    }
}


fn handle_histogram_quantile_function(args: &Vec<Node>, aggregation: &Option<AggregationMod>, query_result: QueryResult) -> EyreResult<QueryResult> {
    if args.len() != 2 {
        return Err(eyre!("'histogram_quantile' needs exactly 2 arguments, got {}", args.len()))
    };

    if aggregation.is_some() {
        return Err(eyre!("'histogram_quantile' expects no aggregation, got {:?}", aggregation))
    }

    let number_arg = process_ast(&args[0], query_result.clone())?;
    let number = match number_arg {
        Scalar(scalar) => scalar,
        _ => return Err(eyre!("Expected a scalar"))
    };

    let series_arg = process_ast(&args[1], query_result)?;
    let series_arg = series_arg.resampled()?;

    let (series, target_time_range) = match series_arg {
        Processed { time_series, target_time_range } => (time_series, target_time_range),
        _ => return Err(eyre!("resampling failed"))
    };

    // println!("Scalar = {number}, QR = {} series", series.len());

    let univ = LabelAndValueUniverse::global();
    let le_code = match univ.label_universe().code_for("Le") {
        None => return Err(eyre!("No label 'Le'")),
        Some(code) => code
    };

    let mut groups =  HashMap::<LabelValueMap, Vec<(f64, TimeSeries)>>::new();

    //Now we want to group by all (remaining) labels *minus* "Le"
    for item in series {
        let lvm = item.label_value_map();
        let (new_lvm, removed) = lvm.without(le_code);
        let removed = match removed {
            None => {
                println!("Series has no 'Le' label, ignoring");
                continue
            }
            Some(removed) => removed
        };

        let number_le = univ.to_number_value(removed)?;

        let lst = groups.entry(new_lvm).or_insert_with(|| Vec::new());
        lst.push((number_le, item.time_series().clone()))
    }

    let mut calculated_labelled_time_series = Vec::<LabelledTimeSeries>::with_capacity(groups.len());

    for (lvm, mut series) in groups.drain() {
        series.sort_by(|a, b| a.0.partial_cmp(&b.0).unwrap());
        // println!("Series for {lvm}: {series:?}");
        let length = series.len();
        let (inf_val, inf_series) = &series[length - 1];

        if *inf_val != f64::INFINITY {
            return Err(eyre!("Inf bucket has finite value: {inf_val}"))
        }

        let time_range = inf_series.timerange().clone();
        let mut result = Vec::<f64>::with_capacity(time_range.len());

        for idx in 0..time_range.len() {
            let total_count = inf_series.values()[idx];
            let cutoff = number * total_count;

            let mut estimated_value_for_quantile = 0.0;
            let mut previous_bucket_count = 0.0;
            let mut previous_bucket_val = 0.0;

            for (bucket_val, bucket_series) in &series[0..length - 1] {
                let bucket_count = bucket_series.values()[idx];
                if bucket_count >= cutoff {
                    let oversize = bucket_count - cutoff;
                    estimated_value_for_quantile = bucket_val - (bucket_val - previous_bucket_val) * oversize / (bucket_count - previous_bucket_count);
                    break
                }

                previous_bucket_count = bucket_count;
                previous_bucket_val = *bucket_val;
                estimated_value_for_quantile = *bucket_val;
            }

            result.push(estimated_value_for_quantile)
        }

        let new_time_series = TimeSeries::new(time_range, Arc::new(result))?;
        let new_lts = LabelledTimeSeries::new(lvm, new_time_series);
        calculated_labelled_time_series.push(new_lts);
    }

    let qrt = QueryResult::from_processed(calculated_labelled_time_series, target_time_range.clone());
    Ok(qrt)
}

fn handle_rate_function(args: &Vec<Node>, aggregation: &Option<AggregationMod>, query_result: QueryResult) -> EyreResult<QueryResult> {
    if aggregation.is_some() {
        return Err(eyre!("'rate' expects no aggregation, got {:?}", aggregation))
    }

    if args.len() != 1 {
        return Err(eyre!("'rate' expects a single argument, got {}", args.len()))
    }

    let argument = process_ast(&args[0], query_result)?;

    return match argument {
        OriginalCollection { .. }
                => Err(eyre!("'rate' cannot process the unfiltered collection")),

        Processed { .. } | ProcessedVector {..}
                => Err(eyre!("'rate' cannot process the processed yet")),

        Unprocessed {..} =>
            return Err(eyre!("'rate' requires a vectorized time series")),

        UnprocessedVector {time_series, target_time_range, over_range, offset} => {
            let offset = offset.unwrap_or(0);

            let target_time_range_vec = target_time_range.realised_timerange();
            let mut results = Vec::with_capacity(target_time_range.len());

            for lts in time_series {
                let rate_series = lts.calculate_rate_series(target_time_range_vec.clone(), over_range, offset);
                results.push(rate_series);
            }

            Ok(QueryResult::from_processed(results, target_time_range.clone()))
        }
        Scalar(_) => {
            Err(eyre!("The rate over a scalar is not meaningful"))
        }
    }
}

fn handle_irate_function(args: &Vec<Node>, aggregation: &Option<AggregationMod>, query_result: QueryResult) -> EyreResult<QueryResult> {
    if aggregation.is_some() {
        return Err(eyre!("'rate' expects no aggregation, got {:?}", aggregation))
    }

    if args.len() != 1 {
        return Err(eyre!("'rate' expects a single argument, got {}", args.len()))
    }

    let argument = process_ast(&args[0], query_result)?;

    return match argument {
        OriginalCollection { .. }
        => Err(eyre!("'rate' cannot process the unfiltered collection")),

        Processed { .. } | ProcessedVector {..}
        => Err(eyre!("'rate' cannot process the processed yet")),

        Unprocessed {..} =>
            return Err(eyre!("'rate' requires a vectorized time series")),

        UnprocessedVector {time_series, target_time_range, over_range, offset} => {
            let offset = offset.unwrap_or(0);

            let target_time_range_vec = target_time_range.realised_timerange();
            let mut results = Vec::with_capacity(target_time_range.len());

            for lts in time_series {
                let rate_series = lts.calculate_irate_series(target_time_range_vec.clone(), over_range, offset);
                results.push(rate_series);
            }

            Ok(QueryResult::from_processed(results, target_time_range.clone()))
        }
        Scalar(_) => {
            Err(eyre!("The rate over a scalar is not meaningful"))
        }
    }
}


pub struct GroupBy {
    groups: HashMap::<LabelValueMap, Vec<TimeSeries>>,
    target_time_range: Arc<RegularTimeRange>
}

impl Display for GroupBy {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "GroupBy({} groups\n", self.groups.len())?;

        for (k, v) in &self.groups {
            write!(f, " - {} has {} series\n", k, v.len())?;
        }

        write!(f, ")\n")
    }
}

impl GroupBy {
    fn construct(query_result: QueryResult, aggregation: &AggregationMod) -> EyreResult<Self> {
        let univ = LabelAndValueUniverse::global();

        let label_codes = match aggregation.action {
            AggregationAction::Without => {
                return Err(eyre!("not implemented"))
            }
            AggregationAction::By => {
                // We're silently ignoring any labels we don't know of
                aggregation.labels.iter()
                    .map(|v| univ.label_universe().code_for(v))
                    .flatten()
                    .sorted()
                    .collect::<Vec<_>>()
            }
        };

        let query_result = query_result.resampled()?;

        let (series, target_time_range) = match query_result {
            Processed { time_series, target_time_range } => (time_series, target_time_range),
            _ => return Err(eyre!("resampling failed"))
        };

        let mut groups =  HashMap::<LabelValueMap, Vec<TimeSeries>>::new();

        for item in series {
            let lvm = item.label_value_map();
            let new_lvm = lvm.retaining(&label_codes);
            let lst = groups.entry(new_lvm).or_insert_with(|| Vec::new());
            lst.push(item.time_series().clone())
        }

        Ok(GroupBy {
            groups,
            target_time_range: target_time_range.clone()
        })
    }
}


fn handle_sum_function(args: &Vec<Node>, aggregation: &Option<AggregationMod>, query_result: QueryResult) -> EyreResult<QueryResult> {
    if args.len() != 1 {
        return Err(eyre!("'rate' expects a single argument, got {}", args.len()))
    }

    let argument = process_ast(&args[0], query_result)?;
    println!("Inner query result = {argument}");

    match aggregation {
        None =>  Err(eyre!("unimplemented")),
        Some(agg) => {
            // println!("Aggregation: {agg:?}");

            let groups = GroupBy::construct(argument, agg)?;
            // println!("Found groups: {}", groups);
            let target_time_range = groups.target_time_range;

            let mut all_series = Vec::new();
            for (labels, series) in &groups.groups {
                match series.len() {
                    0 => {},
                    1 => {
                        let s = series[0].clone();
                        // println!("Single value sum: {:?}", s);
                        let lts = LabelledTimeSeries::new(labels.clone(), s);
                        all_series.push(lts);
                    },
                    n => {
                        // println!("Sum has {n} parts");
                        let mut accumulator = series[0].clone_values();
                        for series_idx in 1..n {
                            let to_add = series[series_idx].ref_values();
                            for value_idx in 0..accumulator.len() {
                                let v1 = accumulator.get_mut(value_idx).unwrap();
                                *v1 += to_add[value_idx];
                            }
                        }

                        let ts = TimeSeries::new(target_time_range.realised_timerange(), Arc::new(accumulator))?;

                        let lts = LabelledTimeSeries::new(labels.clone(), ts);
                        all_series.push(lts);
                    }
                }
            }
            Ok(QueryResult::from_processed(all_series, target_time_range))
        }
    }
}

fn process_ast(ast: &Node, query_result: QueryResult) -> EyreResult<QueryResult> {
    match ast {
        Node::Operator { .. } => {
            Err(eyre!("Not implemented yet"))
        }
        Node::Vector(ref vect) => {
            let labels = &vect.labels;
            let over_range = vect.range.clone();
            let offset = vect.offset.clone();

            let matchers = generate_matchers(labels.iter())?;
            let ref_matchers = matchers.iter().collect::<Vec<_>>();
            let new_result = query_result.filter_by_labels(&ref_matchers, over_range, offset)?;
            // println!("Filtered {new_result:?}");
            Ok(new_result)
        }

        Node::Scalar(scalar) => {
            Ok(QueryResult::from_scalar(*scalar as f64))
        }

        Node::String(_) => {
            Err(eyre!("Not implemented yet"))
        }

        Node::Function { ref name, ref args, ref aggregation } => {
            process_function(name, args, aggregation, query_result)
        }

        Node::Negation(_) => {
            Err(eyre!("Not implemented yet"))
        }
    }
}

pub fn run_query(query: &str, time_series: Arc<TimeSeriesCollection>, range: Arc<RegularTimeRange>) -> EyreResult<QueryResult>{
    println!("Query: {}", query);

    let ast = match parse(query.as_bytes(), false) {
        Ok(ast) => ast,
        Err(e) => return Err(eyre!("Error parsing query: {}", e.to_string()))
    };

    let qr = QueryResult::from_original_collection(time_series, range.clone());
    let result = process_ast(&ast, qr)?;
    // println!("QueryResult {}", result);

    Ok(result.resampled()?)
}