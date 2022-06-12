use std::collections::{HashMap, HashSet};
use std::fmt;
use std::fmt::{Display, Formatter, Write};
use std::ops::Deref;
use std::sync::Arc;

use color_eyre::eyre::eyre;
use serde::{Serialize, Serializer};
use serde::ser::{SerializeMap, SerializeSeq};

use crate::{EyreResult, LabelValueMap, RegularTimeRange};
use crate::labels::{LabelAndValueUniverse, LabelCode, NAME_CODE};
use crate::query::{LabelFilterable, LabelMatcher, LabelMatchMatcher, LabelMatchOperator};
use crate::timevectors::{Duration, IndexLocated, TimeRange, TimeStamp, VecTimeRange};

#[derive(Clone, Debug)]
pub struct TimeSeries {
    time_range: Arc<VecTimeRange>,
    values: Arc<Vec<f64>>
}


impl TimeSeries {
    /// This method consumes its parameters, beware!
    pub fn new(time_range: Arc<VecTimeRange>, values: Arc<Vec<f64>>) -> EyreResult<Self> {
        if time_range.len() != values.len() {
            return Err(eyre!("'time_range' has length {} and 'values' has length {}: they must have the same size", time_range.len(), values.len()))
        }

        Ok(TimeSeries {
            time_range,
            values
        })
    }

    #[inline]
    pub fn len(&self) -> usize {
        self.values.len()
    }

    pub fn clone_values(&self) -> Vec<f64> {
        self.values.deref().clone()
    }

    pub fn timerange(&self) -> &Arc<VecTimeRange> {
        &self.time_range
    }

    pub fn values(&self) -> &Arc<Vec<f64>> {
        &self.values
    }

    pub fn ref_values(&self) -> &Vec<f64> {
        self.values.deref()
    }

    pub fn calculate_resampled_series(&self, sample_range: Arc<VecTimeRange>, offset: usize) -> TimeSeries {
        let offset = offset as Duration;

        let mut results = Vec::with_capacity(sample_range.len());
        for instant_idx in 0..sample_range.len() {
            let instant = sample_range.try_get(instant_idx).unwrap() - offset;
            let value = self.interpolated_at(instant);
            results.push(value);
        }

        TimeSeries::new(sample_range, Arc::new(results)).unwrap()
    }

    pub fn calculate_rate_series(&self, sample_range: Arc<VecTimeRange>, over_range: usize, offset: usize) -> TimeSeries {
        let over_range = over_range as Duration;
        let offset = offset as Duration;

        let mut results = Vec::with_capacity(sample_range.len());

        for instant_idx in 0..sample_range.len() {
            let last_instant = sample_range.try_get(instant_idx).unwrap() - offset;
            let first_instant = last_instant - over_range;

            let value: f64 = self.rate_over_range(first_instant, last_instant);
            results.push(value);
       }

        TimeSeries::new(sample_range, Arc::new(results)).unwrap()
    }

    pub fn calculate_irate_series(&self, sample_range: Arc<VecTimeRange>, over_range: usize, offset: usize) -> TimeSeries {
        let over_range = over_range as Duration;
        let offset = offset as Duration;

        let mut results = Vec::with_capacity(sample_range.len());

        for instant_idx in 0..sample_range.len() {
            let last_instant = sample_range.try_get(instant_idx).unwrap() - offset;
            let first_instant = last_instant - over_range;

            let value: f64 = self.irate_over_range(first_instant, last_instant);
            results.push(value);
        }

        TimeSeries::new(sample_range, Arc::new(results)).unwrap()
    }

    fn rate_over_range(&self, first_instant: TimeStamp, last_instant: TimeStamp) -> f64 {
        if last_instant == first_instant {
            return f64::NAN
        }

        let low = self.interpolated_at(first_instant);
        let high = self.interpolated_at(last_instant);

        (high - low) / (last_instant - first_instant)
    }

    /// Note that this function does not have any reset logic other than rejecting negative rates
    fn irate_over_range(&self, first_instant: TimeStamp, last_instant: TimeStamp) -> f64 {
        if last_instant == first_instant {
            return f64::NAN
        }

        let (above, below) = match self.time_range.locate(last_instant) {
            IndexLocated::NotFound => return f64::NAN,
            IndexLocated::Between(above, below) => (above, below),
            IndexLocated::Above(_) => return f64::NAN,
            IndexLocated::Below(_) => return f64::NAN,
        };

        let beyond_first_instant = self.time_range.try_get(below).unwrap();
        let before_first_instant = self.time_range.try_get(above).unwrap();
        // We might want a minimum delta-t at this point

        let value_above = self.values[above];
        let value_below = self.values[below];


        let previous_instant = match if above > 0 { self.time_range.try_get(above - 1) } else { None } {
            None => {
                let delta_t = beyond_first_instant - before_first_instant;
                let delta_v = value_above - value_below;
                if delta_v < 0.0 {
                    return f64::NAN;
                }

                return delta_v / delta_t
            }
            Some(p) => p
        };

        let previous_value = self.values[above - 1];
        let interpolated_at_last = ((last_instant - before_first_instant) * value_above
                                                + (beyond_first_instant - last_instant) * value_below)
                                                / (beyond_first_instant - before_first_instant);

        let delta_t = last_instant - previous_instant;
        let delta_v = interpolated_at_last - previous_value;
        if delta_v < 0.0 {
            return f64::NAN;
        }

        delta_v / delta_t
    }


    fn interpolated_at(&self, instant: TimeStamp) -> f64 {
        let index = self.time_range.locate(instant);

        match index {
            IndexLocated::NotFound => f64::NAN,

            IndexLocated::Between(low, high) => {
                let time_below = self.time_range.try_get(low).unwrap();
                let value_below = self.values[low];

                let time_above = self.time_range.try_get(high).unwrap();
                let value_above = self.values[high];

                if time_above == time_below {
                    f64::NAN
                }
                else {
                    ((instant - time_below) * value_above + (time_above - instant) * value_below) / (time_above - time_below)
                }
            }
            IndexLocated::Above(idx) => {
                self.values[idx]
            }
            IndexLocated::Below(idx) => {
                self.values[idx]
            }
        }
    }
}

impl Display for TimeSeries {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        let first_time =self.time_range.first().unwrap_or(f64::NAN);
        let last_time =self.time_range.last().unwrap_or(f64::NAN);

        let first_value = *self.values.first().unwrap_or(&f64::NAN);
        let last_value = *self.values.last().unwrap_or(&f64::NAN);

        let len = self.time_range.len() - 2;

        write!(f, "[{first_time} = {first_value} .. {len} more pairs .. {last_time} = {last_value}]")
    }
}

impl Serialize for TimeSeries {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error> where S: Serializer {
        let mut list = serializer.serialize_seq(Some(self.len()))?;

        let mut buffer = String::new();

        for idx in  0..self.len() {
            let time_stamp = self.time_range.try_get(idx).unwrap();
            let value = self.values.get(idx).unwrap().clone();
            buffer.clear();
            write!(&mut buffer, "{}", value).unwrap();
            let elem = (time_stamp, &buffer);
            list.serialize_element(&elem)?;
        }

        list.end()
    }
}

struct TimeSeriesWithOffset<'a> {
    offset: f64,
    time_series: &'a TimeSeries
}

impl Serialize for TimeSeriesWithOffset<'_> {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error> where S: Serializer {
        let mut list = serializer.serialize_seq(Some(self.time_series.len()))?;

        let mut buffer = String::new();

        for idx in  0..self.time_series.len() {
            let time_stamp = self.time_series.time_range.try_get(idx).unwrap() + self.offset;
            let value = self.time_series.values.get(idx).unwrap().clone();
            buffer.clear();
            write!(&mut buffer, "{}", value).unwrap();
            let elem = (time_stamp, &buffer);
            list.serialize_element(&elem)?;
        }

        list.end()
    }
}


#[derive(Debug)]
struct _LabelledTimeSeries {
    label_value_map: LabelValueMap,
    time_series: TimeSeries
}

#[derive(Clone, Debug)]
pub struct LabelledTimeSeries {
    inner: Arc<_LabelledTimeSeries>
}

impl LabelledTimeSeries {
    pub fn new(label_value_map: LabelValueMap, time_series: TimeSeries) -> Self {
        let inner = _LabelledTimeSeries {
            label_value_map,
            time_series
        };

        LabelledTimeSeries {
            inner: Arc::new(inner)
        }
    }

    pub fn label_value_map(&self) -> &LabelValueMap {
        &self.inner.label_value_map
    }

    pub fn time_series(&self) -> &TimeSeries {
        &self.inner.time_series
    }

    pub fn calculate_rate_series(&self, sample_range: Arc<VecTimeRange>, over_range: usize, offset: usize) -> LabelledTimeSeries {
        let labels = self.label_value_map().clone();
        let calculated: TimeSeries = self.time_series().calculate_rate_series(sample_range, over_range, offset);
        LabelledTimeSeries::new(labels, calculated)
    }

    pub fn calculate_irate_series(&self, sample_range: Arc<VecTimeRange>, over_range: usize, offset: usize) -> LabelledTimeSeries {
        let labels = self.label_value_map().clone();
        let calculated: TimeSeries = self.time_series().calculate_irate_series(sample_range, over_range, offset);
        LabelledTimeSeries::new(labels, calculated)
    }

    pub fn resampled(&self, target_time_range: Arc<RegularTimeRange>, offset: usize) -> Self {
        let labels = self.label_value_map().clone();
        let calculated = self.time_series().calculate_resampled_series(target_time_range.realised_timerange(), offset);
        LabelledTimeSeries::new(labels, calculated)
    }
}

impl Display for LabelledTimeSeries {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(f, "labels: {}, values: {}", self.inner.label_value_map, self.inner.time_series)
    }
}


impl Serialize for LabelledTimeSeries {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error> where S: Serializer {
        let mut map = serializer.serialize_map(Some(2))?;
        map.serialize_entry("metric", &self.inner.label_value_map)?;
        map.serialize_entry("values", &self.inner.time_series)?;
        map.end()
    }
}

pub struct LabelledTimeSeriesWithOffset<'a> {
    offset: f64,
    labelled_time_series: &'a LabelledTimeSeries
}

impl <'a> LabelledTimeSeriesWithOffset<'a> {
    pub fn new(offset: f64, labelled_time_series: &'a LabelledTimeSeries) -> Self {
        LabelledTimeSeriesWithOffset{offset, labelled_time_series}
    }
}

impl Serialize for LabelledTimeSeriesWithOffset<'_> {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error> where S: Serializer {
        let mut map = serializer.serialize_map(Some(2))?;
        map.serialize_entry("metric", &self.labelled_time_series.inner.label_value_map)?;
        let tswo = TimeSeriesWithOffset{offset: self.offset, time_series: &self.labelled_time_series.inner.time_series};
        map.serialize_entry("values", &tswo)?;
        map.end()
    }
}




#[derive(Copy, Clone, Debug)]
pub struct MinMaxTime {
    min_time: f64,
    max_time: f64
}

impl MinMaxTime {
    pub fn new() -> Self {
        MinMaxTime::default()
    }

    pub fn observe(&mut self, value: f64) {
        if value.is_finite() {
            if self.min_time.is_nan() || self.min_time > value {
                self.min_time = value
            }

            if self.max_time.is_nan() || self.max_time < value {
                self.max_time = value
            }
        }
    }

    pub fn min_time(&self) -> f64 {
        self.min_time
    }

    pub fn max_time(&self) -> f64 {
        self.max_time
    }
}

impl Default for MinMaxTime {
    fn default() -> Self {
        MinMaxTime {
            max_time: f64::NAN,
            min_time: f64::NAN
        }
    }
}

impl Display for MinMaxTime {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(f, "[{} - {}]", self.min_time, self.max_time)
    }
}

#[derive(Debug)]
pub struct TimeSeriesCollection {
    all_time_series: HashMap<LabelCode, Vec<LabelledTimeSeries>>,
    time_range: MinMaxTime,
    query_time_offset: Option<Duration>, //If, and by how much, the values are seen as shifted into the future
}

impl TimeSeriesCollection {
    pub fn new() -> Self {
        TimeSeriesCollection {
            all_time_series: HashMap::new(),
            time_range: MinMaxTime::new(),
            query_time_offset: None
        }
    }

    pub fn len(&self) -> usize {
        let vals = self.all_time_series.values();
        vals.fold(0, |acc, x| acc + x.len())
    }

    pub fn query_time_offset(&self) -> Option<Duration> {
        self.query_time_offset
    }

    pub fn set_query_start_time(&mut self, query_start_time: TimeStamp) {
        let offset = query_start_time - self.time_range.min_time;
        self.query_time_offset = Some(offset);
    }

    pub fn time_range(&self) -> &MinMaxTime {
        &self.time_range
    }

    pub fn add(&mut self, series: LabelledTimeSeries) -> EyreResult<()> {
        let range = &series.time_series().time_range;

        if let Some(first) = range.first() {
            self.time_range.observe(first);
            self.time_range.observe(range.last().unwrap());
        }

        let code = match series.label_value_map().metric_name_code() {
            Some(code) => code,
            None => return Err(eyre!("Can't find metric name"))
        };

        match self.all_time_series.get_mut(&code) {
            None => {
                let new_vec = vec![series];
                self.all_time_series.insert(code, new_vec);
            }
            Some(vals) => vals.push(series)
        }

        Ok(())
    }

    pub fn filter_by_name(&self, name: &str) -> Vec<LabelledTimeSeries> {
        let universe = LabelAndValueUniverse::global();

        if let Some(code) = universe.value_universe().code_for(name) {
            if let Some(found) = self.all_time_series.get(&code) {
                return found.iter().cloned().collect::<Vec<_>>();
            }
        }

        Vec::with_capacity(0)
    }

    pub fn all_labels(&self) -> Vec<String> {
        let mut label_codes = HashSet::<LabelCode>::new();
        for all_series in self.all_time_series.values() {
            for series in all_series {
                series.label_value_map().for_keys(|k| { label_codes.insert(k); })
            }
        }

        let universe = LabelAndValueUniverse::global().label_universe();

        let mut result = label_codes
            .iter()
            .flat_map(|i| universe.process_for_index(*i, |c| c.to_string()))
            .collect::<Vec<String>>();

        result.sort();
        result
    }

    pub fn all_time_series_names(&self) -> Vec<String> {
        let universe = LabelAndValueUniverse::global().value_universe();

        self.all_time_series.keys()
            .flat_map(|k| universe.process_for_index(*k, |s| s.to_string())).collect::<Vec<_>>()
    }
}


impl LabelFilterable for TimeSeriesCollection {

    fn filter_by_labels(&self, matchers: &Vec<&LabelMatcher>) -> EyreResult<Vec<LabelledTimeSeries>> {
        let universe = LabelAndValueUniverse::global();
        let name_matchers = matchers.iter().filter(|m| m.label_code == NAME_CODE).copied().collect::<Vec<_>>();

        let name_filtered = match name_matchers.len() {
            1 => {
                let matcher = name_matchers[0];
                match matcher.operator {
                    LabelMatchOperator::NoMatch => return Ok(vec![]),
                    LabelMatchOperator::Equals(ref label_value) => {
                        let label_value_code = match universe.value_universe().code_for(label_value) {
                            None => return Ok(vec![]),
                            Some(code) => code
                        };

                        if let Some(found) = self.all_time_series.get(&label_value_code) {
                            found.iter().cloned().collect::<Vec<_>>()
                        }
                        else {
                            vec![]
                        }
                    }

                    LabelMatchOperator::NotEquals(_) => return Err(eyre!("The name filter cannot be a not-equals filter, only equals allowed")),
                    LabelMatchOperator::RegexMatch(_) => return Err(eyre!("The name filter cannot be a regex filter, only equals allowed")),
                    LabelMatchOperator::NotRegexMatch(_) => return Err(eyre!("The name filter cannot be a not-regex filter, only equals allowed")),
                }
            },

            n => return Err(eyre!("Having a single label name filter is mandatory, got {n}"))
        };

        let non_name_matchers = matchers.iter().filter(|m| m.label_code != NAME_CODE).copied().collect::<Vec<_>>();

        if non_name_matchers.is_empty() {
            return Ok(name_filtered)
        }

        let all_filtered = name_filtered.iter()
            .filter(|lts| lts.label_value_map().matches_labels(&non_name_matchers)).cloned().collect::<Vec<_>>();

        Ok(all_filtered)
    }
}


#[cfg(test)]
mod test {
    use serde_json::to_vec_pretty;

    use crate::timeseries::*;
    use crate::timevectors::{TimeRangeIter, TimeStamp};

    #[test]
    fn test_good_vec_time_range() {
        let times: Vec<TimeStamp> = vec![1.0, 2.0, 3.0];
        let tr = VecTimeRange::from_iter_ref(times.iter()).unwrap();
        let new_times: Vec<TimeStamp> = TimeRangeIter::new(&tr).collect();
        assert_eq!(times, new_times)
    }

    #[test]
    fn test_bad_vec_time_range() {
        let times: Vec<TimeStamp> = vec![1.0, 1.0, 3.0];
        let result = VecTimeRange::from_iter_ref(times.iter());
        assert!(result.is_err());
        println!("Result = {}", result.err().unwrap());
    }

    #[test]
    fn test_time_series() {
        let times: Vec<TimeStamp> = vec![100.0, 115.0, 120.0];
        let result = VecTimeRange::from_iter_ref(times.iter()).unwrap();
        let result = Arc::new(result);

        let ts = TimeSeries::new(result, Arc::new(vec![1.0, 2.0, 3.0])).unwrap();

        assert_eq!(ts.len(), 3);
        let val = to_vec_pretty(&ts).unwrap();

        println!("Result {}", String::from_utf8(val).unwrap());
    }

    #[test]
    fn test_time_series_interpolate() {
        let times: Vec<TimeStamp> = vec![100.0, 115.0, 120.0];
        let result = VecTimeRange::from_iter_ref(times.iter()).unwrap();
        let result = Arc::new(result);

        let ts = TimeSeries::new(result, Arc::new(vec![1.0, 2.0, 3.0])).unwrap();

        let interp = ts.interpolated_at(80.0);
        assert_eq!(interp, 1.0);

        let interp = ts.interpolated_at(90.0);
        assert_eq!(interp, 1.0);

        let interp = ts.interpolated_at(100.0);
        assert_eq!(interp, 1.0);

        let interp = ts.interpolated_at(115.0);
        assert_eq!(interp, 2.0);

        let interp = ts.interpolated_at(120.0);
        assert_eq!(interp, 3.0);

        let interp = ts.interpolated_at(130.0);
        assert_eq!(interp, 3.0);

        //True interpolations
        let interp = ts.interpolated_at(107.5);
        assert_eq!(interp, 1.5);

        let interp = ts.interpolated_at(116.0);
        assert_eq!(interp, 2.2);

        let interp = ts.interpolated_at(117.0);
        assert_eq!(interp, 2.4);

        let interp = ts.interpolated_at(118.0);
        assert_eq!(interp, 2.6);

        let interp = ts.interpolated_at(119.0);
        assert_eq!(interp, 2.8);
    }

    #[test]
    fn test_labelled_time_series() {
        let times: Vec<TimeStamp> = vec![100.0, 115.0, 120.0];
        let result = VecTimeRange::from_iter_ref(times.iter()).unwrap();
        let result = Arc::new(result);

        let ts = TimeSeries::new(result, Arc::new(vec![1.0, 2.0, 3.0])).unwrap();

        let mut lvm = LabelValueMap::new();
        lvm.set_value("__name__", "my_duration_seconds");
        lvm.set_value("host", "troubles");

        let lts = LabelledTimeSeries::new(lvm, ts);

        let val = to_vec_pretty(&lts).unwrap();

        println!("Result {}", String::from_utf8(val).unwrap());
    }

    #[test]
    fn test_min_max_time() {
        let mut mmt = MinMaxTime::new();
        assert!(mmt.min_time().is_nan());
        assert!(mmt.max_time().is_nan());

        mmt.observe(42.0);
        assert_eq!(mmt.min_time(), 42.0);
        assert_eq!(mmt.max_time(), 42.0);

        mmt.observe(147.0);
        assert_eq!(mmt.min_time(), 42.0);
        assert_eq!(mmt.max_time(), 147.0);

        mmt.observe(-1.0);
        assert_eq!(mmt.min_time(), -1.0);
        assert_eq!(mmt.max_time(), 147.0);

        mmt.observe(0.0);
        assert_eq!(mmt.min_time(), -1.0);
        assert_eq!(mmt.max_time(), 147.0);
    }
}