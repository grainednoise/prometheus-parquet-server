use std::fmt::Formatter;
use std::str::FromStr;

use chrono::DateTime;
use lazy_static::lazy_static;
use regex::Regex;
use serde::{Deserialize, Deserializer};
use serde::de::{Error, Visitor};

use crate::{eyre, EyreResult, timevectors};

#[derive(Debug, Clone, Copy)]
pub struct Step(f64);

impl From<Step> for timevectors::Duration {
    fn from(st: Step) -> Self {
        st.0
    }
}

impl <'de> Deserialize<'de> for Step {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error> where D: Deserializer<'de> {
        deserializer.deserialize_str(StepVisitor{})
    }
}


struct StepVisitor;

lazy_static! {
    static ref LABELLED_DURATION: Regex = Regex::new(r"^(\d+)(ms|s|m|h|d|w|y)").unwrap();
}


fn parse_partial_duration(value: &str) -> EyreResult<(f64, usize)> {
    let matched = LABELLED_DURATION.captures(value);

    let matched_so_far = match matched {
        None => return Err(eyre!("Cannot find num + unit")),
        Some(m) => m
    };

    let base_num = u64::from_str(matched_so_far.get(1).unwrap().as_str())? as f64;
    let multiplier = match matched_so_far.get(2).unwrap().as_str() {
        "ms" => 0.001,
        "s" => 1.0,
        "m" => 60.0,
        "h" => 3600.0,
        "d" => 3600.0 * 24.0,
        "w" => 3600.0 * 24.0 * 7.0,
        "y" => 3600.0 * 24.0 * 365.0,

        unit => return  Err(eyre!("Bad unit '{unit}'"))
    };

    let consumed = matched_so_far.get(0).unwrap().end();
    Ok((base_num * multiplier, consumed))
}

fn parse_duration(value: &str) -> EyreResult<f64> {
    let value = value.trim();

    if value.len() == 0 {
        return Err(eyre!("empty string"))
    }

    if let Ok(val) = f64::from_str(value) {
        return Ok(val)
    }

    let mut current_val = value;
    let mut total = 0.0;

    loop {
        let (val, consumed) = parse_partial_duration(current_val)?;
        total += val;

        if consumed >= current_val.len() {
            break
        }
        current_val = &current_val[consumed..];
    }

    return Ok(total)
}


impl<'de> Visitor<'de> for StepVisitor {
    type Value = Step;

    fn expecting(&self, formatter: &mut Formatter) -> std::fmt::Result {
        formatter.write_str("a step size")
    }

    fn visit_str<E>(self, v: &str) -> Result<Self::Value, E> where E: Error {
        match parse_duration(v) {
            Ok(result) => Ok(Step(result)),
            Err(err) => Err(E::custom(format_args!("Cannot parse step: {err}")))
        }
    }
}

#[derive(Debug, Clone, Copy)]
pub struct TimeStamp(f64);

impl From<TimeStamp> for timevectors::TimeStamp {
    fn from(ts: TimeStamp) -> Self {
        ts.0
    }
}

impl <'de> Deserialize<'de> for TimeStamp {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error> where D: Deserializer<'de> {
        deserializer.deserialize_str(TimeStampVisitor {})
    }
}

struct TimeStampVisitor;

impl<'de> Visitor<'de> for TimeStampVisitor {
    type Value = TimeStamp;

    fn expecting(&self, formatter: &mut Formatter) -> std::fmt::Result {
        formatter.write_str("an RFC3339 or Epoch timestamp")
    }

    fn visit_str<E>(self, v: &str) -> Result<Self::Value, E> where E: Error {

        if let Ok(val) = f64::from_str(v) {
            return Ok(TimeStamp(val))
        }

        let raw = match DateTime::parse_from_rfc3339(v) {
            Ok(t) => t,
            Err(err) => return Err(E::custom(format_args!("Cannot parse time: {err}")))
        };

        let epoch = raw.timestamp_millis() as f64 / 1000.0;
        Ok(TimeStamp(epoch))
    }
}


#[derive(Deserialize, Debug)]
pub struct RangeQuery {
    pub query: String,
    pub start: TimeStamp,
    pub end: TimeStamp,
    pub step: Step,
}

#[derive(Deserialize, Debug)]
pub struct InstantQuery {
    pub query: String,
    pub time: TimeStamp,
}


#[cfg(test)]
mod test {
    use serde::de::Visitor;

    use crate::web::parse_duration;

    #[test]
    fn test_timestamp_parse() {
        let parser = super::TimeStampVisitor{};
        let res1: Result<_, serde_json::Error> = parser.visit_str("2022-05-28T00:00:00+02:00");
        println!("Parsed: {res1:?}");
        assert!(res1.is_ok());

        let parser = super::TimeStampVisitor{};
        let res2: Result<_, serde_json::Error> = parser.visit_str("1653688800");
        println!("Parsed: {res2:?}");
        assert!(res2.is_ok());

        let res1: f64 = res1.unwrap().into();
        let res2: f64 = res2.unwrap().into();

        assert_eq!(res1, res2);
    }


    #[test]
    fn test_duration_parse1() {
        let val = parse_duration("1.5").unwrap();
        assert_eq!(1.5, val);

        let val = parse_duration("1s").unwrap();
        assert_eq!(1.0, val);

        let val = parse_duration("500ms").unwrap();
        assert_eq!(0.5, val);

        let val = parse_duration("1s500ms").unwrap();
        assert_eq!(1.5, val);

        let val = parse_duration("2m1s500ms").unwrap();
        assert_eq!(121.5, val);

        let val = parse_duration("1h2m1s500ms").unwrap();
        assert_eq!(3721.5, val);
    }

    #[test]
    fn test_duration_parse2() {
        let val = parse_duration("1..5");
        assert!(val.is_err());

        let val = parse_duration("1x");
        assert!(val.is_err());

        let val = parse_duration("1s3");
        assert!(val.is_err());
    }
}