use std::collections::BTreeMap;
use std::fmt;
use std::fmt::Formatter;
use std::str::FromStr;
use std::sync::{Arc, RwLock};

use color_eyre::eyre::eyre;
use lazy_static::lazy_static;
use regex::Regex;
use serde::{Serialize, Serializer};
use serde::ser::SerializeMap;

use crate::EyreResult;
use crate::query::{LabelMatcher, LabelMatchMatcher};
use crate::stringuniverse::{SimpleStringUniverse, StringCode};

pub type LabelCode = StringCode;

pub const NAME_CODE: usize = 0;
pub const NAME_LABEL: &str = "__name__";

/// The constant NAME_CODE is guaranteed to map to "__name__", i.e NAME_LABEL


#[derive(Clone, Debug)]
pub struct LabelUniverse {
    universe: Arc<RwLock<SimpleStringUniverse>>
}

impl LabelUniverse {
    pub fn new() -> Self {
        let mut universe = SimpleStringUniverse::new();
        universe.get_or_insert(NAME_LABEL);

        LabelUniverse {
            universe: Arc::new(RwLock::new(universe))
        }
    }

    pub fn len(&self) -> usize {
        match self.universe.read() {
            Ok(val) => {
                val.len()
            }
            Err(e) => {
                panic!("poisoned lock: {:?}", e)
            }
        }
    }

    pub fn code_for(&self, label: &str) -> Option<LabelCode> {
        match self.universe.read() {
            Ok(val) => {
                val.code_for(label)
            }
            Err(e) => {
                panic!("poisoned lock: {:?}", e)
            }
        }
    }

    pub fn get_or_insert(&self, label: &str) -> LabelCode {
        match self.universe.write() {
            Ok(mut val) => {
                val.get_or_insert(label)
            }
            Err(e) => {
                panic!("poisoned lock: {:?}", e)
            }
        }
    }

    pub fn process_for_index<F, R>(&self, code: LabelCode, func: F) -> Option<R> where F: FnOnce(&str) -> R {
        match self.universe.read() {
            Ok(val) => {
                val.get_for_index(code).map(func)
            }
            Err(e) => {
                panic!("poisoned lock: {:?}", e)
            }
        }
    }
}

#[derive(Clone, Debug)]
pub struct LabelAndValueUniverse {
    label_universe: LabelUniverse,
    value_universe: LabelUniverse,
}


impl LabelAndValueUniverse {
    fn new() -> Self {
        LabelAndValueUniverse {
            label_universe: LabelUniverse::new(),
            value_universe: LabelUniverse::new()
        }
    }

    pub fn label_universe(&self) -> &LabelUniverse {
        &self.label_universe
    }

    pub fn value_universe(&self) -> &LabelUniverse {
        &self.value_universe
    }

    pub fn global() -> &'static LabelAndValueUniverse {
        &*LABEL_AND_VALUE_UNIVERSE
    }

    pub fn to_number_value(&self, code: LabelCode) -> EyreResult<f64> {

        let val = match self.value_universe.process_for_index(code, |s| {
            f64::from_str(s)
        }) {
            None => return Err(eyre!("Unknown label code")),
            Some(val) => val?
        };

        Ok(val)
    }
}

lazy_static! {
    static ref LABEL_AND_VALUE_UNIVERSE: LabelAndValueUniverse = LabelAndValueUniverse::new();
}

#[derive(Clone, Hash, Eq, PartialEq)]
pub struct LabelValueMap {
    label_value_map: BTreeMap<LabelCode, LabelCode>,
}

 lazy_static! {
    static ref VALID_METRIC_NAME: Regex = Regex::new(r"^[a-zA-Z_][a-zA-Z0-9_]*$").unwrap();
}

pub fn is_valid_metric_name(metric_name: &str) -> bool {
    VALID_METRIC_NAME.is_match(metric_name)
}

pub fn make_metric_name_legal(original: &str) -> String {
    if original.is_empty() {
        panic!("Cannot make empty strings meaningfully legal")
    }

    let mut result = original.to_string();

    if is_valid_metric_name(original) {
        return result
    }

    if result.as_bytes()[0].is_ascii_digit() {
        result.insert(0, '_');
    }

    let mut result = result.into_bytes();

    for ch in result.iter_mut() {
        if !matches!(*ch, b'0'..=b'9' | b'A'..=b'Z' | b'a'..=b'z' | b'_') {
            *ch = b'_'
        }
    }

    match String::from_utf8(result) {
        Ok(s) => s,
        Err(err) => panic!("Should never happen: {err}")
    }
}

impl LabelValueMap {
    pub fn new() -> Self {
        LabelValueMap {
            label_value_map: BTreeMap::default()
        }
    }

    pub fn from_keys_and_values(keys: &Vec<LabelCode>, values: &Vec<Option<LabelCode>>) -> Self {
        let mut res = LabelValueMap::new();
        if keys.len() != values.len() {
            panic!("Length of 'keys' ({}), differs from that of 'values' {}", keys.len(), values.len())
        }

        for (k, v) in itertools::zip(keys, values) {
            if let Some(vv) = v {
                res.set_value_code_by_key_code(k.clone(), vv.clone())
            }
        }

        res
    }

    pub fn retaining(&self, keys: &Vec<LabelCode>) -> Self {
        let mut res = LabelValueMap::new();

        for key in keys {
            if let Some(value) = self.value_code_for_key_code(*key) {
                res.set_value_code_by_key_code(*key, value)
            }
        }

        res
    }

    pub fn without(&self, key: LabelCode) -> (Self, Option<LabelCode>) {
        let mut label_value_map = self.label_value_map.clone();
        let removed = label_value_map.remove(&key);
        (LabelValueMap {label_value_map}, removed)
    }

    pub fn len(&self) -> usize {
        self.label_value_map.len()
    }

    pub fn for_keys<F>(&self, mut func: F) where F: FnMut(LabelCode) {
        for k in self.label_value_map.keys() {
            func(*k)
        }
    }

    pub fn set_value(&mut self, key: &str, value: &str) {
        let code = LABEL_AND_VALUE_UNIVERSE.label_universe.get_or_insert(key);
        self.set_value_by_code(code, value);
    }

    pub fn set_value_by_code(&mut self, key_code: LabelCode, value: &str) {
        let value_code = LABEL_AND_VALUE_UNIVERSE.value_universe.get_or_insert(value);
        self.set_value_code_by_key_code(key_code, value_code);
    }

    #[inline]
    pub fn set_value_code_by_key_code(&mut self, key_code: LabelCode, value_code: LabelCode) {
        self.label_value_map.insert(key_code, value_code);
    }

    pub fn register_label(&mut self, label_name: &str) -> LabelCode {
        LABEL_AND_VALUE_UNIVERSE.label_universe.get_or_insert(label_name)
    }

    pub fn register_value(&mut self, label_name: &str) -> LabelCode {
        LABEL_AND_VALUE_UNIVERSE.value_universe.get_or_insert(label_name)
    }

    pub fn process_for_label_index<F, R>(&self, code: LabelCode, func: F) -> Option<R> where F: FnOnce(&str) -> R {
        LABEL_AND_VALUE_UNIVERSE.label_universe.process_for_index(code, func)
    }

    pub fn process_for_value_index<F, R>(&self, code: LabelCode, func: F) -> Option<R> where F: FnOnce(&str) -> R {
        LABEL_AND_VALUE_UNIVERSE.value_universe.process_for_index(code, func)
    }

    pub fn value_code_for_key_code(&self, key_code: LabelCode) -> Option<LabelCode> {
        self.label_value_map.get(&key_code).copied()
    }

    pub fn metric_name_code(&self) -> Option<LabelCode> {
        self.label_value_map.get(&NAME_CODE).copied()
    }

    pub fn add_suffix_to_metric(&mut self, suffix: &str) {
        let value_code = self.metric_name_code().unwrap();
        let new_name = self.process_for_value_index(value_code, |v| format!("{v}_{suffix}"));
        let checked_name = make_metric_name_legal(new_name.unwrap().as_str());
        self.set_value_by_code(NAME_CODE, checked_name.as_str())
    }




    pub fn display_string(&self) -> String {
        let mut result = String::new();
        let mut buffer ="{".to_string();
        let mut needs_comma = false;
        for (k, v) in &self.label_value_map {
            if *k == NAME_CODE {
                self.process_for_value_index(*v, |vv| result.push_str(vv));
                continue
            }
            if needs_comma {
                buffer.push_str(", ")
            }
            needs_comma = true;

            self.process_for_label_index(*k, |kk| buffer.push_str(kk));
            buffer.push_str("=\"");
            self.process_for_value_index(*v, |vv| buffer.push_str(vv));
            buffer.push('"');
        }
        buffer.push('}');
        result.push_str(buffer.as_str());
        result
    }
}

impl LabelMatchMatcher for LabelValueMap {
    fn matches_labels(&self, matchers: &Vec<&LabelMatcher>) -> bool {
        let universe = LabelAndValueUniverse::global();

        for matcher in matchers {
            let value_code = match self.value_code_for_key_code(matcher.label_code) {
                None => return false,
                Some(code) => code
            };

            if !universe.value_universe().process_for_index(value_code, |s| {
                matcher.operator.matches(s)
            }).unwrap() {
                return false
            }
        }

        true
    }
}


impl fmt::Display for LabelValueMap {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.display_string())
    }
}

impl fmt::Debug for LabelValueMap {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.display_string())
    }
}

impl Serialize for LabelValueMap {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error> where S: Serializer {
        let mut map = serializer.serialize_map(Some(self.len()))?;

        for (k, v) in &self.label_value_map {
           match LABEL_AND_VALUE_UNIVERSE.label_universe.process_for_index(*k, |kk| {
               match LABEL_AND_VALUE_UNIVERSE.value_universe.process_for_index(*v, |vv| {
                   map.serialize_entry(kk, vv)
               }) {
                   None => {unreachable!()}
                   Some(res) => res
               }
           }) {
               None => {unreachable!()}
               Some(res) => res?
           }
        }
        map.end()
    }
}




#[cfg(test)]
mod test {
    use serde_json::to_vec_pretty;

    use crate::labels::{LabelUniverse, LabelValueMap, make_metric_name_legal};

    #[test]
    fn test_label_simple_universe() {
        let lm = LabelUniverse::new();
        lm.get_or_insert("egg");
        assert_eq!(lm.len(), 2);

        assert_eq!(lm.code_for("__name__").unwrap(), 0);
        assert_eq!(lm.code_for("egg").unwrap(), 1);
        assert!(lm.code_for("idontexist").is_none());

        assert!(lm.process_for_index(0, |n| {assert_eq!(n, "__name__"); true}).unwrap());
        assert!(lm.process_for_index(1, |n| {assert_eq!(n, "egg"); true}).unwrap());
        assert!(lm.process_for_index(2, |_| assert!(false)).is_none());
    }

    #[test]
    fn test_label_guarded_universe() {
        let univ = LabelUniverse::new();
        univ.get_or_insert("egg");
        assert_eq!(univ.len(), 2);

        assert_eq!(univ.code_for("__name__").unwrap(), 0);
        assert_eq!(univ.code_for("egg").unwrap(), 1);
        assert!(univ.code_for("idontexist").is_none());

        assert!(univ.process_for_index(0, |n| {assert_eq!(n, "__name__"); true}).unwrap());
        assert!(univ.process_for_index(1, |n| {assert_eq!(n, "egg"); true}).unwrap());
        assert!(univ.process_for_index(2, |_| assert!(false)).is_none());
    }

    #[test]
    fn test_label_map() {
        let mut lvm = LabelValueMap::new();
        lvm.set_value("__name__", "my_duration_seconds");
        lvm.set_value("host", "troubles");

        let val = to_vec_pretty(&lvm).unwrap();

        println!("Result {}", String::from_utf8(val).unwrap());
    }

    #[test]
    fn test_make_legal() {
        let res = make_metric_name_legal("a_1");
        assert_eq!(res, "a_1");

        let res = make_metric_name_legal("surely_y0u_jest");
        assert_eq!(res, "surely_y0u_jest");

        let res = make_metric_name_legal("a*");
        assert_eq!(res, "a_");

        let res = make_metric_name_legal("42");
        assert_eq!(res, "_42");

        let res = make_metric_name_legal("s-s-s-[_]??/.");
        assert_eq!(res, "s_s_s________");
    }
}