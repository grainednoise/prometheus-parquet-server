use std::collections::HashMap;
use std::io::{Error, ErrorKind};
use std::path::PathBuf;

use serde::{Deserialize, Serialize};
use serde_yaml::{self};

use crate::LabelValueMap;

#[derive(Debug, Serialize, Deserialize)]
struct Config {
    #[serde(rename = "skip-unmapped")]
    skip_unmapped: bool
}

#[derive(Debug, Serialize, Deserialize)]
struct Mapping {
    tags: Option<HashMap<String, String>>
}

#[derive(Debug, Serialize, Deserialize)]
struct BaseConfig {
    config: Option<Config>,
    mapping: Option<HashMap<String, Mapping>>
}

pub fn read_mapping(file_name: &PathBuf) -> Result<HashMap<String, LabelValueMap>, Error> {
    let f = std::fs::File::open(file_name)?;
    let config: BaseConfig = match serde_yaml::from_reader(f) {
        Ok(conf) => conf,
        Err(err) => {
            return Err(Error::new(ErrorKind::Other, err))
        }
    };
    // println!("{:?}", config);

    let mut result: HashMap<String, LabelValueMap> = Default::default();

    if let Some(mapping) = config.mapping {
        for (name, tag_map) in mapping {
            let mut label_map = LabelValueMap::new();

            if let Some(tags) = tag_map.tags {
                for (label_name, label_value) in tags {
                    label_map.set_value(&label_name, &label_value);
                  }
            }
            result.insert(name, label_map);
        }
    }

    Ok(result)
}


#[cfg(test)]
mod test {
    use std::path::PathBuf;

    use crate::config::read_mapping;

    #[test]
    fn test_reader() {
        let name = "D:/RustDev/prometheus_parquet_server/testdata/performancedata.parquet.zip.yaml";
        let pb = PathBuf::from(name);
        match read_mapping(&pb) {
            Ok(map) => {println!("Map = {:?}", map)}
            Err(_) => {}
        }
    }
}


