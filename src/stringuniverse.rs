use bimap::BiMap;

pub type StringCode = usize;

#[derive(Clone, Debug)]
pub struct SimpleStringUniverse {
    values: BiMap<String, StringCode>,
    next_code: usize,
}

impl SimpleStringUniverse {
    pub fn new() -> Self {
        SimpleStringUniverse {
            values: BiMap::new(),
            next_code: 0,
        }
    }

    pub fn len(&self) -> usize {
        self.next_code
    }

    pub fn code_for(&self, label: &str) -> Option<StringCode> {
        self.values.get_by_left(label).copied()
    }

    pub fn get_or_insert(&mut self, label: &str) -> StringCode {
        match self.code_for(label) {
            Some(t) => t,
            None => {
                let new_code = self.next_code;
                self.next_code += 1;
                self.values.insert(label.to_string(), new_code);
                new_code
            }
        }
    }

    pub fn get_for_index(&self, code: StringCode) -> Option<&str> {
        self.values.get_by_right(&code).map(|x| x.as_str())
    }
}