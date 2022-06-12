use std::ops::{DerefMut, Range};
use std::sync::{Arc, Mutex};

use color_eyre::eyre::eyre;

use crate::EyreResult;

pub type TimeStamp = f64;
pub type Duration = f64;

#[derive(Clone, PartialEq, Debug)]
pub enum IndexLocated {
    NotFound,
    Between(usize, usize),
    Above(usize),
    Below(usize)
}


pub trait TimeRange {
    fn len(&self) -> usize;
    fn try_get(&self, index: usize) -> Option<TimeStamp>;
    fn locate(&self, time_stamp: TimeStamp) -> IndexLocated;
    fn slice(&self, range: Range<usize>) -> &[TimeStamp];

    #[inline]
    fn first(&self) -> Option<TimeStamp> {
        self.try_get(0)
    }

    #[inline]
    fn last(&self) -> Option<TimeStamp> {
        self.try_get(self.len() - 1)
    }
}

pub struct TimeRangeIter<'a, T: TimeRange> {
    time_range: &'a T,
    next_index: usize
}

impl <'a, T: TimeRange> TimeRangeIter<'a, T> {
    pub fn new(time_range: &'a T) -> Self {
        TimeRangeIter {
            time_range,
            next_index: 0
        }
    }
}

impl <'a, T: TimeRange> Iterator for TimeRangeIter<'a, T> {
    type Item = TimeStamp;

    fn next(&mut self) -> Option<Self::Item> {
        match self.time_range.try_get(self.next_index) {
            None => None,
            Some(value) => {
                self.next_index += 1;
                Some(value)
            }
        }
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        let remaining = self.time_range.len() - self.next_index;
        (remaining, Some(remaining))
    }
}

#[derive(Debug, Clone)]
pub struct VecTimeRange {
    time_stamps: Vec<TimeStamp>
}

impl VecTimeRange {
    /// There is one additional constraint to qualify as a proper TimeRange, and that is
    /// that it is a monotonically increasing series. Rust's type system isn't so sophisticated
    /// as to allow us to express this constraint, so let's just test it
    pub fn from_iter_ref<'a, I:Iterator<Item=&'a TimeStamp>>(iter: I) -> EyreResult<Self> {
        VecTimeRange:: from_values(iter.cloned().collect())
    }

    pub fn from_values(time_stamps: Vec<TimeStamp>)  -> EyreResult<Self> {
        let mut last_timestamp = f64::NEG_INFINITY;

        for new_timestamp in time_stamps.iter().cloned() {
            if new_timestamp <= last_timestamp {
                return Err(eyre!("Not a monotonic sequence: previous={last_timestamp}, this={new_timestamp}"))
            }

            last_timestamp = new_timestamp;
        }

        Ok(VecTimeRange {
            time_stamps
        })
    }
}

const EMPTY_SLICE: [TimeStamp;0] = [];

impl TimeRange for VecTimeRange {
    fn len(&self) -> usize {
        self.time_stamps.len()
    }

    fn try_get(&self, index: usize) -> Option<TimeStamp> {
        self.time_stamps.get(index).copied()
    }

    fn locate(&self, time_stamp: TimeStamp) -> IndexLocated {
        let mut low_index = 0_usize;
        let mut high_index = match self.time_stamps.len() {
            0 => return IndexLocated::NotFound,
            1 => return if self.time_stamps[0] > time_stamp {
                     IndexLocated::Below(0)
                 }
                 else {
                     IndexLocated::Above(0)
                 },

            len => len - 1

        };

        if self.time_stamps[low_index] > time_stamp {
            return IndexLocated::Below(low_index)
        }

        if self.time_stamps[high_index] <= time_stamp {
            return IndexLocated::Above(high_index)
        }

        loop {
            if (high_index - low_index) == 1 {
                return IndexLocated::Between(low_index, high_index)
            }

            let test_index =  (high_index + low_index) / 2;

            if self.time_stamps[test_index] <= time_stamp {
                low_index = test_index;
            }
            else {
                high_index = test_index;
            }
        }
    }

    fn slice(&self, range: Range<usize>) -> &[TimeStamp] {
        let slice = self.time_stamps.as_slice();

        if range.end < slice.len() {
            return &slice[range]
        }

        if range.start >= slice.len() {
            return &EMPTY_SLICE
        }

        &slice[range.start..]
    }
}


#[derive(Debug)]
pub struct RegularTimeRange {
    start: TimeStamp,
    end: TimeStamp,
    increment: Duration,
    len: usize,
    realised_timerange: Mutex<Option<Arc<VecTimeRange>>>
}


impl RegularTimeRange {
    pub fn new(start: TimeStamp , end: TimeStamp, increment: Duration, offset: Option<Duration>) -> Self {
        assert!(end > start, "end <= start");
        assert!(increment > 0.0, "increment <= 0");

        let len_calc = ((end - start + increment) / increment).floor() as usize;
        let len = 1.max(len_calc);

        let offset = offset.unwrap_or(0.0);

        RegularTimeRange {
            start: start - offset,
            end: end - offset,
            increment,
            len,
            realised_timerange: Mutex::new(None)
        }
    }

    #[inline]
    pub fn start(&self) -> TimeStamp {
        self.start
    }

    #[inline]
    pub fn end(&self) -> TimeStamp {
        self.end
    }

    #[inline]
    pub fn increment(&self) -> Duration {
        self.increment
    }

    #[inline]
    pub fn len(&self) -> usize {
        self.len
    }

    pub fn realised_timerange(&self) -> Arc<VecTimeRange> {
        let mut mg = match self.realised_timerange.lock() {
            Ok(mg) => mg,
            Err(_) => panic!("poisoned lock")
        };

        let dr = mg.deref_mut();

        match dr {
            None => {
                let res = Arc::new(self.calculate_timerange());
                dr.replace(res.clone());
                res
            }
            Some(res) => res.clone()
        }
    }

    fn calculate_timerange(&self) -> VecTimeRange {
        let mut res = Vec::with_capacity(self.len);

        for n in 0..self.len {
            let t = self.start + self.increment * (n as Duration);
            res.push(t);
        }

        VecTimeRange::from_values(res).unwrap()
    }
}

impl Clone for RegularTimeRange {
    fn clone(&self) -> Self {
        let mg = match self.realised_timerange.lock() {
            Ok(mg) => mg,
            Err(_) => panic!("poisoned lock")
        };

        RegularTimeRange {
            start: self.start,
            end: self.end,
            increment: self.increment,
            len: self.len,
            realised_timerange: Mutex::new(mg.as_ref().map(|tr| tr.clone()))
        }
    }
}



#[cfg(test)]
mod test {
    use crate::timevectors::{IndexLocated, RegularTimeRange, TimeRange, VecTimeRange};

    #[test]
    fn test_find_empty() {
        let empty = VecTimeRange::from_values(vec![]).unwrap();
        let res = empty.locate(1.0);
        assert_eq!(res, IndexLocated::NotFound);
    }

    #[test]
    fn test_find_single() {
        let single = VecTimeRange::from_values(vec![0.5]).unwrap();
        let res = single.locate(0.0);
        assert_eq!(res, IndexLocated::Below(0));

        let res = single.locate(1.0);
        assert_eq!(res, IndexLocated::Above(0));

        let res = single.locate(0.5);
        assert_eq!(res, IndexLocated::Above(0));
    }

    #[test]
    fn test_find_pair() {
        let single = VecTimeRange::from_values(vec![1.0, 2.0]).unwrap();
        let res = single.locate(0.0);
        assert_eq!(res, IndexLocated::Below(0));

        let res = single.locate(2.0);
        assert_eq!(res, IndexLocated::Above(1));

        let res = single.locate(3.0);
        assert_eq!(res, IndexLocated::Above(1));

        let res = single.locate(1.0);
        assert_eq!(res, IndexLocated::Between(0, 1));

        let res = single.locate(1.5);
        assert_eq!(res, IndexLocated::Between(0, 1));
    }

    #[test]
    fn test_find_triplet() {
        let single = VecTimeRange::from_values(vec![1.0, 2.0, 3.0]).unwrap();
        let res = single.locate(0.0);
        assert_eq!(res, IndexLocated::Below(0));

        let res = single.locate(1.0);
        assert_eq!(res, IndexLocated::Between(0, 1));

        let res = single.locate(1.5);
        assert_eq!(res, IndexLocated::Between(0, 1));

        let res = single.locate(2.0);
        assert_eq!(res, IndexLocated::Between(1, 2));

        let res = single.locate(2.5);
        assert_eq!(res, IndexLocated::Between(1, 2));

        let res = single.locate(3.0);
        assert_eq!(res, IndexLocated::Above(2));

        let res = single.locate(3.5);
        assert_eq!(res, IndexLocated::Above(2));
    }

    #[test]
    fn test_find_large() {
        let offset = 3.14_f64;

        let series = (0..100).map(|n| n as f64 + offset).collect::<Vec<_>>();
        let tr = VecTimeRange::from_values(series).unwrap();

        for n in 0..99_usize {
            let res = tr.locate(n as f64 + offset);
            assert_eq!(res, IndexLocated::Between(n, n + 1));

            let res = tr.locate(n as f64 + 0.5 + offset);
            assert_eq!(res, IndexLocated::Between(n, n + 1));
        }

        let res = tr.locate(99.0 + offset);
        assert_eq!(res, IndexLocated::Above(99));

        let res = tr.locate(99.5 + offset);
        assert_eq!(res, IndexLocated::Above(99));
    }

    #[test]
    fn test_slice() {
        let empty = VecTimeRange::from_values(vec![]).unwrap();
        let slc = empty.slice(0..10);
        assert_eq!(slc.len(), 0);

        let small = VecTimeRange::from_values(vec![1.0, 2.0, 3.0]).unwrap();
        let slc = small.slice(0..10);
        assert_eq!(slc, &[1.0, 2.0, 3.0]);

        let slc = small.slice(1..10);
        assert_eq!(slc, &[2.0, 3.0]);

        let slc = small.slice(2..10);
        assert_eq!(slc, &[3.0]);

        let slc = small.slice(3..10);
        assert_eq!(slc.len(), 0);

        let slc = small.slice(4..10);
        assert_eq!(slc.len(), 0);
    }

    #[test]
    #[should_panic(expected = "end <= start")]
    fn test_range_start_after_end() {
        RegularTimeRange::new(2.0, 1.0, 1.0, None);
    }

    #[test]
    #[should_panic(expected = "end <= start")]
    fn test_range_start_is_end() {
        RegularTimeRange::new(1.0, 1.0, 1.0, None);
    }

    #[test]
    #[should_panic(expected = "increment <= 0")]
    fn test_range_zero_increment() {
        RegularTimeRange::new(1.0, 2.0, 0.0, None);
    }

    #[test]
    #[should_panic(expected = "increment <= 0")]
    fn test_range_negative_increment() {
        RegularTimeRange::new(1.0, 2.0, -1.0, None);
    }

    #[test]
    fn test_range_minimal() {
        let rtr = RegularTimeRange::new(1.0, 1.9, 1.0, None);
        let tr = rtr.calculate_timerange();
        println!("tr = {tr:?}");
        assert_eq!(tr.len(), 1);
        assert_eq!(tr.time_stamps[0], 1.0);
    }

    #[test]
    fn test_range_almost_minimal() {
        let rtr = RegularTimeRange::new(1.0, 2.0, 1.0, None);
        let tr = rtr.calculate_timerange();
        println!("tr = {tr:?}");
        assert_eq!(tr.len(), 2);
        assert_eq!(tr.time_stamps[0], 1.0);
        assert_eq!(tr.time_stamps[1], 2.0);
    }
}
