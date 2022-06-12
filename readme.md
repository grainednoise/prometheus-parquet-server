# Prometheus Parquet Server

## What is it?

This application takes a zipped set of parquet files and tries to make them
queryable over a standard Prometheus compatible HTTP-API. It assumes the
data consists of time-series in a particular format.

## What is its intended use?

Its primary use case is in making it easy to explore saved metrics data
that were previously collected, for instance, during testing. It is 
developed with Grafana in mind as its exploration tool.

## Licence

MIT

## Building/installing the project

This is a standard Rust project, so all the standard Cargo commands should
just work, like `cargo build` and `cargo install`. It's developed using the
latest stable Rust version, which is `1.61.0` at the time of writing.

## Zipped `.parquet` files

### Creating them

- The `.parquet` files are in the format Pandas stores its dataframes in
  if `pyarrow` is available for it to use
- The `.parquet` extension is mandatory
- These files then need to be collected in a `.zip` file
- Giving that file the double extension `.parquet.zip` is not strictly
  required, but it is recommended

### Serving them

The executable accepts the path to your `.parquet.zip` as its argument,
and if it points to a directory instead, with just a single `.parquet.zip`
in it, that's also OK.

At the moment, it is mandatory to also have a re-tag yaml in the same
directory, that has the same name, but with an extra `.yaml` appended
to it. You might be able to get away with simply having a file with the
following content:

```Yaml
  ---
  config:
  skip-unmapped: false
```

This file is a way to handle name collisions, and re-tag certain
time-series so that they play well together. More useful documentation
will follow later.

A useful commandline option is `--shift_to_midnight` of `-s` for short.
What this does is let your time series be shifted so they start at
the most recent midnight. This makes it easy to start exploring your
data as the Grafana "today so far" or "today" time ranges will show
your data regardless of when it was actually recorded, or whatever its
time offset was. A lot of data from tests actually starts at time = 0.0.

## Rules for extracting time series from a parquet encoded data set

- The data **MUST** be in the form of a 2-dimensional array
- There **MUST** be exactly one column named:
  - `time`
  - `__time__`
  - `timestamp`
- That column **MUST** contain a floating point number denoting seconds-since-epoch
- The values **MUST** be monotonically non-decreasing, but duplicates are only allowed for different label-values
- There may be any number of columns containing string-values. These will become
  the label/value pairs
- There **MUST** be at least 1 column of floating point numbers
  - If there is indeed just one column, this will become this time-series values
  - If there are multiple columns, but not having names that would indicate it's
    a histogram, all columns will become part of a separate time series. If
    there is a column named `value`, the metric name will be the original, all
    other columns will have a metric column derived from the original metric name
    and the column name.
  - Having the following column names indicates we're dealing with a Histogram:
    - `Le<float>`, where float is a number. These all become separate time series
      with `_bucket` appended to the metric name
    - `sum` (optional) that metric will have `_sum` appended to the metric name
    - `max` (optional) that metric will have `_max` appended to the metric name
    - `count` (optional) that metric will have `_count` appended to the metric
      name. Note that if such a column is missing, *and* there is an `Le+Inf`
      column, the `Le+Inf` column is used to generate such a column anyway.

## Limitations

The currently supported set of features is really limited:

- The `/api/v1/query` is a dummy. It accepts only "1+1" as a query, but this is
  enough for Grafana to accept it a valid Prometheus datasource.
- The `/api/v1/query_range` accepts structurally sound queries, however:

  - `sum` is the only implemented aggregation
  - Only the functions `rate`, `irate` and `histogram_quantile` are implemented
  - `rate` and `irate` do not have any reset-logic yet
  - `rate` does not have the right logic in regard to the beginning and end
    of a range.
  - The `irate` logic needs work
  - Arithmetic is not implemented

- In addition to the endpoints discussed above, just the following API
  endpoints are implemented, and that is just because Grafana needs these:

  - `/api/v1/labels`
  - `/api/v1/label/{label_name}/values`

## Links/references

- [Prometheus API definition](https://prometheus.io/docs/prometheus/latest/querying/api/)
- [Prometheus querying basics](https://prometheus.io/docs/prometheus/latest/querying/basics/)
- [Prometheus query functions](https://prometheus.io/docs/prometheus/latest/querying/functions/)
- [Prometheus histograms & summaries](https://prometheus.io/docs/practices/histograms/)
