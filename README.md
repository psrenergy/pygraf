PyGraf
======


Utility module to read Sddp hdr/bin result file pairs. Some [examples](#usage-samples) to convert it to 
popular formats are available.


Installing
----------

Install the latest version from [PyPI](https://pypi.org/)

```
pip install psr-graf
```

Or download this repository contents.


Usage
-----

Start by importing `psr.graf` module. It's possible to read data directly using `open_bin` and `open_csv` functions 
or `load_as_dataframe` function if `pandas` package is available.


The example below shows how to load data directly into a `pandas.DataFrame` and prints the first 5 lines of data.

```python
import psr.graf

df = psr.graf.load_as_dataframe("sample_data/gerter.hdr")
print(df.head())

```

The output is:
```
                      Thermal 1  Thermal 2  Thermal 3
stage scenario block                                 
1     1        1       7.440000      0.744   0.368069
               2       6.437624      0.000   0.000000
               3       7.440000      0.744   0.576140
               4       7.440000      0.744   2.994997
               5       7.440000      0.744   0.916644
```

Alternatively, `open_bin` and `open_csv` functions can be used for direct data access as shown in the example next.

```python
import psr.graf

with psr.graf.open_bin("sample_data/gerter.hdr") as graf_file:
    print("Stages:", graf_file.stages)
    print("Scenarios:", graf_file.scenarios)
    print("Agents:", graf_file.agents)
    print(f"Initial date: {graf_file.initial_year:04d}/{graf_file.initial_stage:02d}")
    print("Units:", graf_file.units)
    stage = 2
    print(f"Number of blocks at stage {stage}: {graf_file.blocks(stage)}")
    scenario = 10
    block = 1
    print(f"Data at stage {stage}, scenario {scenario}, block {block}:",
          graf_file.read(stage, scenario, block))
```

The output is:
```
Stages: 12
Scenarios: 50
Agents: ('Thermal 1', 'Thermal 2', 'Thermal 3')
Initial date: 2013/01
Units: GWh
Number of blocks at stage 2: 1
Data at stage 2, scenario 10, block 1: (7.440000057220459, 0.7440000176429749, 0.3680693209171295)
```


File Formats
------------

| File Extension | Description                      |
|:--------------:|:---------------------------------|
| .hdr or .bin   | Binary .hdr and .bin pair        |
| .dat           | Single-binary file               |
| .csv           | CSV file with specific structure |

* `load_as_dataframe` supports all of them and will determine which reader will be used based on the file extension.
* `open_bin` supports only .hdr/bin pairs or single-binary files.
* `open_csv` supports only CSV.


Both `open_bin`, `open_csv`, and `load_as_dataframe` functions accept `encoding` parameter to specify the encoding of the strings in file. The default is `utf-8`.


DataFrame options
-----------------


### MultiIndex or single index

`load_as_dataframe` accepts an optional keyword argument `multi_index` (default `True`) to specify if the 
returned `pandas.DataFrame` should use `pandas.MultiIndex` or not. If `False`, the returned `pandas.DataFrame` will have a
single automatic index and the columns 'stage', 'scenario', 'block' will appear before the agents' data.

Example:
```python
import psr.graf
df = psr.graf.load_as_dataframe("sample_data/gerter.hdr", multi_index=False)
print(df.head())
print("Column names:", df.columns.values)
```

The output is:
```
   stage  scenario  block  Thermal 1  Thermal 2  Thermal 3
0      1         1      1   7.440000      0.744   0.368069
1      1         2      1   6.437624      0.000   0.000000
2      1         3      1   7.440000      0.744   0.576140
3      1         4      1   7.440000      0.744   2.994997
4      1         5      1   7.440000      0.744   0.916644
Column names: ['stage' 'scenario' 'block' 'Thermal 1' 'Thermal 2' 'Thermal 3']
```

On the other hand, 

```python
import psr.graf
df = psr.graf.load_as_dataframe("sample_data/gerter.hdr", multi_index=True)
print(df.head())
print("Column names:", df.columns.values)
```

Will produce the following output:
```
                      Thermal 1  Thermal 2  Thermal 3
stage scenario block                                 
1     1        1       7.440000      0.744   0.368069
      2        1       6.437624      0.000   0.000000
      3        1       7.440000      0.744   0.576140
      4        1       7.440000      0.744   2.994997
      5        1       7.440000      0.744   0.916644
Column names: ['Thermal 1' 'Thermal 2' 'Thermal 3']
```

### Index formats

The `index_format` specifies the format of index columns of the returned `pandas.Dataframe`. It accepts the following
values:

| Index Format | Columns                                      |
|:------------:|:---------------------------------------------|
|  `default`   | stage, scenario, block or hour               |
|   `period`   | year, month or week, scenario, block or hour |

* `default` creates a `pandas.DataFrame` with the columns as they are stored in the original file.
* `period` converts `stage` into `year`, `month` or `week` depending on the stage type of the file and the 
   initial year and stage.


Usage Samples
-------------

### [`dataframes_sample.py`](https://github.com/psrenergy/pygraf/blob/main/dataframes_sample.py)

Shows how to read data into `pandas.DataFrame`s.

Requires `pandas` package installed.


### [`matplotlib_sample.py`](https://github.com/psrenergy/pygraf/blob/main/matplotlib_sample.py)

Shows how to read data and plot data from hdr/bin file pairs.

![](https://github.com/psrenergy/pygraf/blob/main/docs/matplotlib_sample_plot.png)

Requires `matplotlib` package installed.



### [`csv_sample.py`](https://github.com/psrenergy/pygraf/blob/main/csv_sample.py)

Shows how to convert from hdr/bin file pairs to csv using `psr.graf` module.

This script can also be called from command line:

```bat
python csv_sample.py input_file.hdr output_file.csv
```

Where `output_file.csv` is optional.


### [`parquet_sample.py`](https://github.com/psrenergy/pygraf/blob/main/parquet_sample.py)

Shows how to convert from hdr/bin file pairs to Apache Parquet format.

Requires `pyarrow` package installed.

This script can also be called from command line:

```bat
python parquet_sample.py input_file.hdr output_file.parquet
```

Where `output_file.parquet` is optional.



Issues and Support
------------------

Check PyGraf's [GitHub repository](https://github.com/psrenergy/pygraf/) and [issues page](https://github.com/psrenergy/pygraf/issues) for support.

