# Simple Data Workflow
[![MIT License](https://img.shields.io/badge/license-MIT-informational.svg)](https://opensource.org/licenses/MIT)
[![Python 3.8](https://img.shields.io/badge/python-3.8-informational.svg)](https://www.python.org/downloads/)
[![Test](https://github.com/topher-lo/simple-data-workflow/workflows/Run%20Tests/badge.svg)](https://github.com/topher-lo/simple-data-workflow/actions)
[![codecov](https://codecov.io/gh/topher-lo/simple-data-workflow/branch/main/graph/badge.svg?token=2E5DZX9VOG)](https://codecov.io/gh/topher-lo/simple-data-workflow)

A simple end-to-end data workflow (preprocessing, modelling, visualization) orchestrated using Prefect tasks and flows.

## 🚀 Quickstart
The easiest way to get started is to clone the repo:
```bash
git clone git@github.com:topher-lo/simple-data-workflow.git
```
Then install its dependencies using pip:
```bash
pip install -r requirements.txt
```

## ✨ Quick Example
#### <kbd>instance</kbd> `e2e_pipeline`
An end-to-end data data workflow that:
1. Downloads data from an URL;
2. Cleans data;
3. Runs linear regression;
4. Plots regression results as a box-and-whisker chart.

```python
from src.flow import e2e_pipeline

# Flow parameters
kwargs = {
    'url': 'https://vincentarelbundock.github.io/Rdatasets/csv/stevedata/fakeTSD.csv',
    'cat_cols': ['year'],  # List of categorical variables in dataset
    'na_strategy': 'mice',  # Method to deal with missing values
    'transf_cols': ['x1', 'x2'],  # Variables to apply transformation on
    'transf_func': 'arcsinh',  # Transformation function
    'endog': 'y',  # Endogenous (outcome) variable
    'exog': ['x1', 'x2']  # Exogenous (feature) variables
}

# Execute flow
state = e2e_pipeline.run(**kwargs)

# Check if flow run was successful
if state.is_successful():
    
    # Get task's reference ID from its name
    task_name = 'plot_confidence_intervals'
    task_ref = e2e_pipeline.get_tasks(name=task_name)[0]
    
    # Get altair chart
    conf_int_chart = state.result[task_ref].result
```

## 🎛 Tasks API
These are individual data tasks that make up each part (i.e. preprocessing, modelling, post-processing) of the end-to-end data flow.

### Preprocessing:

#### <kbd>function</kbd> `sanitize_col_names`
Sanitizes strings in list by: 1. stripping all white-spaces at start and end; 2. replaces any excess whitespace with an underscore; and 3. lower-cases all characters.

**Parameters:**
- `cols (List[str])`: List of string (e.g. column names) to sanitize.

**Returns:**
Sanitized list of strings (e.g. column names).

#### <kbd>function</kbd> `retrieve_data`
Reads data (from url string) into a DataFrame.

**Parameters:**
- `url (str)`: URL to data. Data is a delimiter-separated text file.
- `sep (str)`: Delimiter to use.
- `nrows (int)`: Number of rows of the file to read.

**Returns:**
The delimiter-separated text file as a Pandas DataFrame.

#### <kbd>function</kbd> `_column_wrangler`
Returns DataFrame with columns transformed into a consistent format (see `sanitize_col_names`).

**Parameters:**
- `data (pd.DataFrame)`: The data.

**Returns:**
DataFrame with sanitized column names.

#### <kbd>function</kbd> `_obj_wrangler`
Converts columns with `object` dtype into `StringDtype`.

**Parameters**
- `data (pd.DataFrame)`: The data.

**Returns:**
A copy of the inputted Pandas DataFrame with any `object` dtype columns cast as `StringDtype`.

#### <kbd>function</kbd> `_factor_wrangler`
Converts columns in `is_cat` into `CategoricalDtype`.

**Parameters**
- `data (pd.DataFrame)`: The data.
- `cat_cols (list of str)`: List of columns to convert to `CategoricalDtype`.
- `ordered_cols (list of str)`: List of categorical columns to declare to have an ordered relationship between its categories.
- `categories (dict of [str, int, float])`: Dictionary with column names as keys and list of str, int, or float as values.
- `str_to_cat (bool)`: If True, converts all `StringDtype` columns to `CategoricalDtype`.
- `dummy_to_bool (bool):` If True, converts all columns with integer [0, 1] values or float [0.0, 1.0] values into `BooleanDtype`.

**Returns:**
A copy of the inputted Pandas DataFrame. Converts specified columns to `CategoricalDtype`, both ordered and unordered, and sets specified categorical columns' categories. All other columns' dtypes are unchanged.

#### <kbd>function</kbd> `_check_model_assumptions`
Empty function to be implemented.

#### <kbd>function</kbd> `clean_data`
Data preprocessing pipeline. Runs the following data wranglers on `data`:
1. `convert_dtypes`
2. `_replace_na`
3. `_column_wrangler`
4. `_obj_wrangler` 
5. `_factor_wrangler`
6. `_check_model_assumptions`.

**Parameters:**
- `data (pd.dataFrame)`: The data.
- `na_values (list of str, int, or float)`: List of values to replace with NA.
- `kwargs`: keyword arguments in `_factor_wrangler`.

**Returns:**
The preprocessed data.


#### <kbd>function</kbd> `encode_data`
Transforms columns with unordered `CategoricalDtype` into dummy columns. Dummy columns are cast as `BooleanDtype` columns. Transforms columns with ordered `CategoricalDtype`into their category integer codes.

**Parameters:**
- `data (pd.dataFrame)`: The data.

**Returns:**
The encoded data.

#### <kbd>function</kbd> `wrangle_na`
Wrangles missing values. 5 available strategies: complete case ("cc"), fill-in ("fi"), fill-in with indicators ("fii"), grand model ("gm"), and MICE ("mice").

**Parameters:**
- `data (pd.dataFrame)`: The data.
- `strategy (str)`: Strategy to deal with missing values. 
- `cols (list of str)`: columns to wrangle.

**Returns:**
The data with missing data wrangled according to the specified strategy.

#### <kbd>function</kbd> `transform_data`
Applies either log or arcsine transformations on data.

**Parameters:**
- `data (pd.dataFrame)`: The data.
- `cols (list of str)`: Columns to transform. 
- `func (str)`: log transform ("log") or inverse hyperbolic sine transform ("arcsinh").

**Returns:**
The data with transformation applied to specified columns.

#### <kbd>function</kbd> `gelman_standardize_data`
Standardizes data by dividing by 2 standard deviations and mean-centering them.

**Parameters:**
- `data (pd.dataFrame)`: The data.

**Returns:**
The standardized data.

### Modelling:

#### <kbd>function</kbd> `run_model`
`statsmodels` linear regression implementation.

**Parameters:**
- `data (pd.dataFrame)`: The data.
- `strategy (str)`: Strategy to deal with missing values. 
- `cols (list of str)`: columns to wrangle.

**Returns:**
The data with missing data wrangled according to the specified strategy.

### Post-processing:

#### <kbd>function</kbd> `plot_confidence_intervals`
Given a fitted OLS model in `statsmodels`, returns a box and whisker regression coefficient plot.

**Parameters:**
- `res (RegressionResultsWrapper)`: regression results from statsmodels OLS.

**Returns:**
A matplotlib axes containing a box and whisker Altair plot of regression coefficients' point estimates and confidence intervals.