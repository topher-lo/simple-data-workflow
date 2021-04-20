# Simple Data Workflow
[![MIT License](https://img.shields.io/badge/license-MIT-informational.svg)](https://opensource.org/licenses/MIT)
[![Python 3.8](https://img.shields.io/badge/python-3.8-informational.svg)](https://www.python.org/downloads/)
[![Test](https://github.com/topher-lo/simple-data-workflow/workflows/Run%20Tests/badge.svg)](https://github.com/topher-lo/simple-data-workflow/actions)

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

#### <kbd>function</kbd> `retrieve_data`
Reads data (from url string) into a DataFrame.

#### <kbd>function</kbd> `_column_wrangler`
Transforms column names into a consistent format.

#### <kbd>function</kbd> `_obj_wrangler`
Converts columns with `object` dtype into `StringDtype`.

#### <kbd>function</kbd> `_factor_wrangler`
Converts columns in `is_cat` into `CategoricalDtype`.

#### <kbd>function</kbd> `_check_model_assumptions`
Empty function to be implemented.

#### <kbd>function</kbd> `clean_data`
A Pandas pipeline of data wranglers in order of:
1. `convert_dtypes`
2. `_replace_na`
3. `_column_wrangler`
4. `_obj_wrangler` 
5. `_factor_wrangler`
6. `_check_model_assumptions`.

#### <kbd>function</kbd> `encode_data`
Transforms columns with `category` dtype into columns of dummies.

#### <kbd>function</kbd> `wrangle_na`
Wrangles missing values. 5 available strategies: complete case, fill-in, fill-in with indicators,
grand model, and MICE.

#### <kbd>function</kbd> `transform_data`
Applies either log or arcsine transformations on data.

#### <kbd>function</kbd> `gelman_standardize_data`
Standardizes data by dividing by 2 standard deviations and mean-centering them.

### Modelling:

#### <kbd>function</kbd> `run_model`
`statsmodels` linear regression implementation.

### Post-processing:

#### <kbd>function</kbd> `plot_confidence_intervals`
Given a fitted OLS model in `statsmodels`, returns a box and whisker regression coefficient plot.
