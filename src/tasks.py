"""Individual data tasks (preprocessing, modelling, and postprocessing) are
encapsulated into the following functions:

--- Preprocessing ---

1. `retrieve_data`:
Retrieves data from a url, returns data as a DataFrame.

2. `_column_wrangler`:
Transforms column names into a consistent format.

3. `_obj_wrangler`:
Converts columns with `object` dtype into `StringDtype`.

4. `_factor_wrangler`:
Converts columns in `is_cat` into `CategoricalDtype`.

5. `_check_model_assumptions`:
Empty function

6. `clean_data`:
A pandas pipeline of data wranglers.

7. `encode_data`:
Transforms columns with `category` dtype into columns of dummies.

8. `wrangle_na`:
Wrangles missing values. 5 available strategies:
- Complete case
- Fill-in
- Fill-in with indicators
- Grand model
- MICE

9. `transform_data`:
Applies transformations on data.

10. `gelman_standardize_data`:
Standardizes data by dividing by 2 standard deviations and mean-centering them.

--- Modelling ---

11. `run_model`:
`statsmodels` linear regression implementation.

--- Post-processing ---

12. `plot_confidence_intervals`:
Given a fitted OLS model in `statsmodels`, returns a box and whisker regression coefficient plot.

Note 1. Public functions (i.e. functions without a leading underscore `_func`) are wrapped around Prefect's `@task` decorator

Note 2. Empty functions (e.g. `_check_model_assumptions`) are
suggested data tasks for the user to implement.
For instance, the model assumptions of multiple linear regression
(i.e. no multicollinearity) might not appAly for another model
(e.g. non-parametric models such as random forest).

Note 3. The implementations in functions 9. and 10. are simple examples only.
Replace the code within these functions to according to your data model.
"""

import altair as alt
import datetime as dt
import numpy as np
import pandas as pd
import itertools
import re

import statsmodels.api as sm

from patsy import dmatrix
from prefect import task
from typing import List
from typing import Union
from typing import Mapping

from src.styles import streamlit_theme

from statsmodels.regression.linear_model import RegressionResultsWrapper
from statsmodels.imputation.mice import MICEData
from pandas.api.types import is_categorical_dtype
from pandas.api.types import is_float_dtype


# Helper functions

def clean_text(text: str):
    """Returns string:
    1. Stripped of all whitespaces at start and end
    2. Any excess whitespace in between are replaced with an underscore "_"
    3. All characters are lowercased
    """
    clean_text = re.sub(' +', '_', text.strip()).lower()
    return clean_text


# Sanitize user inputted column names
@task
def sanitize_col_names(cols: List[str]) -> List[str]:
    if cols:
        return [clean_text(col) for col in cols]


# Pre-processing

@task(max_retries=3, retry_delay=dt.timedelta(seconds=10))
def retrieve_data(url: str,
                  sep: str = ',',
                  nrows: Union[None, int] = None) -> pd.DataFrame:
    """Reads data (from url string) into a DataFrame.

    Args:
        url (str): 
            URL to data. Data is a delimiter-separated text file.

        sep (str): 
            Delimiter to use.

        nrows (int): 
            Number of rows of the file to read. Useful
            for examining the header without downloading the entire file, or
            for reading pieces of large files.

    Returns:
        A delimiter-separated text file is returned as a Pandas DataFrame.

    Note 1. pandas uses its super fast C engine to read flat files
    ONLY IF `sep` is explicitly given. Otherwise, it uses
    Python's parsing engine to automically detect the seperator
    and read the file. The Python engine is considerably slower.

    Note 2. pandas's input/output API supports an extensive range of
    data formats. See https://pandas.pydata.org/pandas-docs/dev/user_guide/io.html
    for more information. Change the code within this function to retrieve data
    from sources other than CSV (e.g. data stored on a SQL database).
    """
    # If `sep` is specified as None, the separator is automatically
    # detected using Python's builtin sniffer tool `csv.sniffer`.
    data = pd.read_csv(url, sep=sep, nrows=nrows)
    # Remove unnamed index columns
    data = data.loc[:, ~data.columns.str.contains('Unnamed')]
    return data


def _replace_na(
    data: pd.DataFrame,
    na_values: Union[None, List[Union[str, int, float]]]
) -> pd.DataFrame:
    """Replaces values in `na_values` with `np.nan`.
    """
    if na_values:
        data = data.replace(na_values, np.nan)
    return data


def _column_wrangler(data: pd.DataFrame) -> pd.DataFrame:
    """Returns DataFrame with columns transformed into a consistent format:
    1. Stripped of all whitespaces at start and end
    2. Any excess whitespace in between are replaced with an underscore "_"
    3. All characters are lowercased
    """
    data.columns = (data.columns
                        .str.strip()
                        .str.replace(r' +', '_')
                        .str.lower())
    return data


def _obj_wrangler(data: pd.DataFrame) -> pd.DataFrame:
    """Converts columns with `object` dtype to `StringDtype`.
    """
    obj_cols = (data.select_dtypes(include=['object'])
                    .columns)
    data.loc[:, obj_cols] = (data.loc[:, obj_cols]
                                 .astype('string'))
    return data


def _factor_wrangler(
    data: pd.DataFrame,
    cat_cols: Union[None, List[str]] = None,
    ordered_cols: Union[None, List[str]] = None,
    categories: Union[None, Mapping[str, List[Union[str, int, float]]]] = None,
    str_to_cat: bool = True,
    dummy_to_bool: bool = True,
) -> pd.DataFrame:
    """Converts columns in `is_cat` to `CategoricalDtype`.

    Pre-conditions:
    1. Columns in `is_cat` must be convertible to `CategoricalDtype`.
    2. Columns in `is_ordered` must already be cast as a categorical column.
       Otherwise, the columns must either be specified in `is_cat`, is a string
       column and `str_to_cat` is True, or is a boolean column and
       `dummy_to_bool` is True.
    3. All column names are in a consistent format (see `_column_wrangler`)

    Args:
        data (pd.DataFrame): 
            The data.

        cat_cols (list of str): 
            List of columns to convert to `CategoricalDtype`.

        ordered_cols (list of str): 
            List of categorical columns to declare to have an ordered
            relationship between its categories. If column is not specified
            in `categories` argument, then the column's categories are set in
            alphanumeric order.

        categories (dict of [str, int, float]): 
            Dictionary with column names as keys and list of str, int, or
            float as values. Column names must refer to categorical columns.
            For each dictionary key in `categories`, `_factor_wrangler`
            sets the corresponding key's (i.e. column's) categories to the
            corresponding dictionary value (i.e. list of str, int, or float).

        str_to_cat (bool): 
            If True, converts all `StringDtype` columns
            to `CategoricalDtype`.

        dummy_to_bool (bool): 
            If True, converts all columns with integer
            [0, 1] values or float [0.0, 1.0] values into `BooleanDtype`.

    Returns:
        A copy of the inputted Pandas DataFrame. Converts specified columns to
        `CategoricalDtype`, both ordered and unordered, and sets specified
        categorical columns' categories. All other columns' dtypes
        are unchanged.
    """

    all_cat_cols = []
    if str_to_cat:
        str_cols = (data.select_dtypes(include=['string'])
                        .columns
                        .tolist())
        all_cat_cols += str_cols
    if dummy_to_bool:
        # Select columns with [0, 1] (integer or float) values only
        sum_cols = (data.select_dtypes(include=['integer', 'float'])
                        .apply(pd.Series.unique)
                        .apply(np.nansum) == 1)
        mask = sum_cols.loc[sum_cols].index
        # Convert floats in mask to integer
        for col_name in mask:
            col = data.loc[:, col_name]
            if is_float_dtype(col):
                data.loc[:, col_name] = (data.loc[:, col_name]
                                             .astype('Int8'))

        # Convert dummy_cols into BooleanDtype
        data.loc[:, mask] = (data.loc[:, mask]
                                 .astype('boolean'))

    if cat_cols:
        all_cat_cols += cat_cols
    if all_cat_cols:
        for col in all_cat_cols:
            data.loc[:, col] = (data.loc[:, col]
                                    .astype('category'))
    # Set categories
    if categories:
        # Clean col names
        categories = {k: v for k, v in categories.items()}
        for col, cats in categories.items():
            data.loc[:, col] = (data.loc[:, col]
                                    .cat
                                    .set_categories(cats))
    # Set is_ordered
    if ordered_cols:
        # Clean col names
        ordered_cols = [col for col in ordered_cols]
        for col in ordered_cols:
            data.loc[:, col] = (data.loc[:, col]
                                    .cat
                                    .as_ordered())
    return data


def _check_model_assumptions(data: pd.DataFrame) -> pd.DataFrame:
    """To be implemented. Write checks for your model's assumptions.
    Consider throwing a ValueError exception if critical assumptions
    are violated.
    """
    return data


@task
def clean_data(
    data: pd.DataFrame,
    na_values: Union[None, List[Union[str, int, float]]] = None,
    cat_cols: Union[None, List[str]] = None,
    ordered_cols: Union[None, List[str]] = None,
    categories: Union[None, Mapping[str, List[Union[str, int, float]]]] = None,
    str_to_cat: bool = True,
    dummy_to_bool: bool = True,
) -> pd.DataFrame:
    """Data preprocessing pipeline. Runs the following data wranglers on `data`:
    1. convert_dtypes
    2. _replace_na
    3. _column_wrangler
    4. _obj_wrangler
    5. _factor_wrangler
    6. _check_model_assumptions
    """
    data = (data.convert_dtypes()
                .pipe(_replace_na, na_values)
                .pipe(_column_wrangler)
                .pipe(_obj_wrangler)
                .pipe(_factor_wrangler,
                      cat_cols,
                      ordered_cols,
                      categories,
                      str_to_cat,
                      dummy_to_bool)
                .pipe(_check_model_assumptions))
    return data


@task
def encode_data(data: pd.DataFrame) -> pd.DataFrame:
    """Transforms columns with unordered `CategoricalDtype` into dummy
    columns. Dummy columns are cast as `BooleanDtype` columns. Transforms
    columns with ordered `CategoricalDtype`into their category integer codes.

    Pre-conditions:
    1. All columns are cast as a nullable dtype in Pandas.
    2. All columns contain at most 1 nullable dtype (this condition
       should follow if 1. holds).
    3. There are only float, integer, boolean, or category columns.

    Post-conditions:
    1. There are no unordered `CategoricalDtype` columns. They are all
       transformed into `BooleanDtype` dummy columns.
    2. There are no ordered `CategoricalDtype` columns. They are all
       transformed into Int64 dtype columns.

    Note: missing values are considered as its own individual category.
    """
    unordered_mask = data.apply(lambda col: is_categorical_dtype(col) and
                                not(col.cat.ordered))
    ordered_mask = data.apply(lambda col: is_categorical_dtype(col) and
                              col.cat.ordered)
    unordered = (data.loc[:, unordered_mask]
                     .columns)
    ordered = (data.loc[:, ordered_mask]
                   .columns)
    if unordered.any():
        dummies = pd.get_dummies(data.loc[:, unordered]).astype('boolean')
        data = (data.loc[:, ~data.columns.isin(unordered)]
                    .join(dummies))
    if ordered.any():
        for col in ordered:
            data.loc[:, col] = data.loc[:, col].cat.codes
    return data


@task
def wrangle_na(data: pd.DataFrame,
               strategy: str,
               cols: Union[None, List[str]] = None,
               **kwargs) -> pd.DataFrame:
    """Wrangles missing values in `data` according to the
    strategy specified in `strategy`.

    Pre-conditions:
    1. All columns are cast as a nullable dtype in Pandas.
    2. All columns contain at most 1 nullable dtype (this condition
       should follow if 1. holds).
    3. All column names are in a consistent format(see `_column_wrangler`).

    Available methods:

    1. "cc" -- Complete case: 
        Drops all missing values.

    2. "fi" -- Fill-in: 
        Imputes missing values.

    3. "fii" --Fill-in with indicators: 
        Imputes missing values; creates indicator columns for patterns of
        missing values across feature columns.

    4. "gm" -- Fill-in with indicators and interactions (AKA grand model): 
        Imputes missing values; creates indicator columns akin to strategy 3;
        creates additional missing value indictor columns for the complete set
        of interactions between features and the missing value indicators.

    5. "mice" -- Multiple imputation with chained equations: 
        Performs MICE procedure. Returns each imputed dataset from N draws of
        the original dataset. Optional arguments to specify in `kwargs`:

        - `n_burnin`: 
            First `n_burnin` MICE iterations to skip; defaults to 20.
        - `n_imputations`: 
            Number of MICE iterations to save after burn-in phase;
            defaults to 10.
        - `n_spread`: 
            Number of MICE iterations to skip between saved imputations;
            defaults to 20.

    Post-conditions:
    1. Transformed columns originally cast as a nullable integer dtype
       are coerced into Float64.
    2. Indicator columns are cast as `BooleanDtype`.

    Note 1. `**kwargs` contains required or optional keyword arguments for
    `statsmodels.imputation.mice.MICEData`.

    Note 2. For "fi", "fii", and "gm", missing values in
    float columns are replaced by the mean along the column. Missing values
    in integer columns are replaced by the median along the column.
    Missing values in categorical and boolean columns are replaced by the most
    frequent value along the column.
    """

    # If no missing values
    if pd.notna(data).all().all():
        # Return inputted dataframe
        return data

    # If no missing values
    if pd.notna(data).all().all():
        return data

    # If complete case
    if strategy == 'cc':
        data = data.dropna(**kwargs)

    # If fill-in with indicators or grand model
    if strategy in ['fii', 'gm']:
        # Create indicator columns for patterns of na
        na_indicators = (
            data.applymap(lambda x: '1' if pd.isna(x) else '0')
                .agg(''.join, axis=1)
                .pipe(pd.get_dummies)
                .add_prefix('na_')
                .drop('na_{}'.format('0'*len(data.columns)), axis=1)
                .astype('boolean')
        )
        data = data.join(na_indicators)

    # If fill-in (or related)
    if strategy in ['fi',  'fii', 'gm']:
        # Imputation (floats)
        float_cols = data.select_dtypes(include=['float']).columns
        if any(float_cols):
            data.loc[:, float_cols] = (
                data.loc[:, float_cols].fillna(data.loc[:, float_cols].mean())
            )
        # Imputation (integer)
        int_cols = data.select_dtypes(include=['integer']).columns
        if any(int_cols):
            data.loc[:, int_cols] = (
                data.loc[:, int_cols].fillna(data.loc[:, int_cols].median())
            )
        # Imputation (categorical and boolean columns)
        fact_cols = (data.select_dtypes(include=['category', 'boolean'])
                         .columns)
        if any(fact_cols):
            modes_df = data.loc[:, fact_cols].mode()
            col_mode_dict = {col: modes_df.loc[0, col] for col
                             in modes_df.columns}
            data.loc[:, fact_cols] = (
                data.loc[:, fact_cols].fillna(col_mode_dict)
            )

    # If grand model
    if strategy == 'gm':
        # Get interactions between features and na_indicators
        na_cols = [col for col in data.columns if col.startswith('na_')]
        feature_cols = [col for col in data.columns if col not in na_cols]
        # Convert to non-nullable dtypes
        temp_data = pd.DataFrame(data[feature_cols + na_cols]
                                 .to_numpy()).infer_objects()
        temp_data.columns = feature_cols + na_cols
        # Get interactions
        interaction_terms = list(itertools.product(feature_cols, na_cols))
        interaction_formula = ' + '.join(
            ['Q("{}"):Q("{}")'.format(*term) for term
             in interaction_terms]
        ) + '-1'
        interactions = dmatrix(interaction_formula,
                               temp_data,
                               return_type='dataframe')
        data = data.join(interactions)

    # If MICE
    if strategy == 'mice':
        # Label encode columns
        column_codes = pd.Categorical(data.columns)
        # Dictionary codes label
        col_code_map = dict(enumerate(column_codes.categories))
        # Rename columns to standardized terms for patsy
        data.columns = [f'col{c}' for c in column_codes.codes]
        imputer = MICEData(data, **kwargs)
        n_burnin = kwargs.get('n_burnin', 20)
        n_imputations = kwargs.get('n_imputations', 10)
        n_spread = kwargs.get('n_spread', 20)
        imputed_datasets = []
        # Draw n_burnin + n_imputations + n_imputations * n_spread
        # MICE iterations
        for i in range(n_imputations + 1):
            if i == 0:
                # Burn-in phase
                imputer.update_all(n_iter=n_burnin)
            else:
                # Imputation phase
                # Select final update after n_spread iterations
                imputer.update_all(n_iter=n_spread)
                imputed_datasets.append(imputer.data)
        data = pd.concat(imputed_datasets,
                         keys=list(range(n_imputations)))
        data.index = data.index.set_names(['iter', 'index'])
        # Inverse label encode columns
        data.columns = [col_code_map[int(c[3:])] for c
                        in data.columns]
    return data


@task
def transform_data(
    data: pd.DataFrame,
    cols: Union[None, List[str]] = None,
    func: str = 'arcsinh',
) -> pd.DataFrame:
    """Transforms columns in `cols` according to
    specified transformation in `func`.

    Pre-conditions:
    1. All columns are cast as a nullable dtype in Pandas.
    2. All columns contain at most 1 nullable dtype (this condition
       should follow if 1. holds).
    3. All column names are in a consistent format (see `_column_wrangler`).
    4. All columns in `cols` are cast as a numeric dtype.

    Transformations available:
    - "log" -- Log transform
    - "arcsinh" -- Inverse hyperbolic sine transform

    Post-conditions:
    1. Transformed columns originally cast as a nullable integer dtype
       are coerced into Float64.

    Raises:
        ValueError: if `cols` in `data` contain zero values and
        `func` is specified as "log".
    """

    functions = {
        'log': np.log,
        'arcsinh': np.arcsinh,
    }
    if cols:
        if func == 'log' and (data.loc[:, cols] == 0).any().any():
            raise ValueError('Dataset contains zero values. Cannot take logs.')
        # Get dict of dtypes in original cols
        dtypes = (data.loc[:, cols]
                      .dtypes.apply(lambda x: x.name).to_dict())
        # Replace Int64 with Float64 in dtypes dict
        coerced_dtypes = {k: (lambda v: 'Float64' if 'Int' in v else v)(v)
                          for k, v in dtypes.items()}
        # Apply transformation and coerce previously Int64 cols to Float64
        data.loc[:, cols] = (functions[func](data.loc[:, cols])
                             .astype(coerced_dtypes))
    return data


@task
def gelman_standardize_data(data: pd.DataFrame):
    """Standardizes data by dividing by 2 standard deviations and
    mean-centering them. Non-numeric columns (except boolean columns)
    are ignored. Boolean columns are shifted to have mean zero (with True
    represented by 1 and False by 0) but are not rescaled.

    Pre-conditions:
    1. All columns are cast as a nullable dtype in Pandas.
    2. All columns contain at most 1 nullable dtype (this condition
       should follow if 1. holds).
    3. All column names are in a consistent format(see `_column_wrangler`).

    Post-conditions:
    1. Transformed columns originally cast as a nullable integer or nullable
       boolean dtypes are coerced into Float64.
    """

    # Rescale numeric data
    num_cols = (data.select_dtypes(include=['number'])
                    .columns)
    # Subtract mean, then divide by 2 std
    num_cols_df = data.loc[:, num_cols].copy()  # Defensive copying
    num_mean = num_cols_df.mean()
    num_std = num_cols_df.std()
    data.loc[:, num_cols] = (num_cols_df - num_mean) / (2*num_std)
    # Rescale boolean data
    bool_cols = (data.select_dtypes(include=['boolean'])
                     .columns)
    bool_cols_df = data.loc[:, bool_cols].copy()  # Defensive copying
    # Only subtract mean
    data.loc[:, bool_cols] = bool_cols_df - bool_cols_df.mean()
    return data


# Modelling

@task
def run_model(data: pd.DataFrame,
              y: str,
              X: Union[str, List[str]]) -> RegressionResultsWrapper:
    """Runs a linear regression of y on X and
    returns a fitted OLS model in `statsmodels`.

    Pre-conditions:
    1. All columns are cast as a nullable numeric dtype in Pandas.
    2. All columns contain at most 1 nullable dtype (this condition
       should follow if 1. holds).
    """

    # Downcast nullable numeric types to numpy float64
    data = data.astype(np.float64)
    X_with_dummies = [col for col in data.columns if col != y and
                      any(x in col for x in X)]
    mod = sm.OLS(data[y], data[X_with_dummies])
    res = mod.fit()
    return res


# Post-processing

@task
def plot_confidence_intervals(res: RegressionResultsWrapper) -> alt.Chart:
    """Returns a matplotlib axes containing a box and whisker
    Seaborn plot of regression coefficients' point estimates and
    confidence intervals.
    """
    alt.themes.register("streamlit", streamlit_theme)  # Enable custom theme
    alt.themes.enable("streamlit")
    conf_int = res.conf_int()  # 95% C.I.
    # Stack lower and upper columns
    conf_int = conf_int.stack()
    conf_int.name = "estimate"
    conf_int = pd.DataFrame(conf_int)
    conf_int = (conf_int.reset_index()
                        .rename(columns={'level_0': 'regressor',
                                         'level_1': 'interval'}))
    chart = alt.Chart(conf_int).mark_boxplot().encode(
        x='regressor:O',
        y='estimate:Q'
    ).properties(
        width=200,
        height=500
    )
    return chart


if __name__ == "__main__":
    pass
