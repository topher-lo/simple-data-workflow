from src.flow import e2e_pipeline
from prefect.executors import LocalExecutor

def test_e2e_pipeline():
    """Smoke test. Flow successfully executes using a local executor.
    """

    kwargs = {
        'url': 'https://vincentarelbundock.github.io/Rdatasets/csv/stevedata/fakeTSD.csv',
        'cat_cols': ['year'],
        'endog': 'y',
        'exog': ['x1', 'x2']
    }

    state = e2e_pipeline.run(**kwargs, executor=LocalExecutor())
    assert state.is_successful()
