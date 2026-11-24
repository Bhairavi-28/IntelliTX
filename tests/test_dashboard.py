"""Dashboard smoke tests using Streamlit's headless AppTest: every page must render without exception."""
import pytest

pytestmark = [pytest.mark.db, pytest.mark.engine, pytest.mark.usefixtures("db", "engine_cli")]
from pathlib import Path
APP = str(Path(__file__).resolve().parent.parent / "frontend" / "dashboard.py")
PAGES = ["Integrated Pipeline", "Transaction Lab", "Concurrency & Deadlock Lab", "Isolation & Serializability Lab", "CPU Scheduling Lab", "SQL & Indexing Lab",
         "Recovery Lab", "File Allocation & Disk Lab", "Performance Analysis", "Experiment History"]


@pytest.fixture(scope="module")
def seeded(db, engine_cli):
    from backend.services import experiments as ex
    ex.run_transaction_experiment(workers=2, txns=1000, records=200)
    ex.run_scheduling_experiment(txn_jobs=6)


@pytest.mark.parametrize("page", PAGES)
def test_every_page_renders(page, seeded):
    from streamlit.testing.v1 import AppTest
    at = AppTest.from_file(APP, default_timeout=60).run()
    assert not at.exception
    at.sidebar.radio[0].set_value(page).run()
    assert not at.exception, [e.value for e in at.exception]
    assert at.header[0].value.startswith(page.split(" &")[0].split(" ")[0])


def test_transaction_form_runs_and_saves(seeded):
    from streamlit.testing.v1 import AppTest
    at = AppTest.from_file(APP, default_timeout=120).run()
    at.sidebar.radio[0].set_value("Transaction Lab").run()
    at.button[0].click().run()  # the form submit button of the Transaction Lab
    assert not at.exception
    assert any("saved" in s.value for s in at.success), [s.value for s in at.success] + [e.value for e in at.error]


def test_pipeline_form_runs_all_checks(seeded):
    from streamlit.testing.v1 import AppTest
    at = AppTest.from_file(APP, default_timeout=180).run()   # landing page = Integrated Pipeline
    at.button[0].click().run()
    assert not at.exception, [e.value for e in at.exception]
    assert any("correctness checks passed" in s.value for s in at.success), [s.value for s in at.success] + [e.value for e in at.error]
