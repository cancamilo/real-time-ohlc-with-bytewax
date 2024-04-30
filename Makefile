.PHONY: deploy delete list project-files run

# install Poetry and Python dependencies
init:
	curl -sSL https://install.python-poetry.org | python3 -
	poetry install

# run flow only
run-flow:
	poetry run python -m bytewax.run "src.dataflow:get_dataflow()"

# run the feature-pipeline locally
run:
	poetry run streamlit run src/frontend.py