install:
    poetry lock --no-update
    poetry install --sync
test: install
    poetry run pytest