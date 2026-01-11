.PHONY: init_db meta_db

init_db:
	uv run init_db/init_db.py

meta_db:
	uv run meta_db/main.py