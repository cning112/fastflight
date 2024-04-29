## Convert pydantic classes to Zod schemas

First, make sure a python venv is created and synced with
the requirements.txt. Also make sure `npx` is in PATH.

1. Open a terminal in the `$ContentRoot$`
2. Run `nox -f backend/noxfile.python -s schema`. Schemas of pydantic
   models in `my_fastapi.internal.schemas` module are now
   saved as .json files in `backend/schemas`
3. Run `frontend/scripts/convert_to_zod_schema.sh` which will
   load the .json files generated in the previous step and
   convert them to .ts files where the zod schemas are defined.