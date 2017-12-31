# Cebes Documentation

Our documentation uses extended Markdown, as implemented by MkDocs.

## Building the documentation

- install MkDocs and themes: `pip install mkdocs mkdocs-bootswatch`
- `cd` to the `docs/` folder and run:

        $ mkdocs serve # Starts a local webserver: localhost:8000
        $ mkdocs build # Builds a static site in "site" directory
        
## API Reference pages

They are taken from `cebes-python`:

```bash
/code/cebes/cebes-python$ pydocmd simple pycebes.core.dataframe.Dataframe++ pycebes.core.dataframe.GroupedDataframe+ > dataframe_reference.md
```