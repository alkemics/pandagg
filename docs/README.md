## Sphinx documentation

Documentation stems from 3 sources:
- automatically generated based on repository sources in `docs/source/reference` directory 
- manually written documentation in all other files of `docs/source` directory
- a jupyter notebook file generated following procedure in `example/imdb`, then running notebook and exporting 
html file

#### Procedure
```
# auto doc generation
rm -r docs/source/reference/*
sphinx-apidoc -o docs/source/reference pandagg -Te

# build html
rm -r docs/build/*
sphinx-build -b html docs/source docs/build

# follow example/imdb instructions to generate notebook html
# then copy generated file into `build/_external` directory
mkdir docs/build/_external
cp examples/imdb/IMDB\ exploration.html docs/build/_external/imdb_exploration.html
cp -r examples/imdb/ressources docs/build/_external/
```
