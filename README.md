ComDaAn: Community Data Analysis
================================

This is a suite of tools for conducting analysis from data produced by FOSS
communities. This is currently mainly focusing on git repositories.

Dependencies
------------
The scripts in this repository depend on the following python modules:
 * pandas: https://pandas.pydata.org
 * networkx: https://networkx.github.io
 * bokeh: https://bokeh.pydata.org

They are commonly available via pip or your OS packaging system.
You can run `pipenv install` to install them in a pipenv managed virtualenv.

If you plan to develop on it we advise using `pipenv install -d` to also
bring `black` which we use for the formatting. Make sure to run it on new
code before submitting your contribution.

Running
-------
The scripts require you to provide at least one path to a checked-out git
repository. One than more path can be provided. The scripts also work with
directories containing a tree of git repositories and will traverse them all.
This is a convenient way to analyze teams working across more than one
repository.

For a description of the other options, please run the scripts with the `--help`
argument.

Acknowledgment
--------------
The git log parsing code is heavily based on code from Paul Adams' git-viz
project: https://github.com/therealpadams/git-viz

The ideas behind activity.py and network.py are also influenced by git-viz.

