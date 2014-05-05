spreads
=======

Statistical analysis of Vegas spreads in sports

Requirements
------------

Spreads uses [Python](https://www.python.org/) version 3 to download the data
set and [Stata](http://www.stata.com/) version 10 or greater for
analysis. Required Python packages are listed in `requirements.txt`.

Download and Install
--------------------

Download the project from [GitHub](https://github.com/wkschwartz/spreads). Then install the Python requirements with [Pip](https://pypi.python.org/pypi/pip):

```bash
$ pip install -r requirements.txt
```

Usage
-----

First, download the data set with `spreads.py`. For its usage instructions, run

```bash
$ python3 spreads.py --help
```

`spreads.py` writes the data to standard out, so you may want to redirect its
output. For example, if you want to save the data in a file called
`spreads.csv`, run

```bash
$ python3 spreads.py > spreads.csv
```

Once you have downloaded the data to, say, `spreads.csv`, import it to Stata using the Stata ADO program `spreads_read.ado`:

```stata
. spreads_read spreads.csv, clear
```

Testing
-------

The Python code comes with automated unit tests. Run them, like usual, with:

```bash
$ python3 -m unittest discover
```