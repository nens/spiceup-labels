spiceup-labels
==========================================

Introduction

Usage, etc.


Installation
------------

We can be installed with::

  $ pip install spiceup-labels

(TODO: after the first release has been made)


Development installation of this project itself
-----------------------------------------------

We use python's build-in "virtualenv" to get a nice isolated directory. You
only need to run this once::

  $ python3 -m venv .

A virtualenv puts its commands in the ``bin`` directory. So ``bin/pip``,
``bin/pytest``, etc. Set up the dependencies like this::

  $ bin/pip install -r requirements.txt

There will be a script you can run like this::

  $ bin/run-spiceup-labels

It runs the `main()` function in `spiceup-labels/scripts.py`,
adjust that if necessary. The script is configured in `setup.py` (see
`entry_points`).

In order to get nicely formatted python files without having to spend manual
work on it, run the following command periodically::

  $ bin/black spiceup_labels

Run the tests regularly. This also checks with pyflakes, black and it reports
coverage. Pure luxury::

  $ bin/pytest

The tests are also run automatically `on "github actions"
<https://githug.com/nens/spiceup-labels/actions>`_ for
"master" and for pull requests. So don't just make a branch, but turn it into
a pull request right away:

- Prepend the title with "[WIP]", work in progress. That way you make clear it
  isn't ready yet to be merged.

- **Important**: it is easy to give feedback on pull requests. Little comments
  on the individual lines, for instance. So use it to get early feedback, if
  you think that's useful.

- On your pull request page, you also automatically get the feedback from the
  automated tests.

There's also
`coverage reporting <https://coveralls.io/github/nens/spiceup-labels>`_
on coveralls.io (once it has been set up).

If you need a new dependency (like ``requests``), add it in ``setup.py`` in
``install_requires``. Local development tools, like "black", can be added to the
``requirements.txt`` directoy. In both cases, run install again to actuall
install your dependency::

  $ bin/pip install -r requirements.txt


Steps to do after generating with cookiecutter
----------------------------------------------

- Add a new project on https://github.com/nens/ with the same name. Set
  visibility to "public" and do not generate a license or readme.

  Note: "public" means "don't put customer data or sample data with real
  persons' addresses on github"!

- Follow the steps you then see (from "git init" to "git push origin master")
  and your code will be online.

- Go to
  https://github.com/nens/spiceup-labels/settings/collaboration
  and add the teams with write access (you might have to ask someone with
  admin rights to do it).

- Update this readme. Use `.rst
  <http://www.sphinx-doc.org/en/stable/rest.html>`_ as the format.

- Ask Reinout to configure travis and coveralls.

- Remove this section as you've done it all :-)
"# spiceup-labels" 
