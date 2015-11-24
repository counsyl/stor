# -*- coding: utf-8 -*-
#
# {{ repo_name }} documentation build configuration file
import inspect
import os
import re
import subprocess


def get_version():
    """
    Extracts the version number from setup.py
    """
    proc = subprocess.Popen(['make', 'version'], cwd='..', stdout=subprocess.PIPE)
    return proc.communicate()[0]


# -- General configuration ------------------------------------------------

extensions = [
    'sphinx.ext.autodoc',
    'sphinx.ext.intersphinx',
    #'sphinx.ext.viewcode',
]

# Add any paths that contain templates here, relative to this directory.
templates_path = ['_templates']

# The suffix of source filenames.
source_suffix = '.rst'

# The master toctree document.
master_doc = 'toc'

# General information about the project.
project = u'counsyl-io'
copyright = u'2015, Counsyl Inc.'

# The short X.Y version.
version = get_version()
# The full version, including alpha/beta/rc tags.
release = version

exclude_patterns = ['_build']

# The name of the Pygments (syntax highlighting) style to use.
pygments_style = 'sphinx'

intersphinx_mapping = {
    'python': ('http://python.readthedocs.org/en/v2.7.2/', None),
    'django': ('http://django.readthedocs.org/en/latest/', None),
    #'celery': ('http://celery.readthedocs.org/en/latest/', None),
}

# -- Options for HTML output ----------------------------------------------

html_theme = 'default'
#html_theme_path = []

on_rtd = os.environ.get('READTHEDOCS', None) == 'True'
if not on_rtd:  # only import and set the theme if we're building docs locally
    import sphinx_rtd_theme
    html_theme = 'sphinx_rtd_theme'
    html_theme_path = [sphinx_rtd_theme.get_html_theme_path()]

# Add any paths that contain custom static files (such as style sheets) here,
# relative to this directory. They are copied after the builtin static files,
# so a file named "default.css" will overwrite the builtin "default.css".
# html_static_path = ['_static']

# Custom sidebar templates, maps document names to template names.
#html_sidebars = {}

# Additional templates that should be rendered to pages, maps page names to
# template names.
#html_additional_pages = {}

# If true, "Created using Sphinx" is shown in the HTML footer. Default is True.
html_show_sphinx = False

# If true, "(C) Copyright ..." is shown in the HTML footer. Default is True.
html_show_copyright = True

# Output file base name for HTML help builder.
htmlhelp_basename = '{{ repo_name }}doc'


# -- Options for LaTeX output ---------------------------------------------

latex_elements = {
# The paper size ('letterpaper' or 'a4paper').
#'papersize': 'letterpaper',

# The font size ('10pt', '11pt' or '12pt').
#'pointsize': '10pt',

# Additional stuff for the LaTeX preamble.
#'preamble': '',
}

# Grouping the document tree into LaTeX files. List of tuples
# (source start file, target name, title,
#  author, documentclass [howto, manual, or own class]).
latex_documents = [
  ('index', '{{ repo_name }}.tex', u'{{ repo_name }} Documentation',
   u'{{ author_name }}', 'manual'),
]

# -- Options for manual page output ---------------------------------------

# One entry per manual page. List of tuples
# (source start file, name, description, authors, manual section).
man_pages = [
    ('index', '{{ repo_name }}', u'{{ repo_name }} Documentation',
     [u'{{ author_name }}'], 1)
]

# -- Options for Texinfo output -------------------------------------------

# Grouping the document tree into Texinfo files. List of tuples
# (source start file, target name, title, author,
#  dir menu entry, description, category)
texinfo_documents = [
  ('index', '{{ repo_name }}', u'{{ repo_name }} Documentation',
   u'{{ author_name }}', '{{ repo_name }}', 'A short description',
   'Miscellaneous'),
]


def process_django_model_docstring(app, what, name, obj, options, lines):
    """
    Does special processing for django model docstrings, making docs for
    fields in the model.
    """
    # This causes import errors if left outside the function
    from django.db import models
    
    # Only look at objects that inherit from Django's base model class
    if inspect.isclass(obj) and issubclass(obj, models.Model):
        # Grab the field list from the meta class
        fields = obj._meta.fields
    
        for field in fields:
            # Decode and strip any html out of the field's help text
            help_text = strip_tags(force_unicode(field.help_text))
            
            # Decode and capitalize the verbose name, for use if there isn't
            # any help text
            verbose_name = force_unicode(field.verbose_name).capitalize()
            
            if help_text:
                # Add the model field to the end of the docstring as a param
                # using the help text as the description
                lines.append(u':param %s: %s' % (field.attname, help_text))
            else:
                # Add the model field to the end of the docstring as a param
                # using the verbose name as the description
                lines.append(u':param %s: %s' % (field.attname, verbose_name))
                
            # Add the field's type to the docstring
            lines.append(u':type %s: %s' % (field.attname, type(field).__name__))
    
    # Return the extended docstring
    return lines  


def setup(app):
    # Register the docstring processor with sphinx
    app.connect('autodoc-process-docstring', process_django_model_docstring)
