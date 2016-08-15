Installation
============

To install the latest release, type::

    pip install counsyl-storage-utils

To install the latest code directly from source, type::

    pip install git+git://github.counsyl.com/dev/counsyl-storage-utils.git


..  _cli_tab_completion_installation:

CLI Tab Completion Installation
-------------------------------

In order to install tab completion with the stor CLI, one must do some additional
steps based on their environment. Instructions for OSX and Linux are provided in the following.

OSX
~~~

Tab completion requires the installation of `Bash Completion <https://github.com/scop/bash-completion>`_,
which can be be installed with `Homebrew <http://brew.sh/>`_.

Install Bash Completion using::

    brew install bash-completion

After this, you will need to edit your ~/.bashrc or ~/.bash_profile to include the following lines::

    if [ -f $(brew --prefix)/etc/bash_completion ]; then
        . $(brew --prefix)/etc/bash_completion
    fi

The final step is to then copy stor's tab completion script in the proper location::

    mkdir `brew --prefix`/etc/bash_completion.d
    curl -o `brew --prefix`/etc/bash_completion.d/stor https://github.counsyl.com/dev/counsyl-storage-utils/storage_utils/stor-completion.bash
