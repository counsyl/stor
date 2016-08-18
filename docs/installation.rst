Installation
============

To install the latest release, type::

    pip install stor

To install the latest code directly from source, type::

    pip install git+git://github.com/counsyl/stor.git


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
    cp `which stor-completion.bash` `brew --prefix`/etc/bash_completion.d/stor

Linux
~~~~~

Depending on your Linux distribution, you will need to first install `Bash Completion <https://github.com/scop/bash-completion>`_.

It can be installed using apt-get or yum::

    apt-get install bash-completion

The bash completion script for stor can then be installed with::

    cp `which stor-completion.bash` /etc/bash_completion.d/stor

If you don't have permissions to install the script to /etc, it can also be saved in your home directory as follows::

    cat `which stor-completion.bash` >> ~/.bash_completion
