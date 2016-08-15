#!/bin/bash

# Get the path prefix of the given word. (The parent of the given path.)
__get_path_prefix()
{
        local prefix=${1%"${1##*/}"}
        echo $prefix
}

# A completion function for stor
_stor_complete()
{
        COMPREPLY=()
        local cur
        _get_comp_words_by_ref -n : cur
        local words=$(stor ls "$(__get_path_prefix $cur)" 2>/dev/null) || ""
        COMPREPLY=( $(compgen -W "$words" -- "$cur") )
        __ltrim_colon_completions "$cur"
}

complete -o bashdefault -o default -o filenames -o nospace -F _stor_complete stor
