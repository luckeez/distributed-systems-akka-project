#!/bin/bash
# to use: watch "bash getinfo.sh"

tail -n 90 logs/getinfo.ans | gawk -v RS= -v ORS="\n\n" '
{
    id = -1
    for (i=1; i<=NF; i++) {
        if ($i == "REPLICA") {
            id = $(i+1)
            break
        }
    }
    if (id >= 0) {
        blocks[id] = $0
        ids[id] = id
    }
}
END {
    n = asort(ids)
    for (i=1; i<=n; i++) {
	block = blocks[ids[i]]
	sub(/^[^\n]*\n/, "", block)
        print block
    }
}
'
