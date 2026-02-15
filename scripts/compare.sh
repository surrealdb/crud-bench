#!/usr/bin/env bash
# ============================================================
# compare.sh - Side-by-side comparison of two benchmark CSVs
#
# Usage: ./scripts/compare.sh "Title" file_a.csv "Label A" file_b.csv "Label B"
#
# Compares Mean latency and OPS between two benchmark runs.
# Shows delta percentages.
# ============================================================
set -euo pipefail

TITLE="${1:?Usage: compare.sh TITLE file_a.csv label_a file_b.csv label_b}"
FILE_A="${2:?Missing file A}"
LABEL_A="${3:?Missing label A}"
FILE_B="${4:?Missing file B}"
LABEL_B="${5:?Missing label B}"

if [[ ! -f "$FILE_A" ]]; then
    echo "ERROR: $FILE_A not found. Run the benchmark first." >&2
    exit 1
fi
if [[ ! -f "$FILE_B" ]]; then
    echo "ERROR: $FILE_B not found. Run the benchmark first." >&2
    exit 1
fi

echo ""
echo "================================================================"
echo " $TITLE"
echo "================================================================"
echo ""

awk -F',' -v la="$LABEL_A" -v lb="$LABEL_B" '
function strip(s) { gsub(/^ +| +$/, "", s); gsub(/ ms$/, "", s); return s + 0 }
BEGIN {
    fmt  = "%-45s  %9s  %9s  %9s  %12s  %12s  %9s\n"
    fmtd = "%-45s  %9.2f  %9.2f  %+8.1f%%  %12.1f  %12.1f  %+8.1f%%\n"
    printf fmt, "Test", "Mean A", "Mean B", "Mean %", "OPS A", "OPS B", "OPS %"
    printf fmt, "---", "------", "------", "------", "-----", "-----", "-----"
}
NR == FNR && FNR > 1 { t=$1; ma[t]=strip($3); oa[t]=$13+0; ord[++n]=t; seen[t]=1; next }
FNR > 1 { t=$1; mb[t]=strip($3); ob[t]=$13+0; if(!(t in seen)){ord[++n]=t; seen[t]=1} }
END {
    for(i=1;i<=n;i++){
        t=ord[i]; a=ma[t]+0; b=mb[t]+0; x=oa[t]+0; y=ob[t]+0
        if(x>0 && y>0){
            mp=(a>0)?((b-a)/a)*100:0; op=(x>0)?((y-x)/x)*100:0
            printf fmtd, t, a, b, mp, x, y, op
        }
    }
    print ""; printf "A = %s | B = %s\n", la, lb
    print "Mean: ms (lower=better). Negative Mean% = B faster."
    print "OPS: ops/sec (higher=better). Positive OPS% = B faster."
}
' "$FILE_A" "$FILE_B"

echo ""
