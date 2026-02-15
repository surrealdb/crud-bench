#!/usr/bin/env bash
# ============================================================
# compare-multi.sh - Compare OPS across 2-3 benchmark CSVs
#
# Usage: ./scripts/compare-multi.sh "Title" \
#            file1.csv "Label1" file2.csv "Label2" [file3.csv "Label3"]
#
# Shows OPS for each benchmark across all backends side by side.
# ============================================================
set -euo pipefail

TITLE="${1:?Usage: compare-multi.sh TITLE file1.csv label1 file2.csv label2 [file3.csv label3]}"
shift

# Parse file/label pairs
declare -a FILES=()
declare -a LABELS=()
while [[ $# -ge 2 ]]; do
    FILES+=("$1")
    LABELS+=("$2")
    shift 2
done

COUNT=${#FILES[@]}
if [[ $COUNT -lt 2 ]]; then
    echo "ERROR: Need at least 2 file/label pairs" >&2
    exit 1
fi

# Check files exist
for f in "${FILES[@]}"; do
    if [[ ! -f "$f" ]]; then
        echo "ERROR: $f not found. Run the benchmark first." >&2
        exit 1
    fi
done

echo ""
echo "================================================================"
echo " $TITLE"
echo "================================================================"
echo ""

if [[ $COUNT -eq 2 ]]; then
    awk -F',' -v la="${LABELS[0]}" -v lb="${LABELS[1]}" '
    function strip(s) { gsub(/^ +| +$/, "", s); gsub(/ ms$/, "", s); return s + 0 }
    BEGIN {
        fmt  = "%-45s  %9s  %9s  %9s  %12s  %12s  %9s\n"
        fmtd = "%-45s  %9.2f  %9.2f  %+8.1f%%  %12.1f  %12.1f  %+8.1f%%\n"
        printf fmt, "Test", "Mean A", "Mean B", "Mean %", "OPS A", "OPS B", "OPS %"
        printf fmt, "---", "------", "------", "------", "-----", "-----", "-----"
    }
    NR==FNR && FNR>1 { t=$1; ma[t]=strip($3); oa[t]=$13+0; ord[++n]=t; seen[t]=1; next }
    FNR>1 { t=$1; mb[t]=strip($3); ob[t]=$13+0; if(!(t in seen)){ord[++n]=t; seen[t]=1} }
    END {
        for(i=1;i<=n;i++){
            t=ord[i]; a=ma[t]+0; b=mb[t]+0; x=oa[t]+0; y=ob[t]+0
            if(x>0&&y>0){
                mp=(a>0)?((b-a)/a)*100:0; op=(x>0)?((y-x)/x)*100:0
                printf fmtd, t, a, b, mp, x, y, op
            }
        }
        printf "\nA = %s | B = %s\n", la, lb
    }
    ' "${FILES[0]}" "${FILES[1]}"
elif [[ $COUNT -eq 3 ]]; then
    awk -F',' -v la="${LABELS[0]}" -v lb="${LABELS[1]}" -v lc="${LABELS[2]}" \
              -v f1="${FILES[0]}" -v f2="${FILES[1]}" -v f3="${FILES[2]}" '
    function strip(s) { gsub(/^ +| +$/, "", s); gsub(/ ms$/, "", s); return s + 0 }
    BEGIN {
        fmt  = "%-45s  %9s  %9s  %9s  %12s  %12s  %12s\n"
        fmtd = "%-45s  %9.2f  %9.2f  %9.2f  %12.1f  %12.1f  %12.1f\n"
        printf fmt, "Test", "Mean A", "Mean B", "Mean C", "OPS A", "OPS B", "OPS C"
        printf fmt, "---", "------", "------", "------", "-----", "-----", "-----"
    }
    FILENAME==f1 && FNR>1 { t=$1; ma[t]=strip($3); oa[t]=$13+0; ord[++n]=t; seen[t]=1; next }
    FILENAME==f2 && FNR>1 { t=$1; mb[t]=strip($3); ob[t]=$13+0; if(!(t in seen)){ord[++n]=t;seen[t]=1}; next }
    FILENAME==f3 && FNR>1 { t=$1; mc[t]=strip($3); oc[t]=$13+0; if(!(t in seen)){ord[++n]=t;seen[t]=1} }
    END {
        for(i=1;i<=n;i++){
            t=ord[i]
            printf fmtd, t, ma[t]+0, mb[t]+0, mc[t]+0, oa[t]+0, ob[t]+0, oc[t]+0
        }
        printf "\nA = %s | B = %s | C = %s\n", la, lb, lc
    }
    ' "${FILES[0]}" "${FILES[1]}" "${FILES[2]}"
fi

echo ""
