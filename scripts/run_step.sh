#!/bin/bash
# ============================================================
# run_step.sh -- Run a single pipeline step
# Usage: ./scripts/run_step.sh --step 1 --start-date 2024-01-01 --end-date 2024-01-31
# ============================================================
set -e
STEP=""; START=""; END=""; ENV="local"; DATA="./data"
while [[ $# -gt 0 ]]; do
  case $1 in
    --step)       STEP="$2";  shift 2 ;;
    --start-date) START="$2"; shift 2 ;;
    --end-date)   END="$2";   shift 2 ;;
    --env)        ENV="$2";   shift 2 ;;
    *) echo "Unknown: $1"; exit 1 ;;
  esac
done

echo "[Step $STEP] $START -> $END [$ENV]"
STEP_START=$(date +%s)

case $STEP in
  0a) python src/bronze/raw_events_pipeline.py --start-date "$START" --end-date "$END" --env "$ENV" ;;
  0b) python src/bronze/ixp_assignments.py     --start-date "$START" --end-date "$END" --env "$ENV" ;;
  1)  python src/silver/cancel_initiations.py  --start-date "$START" --end-date "$END" --env "$ENV" ;;
  2)  python src/gold/ipd_engagement.py         --start-date "$START" --end-date "$END" --env "$ENV" ;;
  3)  python src/silver/save_attribution.py     --start-date "$START" --end-date "$END" --env "$ENV" ;;
  4)  python src/gold/final_metrics.py          --env "$ENV" ;;
  *)  echo "Unknown step: $STEP. Use 0a|0b|1|2|3|4"; exit 1 ;;
esac

ELAPSED=$(( $(date +%s) - STEP_START ))
echo "✅ Step $STEP complete in ${ELAPSED}s"
