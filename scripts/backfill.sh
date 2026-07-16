#!/bin/bash
# ============================================================
# backfill.sh -- Historical Backfill Runner
# ============================================================
# Runs Step 0->4 in monthly batches from START to END.
# Usage:
#   ./scripts/backfill.sh --start-date 2024-01-01 --end-date 2024-12-31 --env prod
# ============================================================
set -e

START_DATE=""; END_DATE=""; ENV="local"; DATA_PATH="./data"
while [[ $# -gt 0 ]]; do
  case $1 in
    --start-date) START_DATE="$2"; shift 2 ;;
    --end-date)   END_DATE="$2";   shift 2 ;;
    --env)        ENV="$2";        shift 2 ;;
    *) echo "Unknown: $1"; exit 1 ;;
  esac
done
[[ -z "$START_DATE" || -z "$END_DATE" ]] && echo "ERROR: --start-date and --end-date required" && exit 1

echo "================================================================"
echo "  StreamFlow Analytics — Historical Backfill"
echo "  Range : $START_DATE -> $END_DATE"
echo "  Env   : $ENV"
echo "================================================================"

# Loop month by month
current="$START_DATE"
while [[ "$current" < "$END_DATE" || "$current" == "$END_DATE" ]]; do
  month_end=$(date -d "$current +1 month -1 day" +%Y-%m-%d 2>/dev/null || date -v+1m -v-1d -jf "%Y-%m-%d" "$current" +%Y-%m-%d)
  if [[ "$month_end" > "$END_DATE" ]]; then month_end="$END_DATE"; fi

  echo ""
  echo ">>> Processing: $current -> $month_end"
  START_TS=$(date +%s)

  # Step 0-A: Raw events
  echo "  [Step 0-A] Raw ECS events..."
  python src/bronze/raw_events_pipeline.py \
    --start-date "$current" --end-date "$month_end" \
    --output-path "$DATA_PATH/output/stg_raw_events" --env "$ENV"

  # Step 0-B: IXP assignments
  echo "  [Step 0-B] IXP assignments..."
  python src/bronze/ixp_assignments.py \
    --start-date "$current" --end-date "$month_end" \
    --output-path "$DATA_PATH/output/stg_ixp_assignments" --env "$ENV"

  # Steps 1-4 (full rebuild after each Step 0 batch)
  for step in 1 2 3 4; do
    echo "  [Step $step]..."
    ./scripts/run_step.sh --step $step --start-date "$current" --end-date "$month_end" --env "$ENV"
  done

  ELAPSED=$(( $(date +%s) - START_TS ))
  echo "  Batch complete in ${ELAPSED}s"
  current=$(date -d "$current +1 month" +%Y-%m-%d 2>/dev/null || date -v+1m -jf "%Y-%m-%d" "$current" +%Y-%m-%d)
done

echo ""
echo "================================================================"
echo "  Backfill COMPLETE: $START_DATE -> $END_DATE"
echo "================================================================"
