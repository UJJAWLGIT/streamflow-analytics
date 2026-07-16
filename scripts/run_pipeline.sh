#!/usr/bin/env bash
# =============================================================================
# run_pipeline.sh — Full Pipeline Runner (Steps 0–4 + DQ + OPTIMIZE)
# =============================================================================
# Usage:
#   ./scripts/run_pipeline.sh \
#     --start-date 2024-01-01 \
#     --end-date 2024-12-31 \
#     --env local
# =============================================================================

set -euo pipefail

# ── Defaults ───────────────────────────────────────────────────────────────────
START_DATE=""
END_DATE=""
ENV="local"
DATA_PATH="./data"
SKIP_GENERATE="false"
SKIP_DQ="false"

# ── Argument parsing ───────────────────────────────────────────────────────────
while [[ $# -gt 0 ]]; do
  case $1 in
    --start-date)     START_DATE="$2";     shift 2 ;;
    --end-date)       END_DATE="$2";       shift 2 ;;
    --env)            ENV="$2";            shift 2 ;;
    --data-path)      DATA_PATH="$2";      shift 2 ;;
    --skip-generate)  SKIP_GENERATE="true"; shift 1 ;;
    --skip-dq)        SKIP_DQ="true";      shift 1 ;;
    *) echo "Unknown argument: $1"; exit 1 ;;
  esac
done

[[ -z "$START_DATE" || -z "$END_DATE" ]] && {
  echo "❌  --start-date and --end-date are required."
  exit 1
}

RAW_PATH="$DATA_PATH/raw"
OUT_PATH="$DATA_PATH/output"
LOG_DIR="$DATA_PATH/logs"
mkdir -p "$LOG_DIR"
LOG_FILE="$LOG_DIR/pipeline_$(date +%Y%m%d_%H%M%S).log"

START_TS=$(date +%s)

log() { echo "[$(date +'%Y-%m-%d %H:%M:%S')] $*" | tee -a "$LOG_FILE"; }
die() { log "❌  $*"; exit 1; }

log "================================================================"
log "  StreamFlow Analytics — Cancel Flow Pipeline"
log "  Start: $START_DATE | End: $END_DATE | Env: $ENV"
log "  Output: $OUT_PATH"
log "================================================================"

# ── [0] Synthetic data generation (local only) ─────────────────────────────────
if [[ "$SKIP_GENERATE" == "false" && "$ENV" == "local" ]]; then
  log "▶ [0] Generating synthetic data..."
  python data/synthetic/generator.py \
    --start-date "$START_DATE" \
    --end-date "$END_DATE" \
    --companies 100000 \
    --output-path "$RAW_PATH" 2>&1 | tee -a "$LOG_FILE" \
    || die "Synthetic data generation failed"
  log "   ✅ Synthetic data ready"
fi

# ── [Step 0-A] Raw ECS events ──────────────────────────────────────────────────
log "▶ [Step 0-A] Raw ECS Events..."
STEP_0A_START=$(date +%s)
python src/bronze/raw_events_pipeline.py \
  --input-path "$RAW_PATH/raw_events.parquet" \
  --output-path "$OUT_PATH/bronze/raw_events" \
  --start-date "$START_DATE" \
  --end-date "$END_DATE" \
  --env "$ENV" 2>&1 | tee -a "$LOG_FILE" \
  || die "Step 0-A failed"
STEP_0A_END=$(date +%s)
log "   ✅ Step 0-A complete ($(( STEP_0A_END - STEP_0A_START ))s)"

# ── [Step 0-B] IXP assignments (parallel with 0-A in prod) ────────────────────
log "▶ [Step 0-B] IXP Assignments..."
STEP_0B_START=$(date +%s)
python src/bronze/ixp_assignments.py \
  --output-path "$OUT_PATH/bronze/ixp_assignments" \
  --start-date "$START_DATE" \
  --end-date "$END_DATE" \
  --env "$ENV" 2>&1 | tee -a "$LOG_FILE" \
  || die "Step 0-B failed"
STEP_0B_END=$(date +%s)
log "   ✅ Step 0-B complete ($(( STEP_0B_END - STEP_0B_START ))s)"

# ── [Step 1] Cancel Initiations ────────────────────────────────────────────────
log "▶ [Step 1] Cancel Initiations..."
STEP_1_START=$(date +%s)
python src/silver/cancel_initiations.py \
  --raw-events-path "$OUT_PATH/bronze/raw_events" \
  --companies-path  "$RAW_PATH/companies.parquet" \
  --output-path     "$OUT_PATH/silver/stg_cancel_initiations" \
  --start-date      "$START_DATE" \
  --end-date        "$END_DATE" \
  --env             "$ENV" 2>&1 | tee -a "$LOG_FILE" \
  || die "Step 1 failed"
STEP_1_END=$(date +%s)
log "   ✅ Step 1 complete ($(( STEP_1_END - STEP_1_START ))s)"

# ── [Step 2] IPD Detailed Engagement ───────────────────────────────────────────
log "▶ [Step 2] IPD Detailed Engagement..."
STEP_2_START=$(date +%s)
python src/gold/ipd_engagement.py \
  --cancel-initiations-path "$OUT_PATH/silver/stg_cancel_initiations" \
  --raw-events-path         "$OUT_PATH/bronze/raw_events" \
  --offer-catalog-path      "$RAW_PATH/offer_catalog.parquet" \
  --output-path             "$OUT_PATH/gold/rpt_ipd_detailed_engagement" \
  --env                     "$ENV" 2>&1 | tee -a "$LOG_FILE" \
  || die "Step 2 failed"
STEP_2_END=$(date +%s)
log "   ✅ Step 2 complete ($(( STEP_2_END - STEP_2_START ))s)"

# ── [Step 3] Save Attribution ─────────────────────────────────────────────────
log "▶ [Step 3] Save Attribution..."
STEP_3_START=$(date +%s)
python src/silver/save_attribution.py \
  --cancel-initiations-path "$OUT_PATH/silver/stg_cancel_initiations" \
  --ipd-engagement-path     "$OUT_PATH/gold/rpt_ipd_detailed_engagement" \
  --reactive-saves-path     "$RAW_PATH/reactive_saves.parquet" \
  --offer-history-path      "$RAW_PATH/offer_history.parquet" \
  --output-path             "$OUT_PATH/silver/stg_save_attribution" \
  --env                     "$ENV" 2>&1 | tee -a "$LOG_FILE" \
  || die "Step 3 failed"
STEP_3_END=$(date +%s)
log "   ✅ Step 3 complete ($(( STEP_3_END - STEP_3_START ))s)"

# ── [Step 4] Final Metrics ────────────────────────────────────────────────────
log "▶ [Step 4] Final Metrics..."
STEP_4_START=$(date +%s)
python src/gold/final_metrics.py \
  --cancel-initiations-path "$OUT_PATH/silver/stg_cancel_initiations" \
  --ipd-engagement-path     "$OUT_PATH/gold/rpt_ipd_detailed_engagement" \
  --save-attribution-path   "$OUT_PATH/silver/stg_save_attribution" \
  --subscriber-status-path  "$RAW_PATH/subscriber_status.parquet" \
  --output-path             "$OUT_PATH/gold/rpt_cancel_flow_final_metrics" \
  --env                     "$ENV" 2>&1 | tee -a "$LOG_FILE" \
  || die "Step 4 failed"
STEP_4_END=$(date +%s)
log "   ✅ Step 4 complete ($(( STEP_4_END - STEP_4_START ))s)"

# ── [DQ] Data Quality Checks ─────────────────────────────────────────────────
if [[ "$SKIP_DQ" == "false" ]]; then
  log "▶ [DQ] Data Quality Validation..."
  DQ_START=$(date +%s)
  python src/dq/dq_checks.py \
    --all \
    --data-path "$OUT_PATH/gold" 2>&1 | tee -a "$LOG_FILE" \
    || { log "⚠️  DQ checks failed — pipeline output still available"; }
  DQ_END=$(date +%s)
  log "   ✅ DQ checks complete ($(( DQ_END - DQ_START ))s)"
fi

# ── Summary ────────────────────────────────────────────────────────────────────
END_TS=$(date +%s)
TOTAL_MINS=$(( (END_TS - START_TS) / 60 ))
TOTAL_SECS=$(( (END_TS - START_TS) % 60 ))

log ""
log "================================================================"
log "  ✅ Pipeline Complete! Total: ${TOTAL_MINS}m ${TOTAL_SECS}s"
log ""
log "  Step runtimes:"
log "    0-A Raw Events:    $(( STEP_0A_END - STEP_0A_START ))s"
log "    0-B IXP:           $(( STEP_0B_END - STEP_0B_START ))s"
log "    1 Initiations:     $(( STEP_1_END - STEP_1_START ))s"
log "    2 IPD Engagement:  $(( STEP_2_END - STEP_2_START ))s"
log "    3 Save Attribution:$(( STEP_3_END - STEP_3_START ))s"
log "    4 Final Metrics:   $(( STEP_4_END - STEP_4_START ))s"
log ""
log "  Output tables:"
log "    ⭐⭐⭐ $OUT_PATH/gold/rpt_cancel_flow_final_metrics"
log "    ⭐⭐⭐ $OUT_PATH/gold/rpt_ipd_detailed_engagement"
log "    Log:  $LOG_FILE"
log "================================================================"
