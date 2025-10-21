import json
import sys

RESULTS_PATH = sys.argv[1] if len(sys.argv) > 1 else '/tmp/watermark_etl_results.json'
try:
    with open(RESULTS_PATH, 'r') as f:
        results = json.load(f)
except Exception as e:
    print(f"❌ No results file found or error reading file: {e}")
    sys.exit(1)

print("📈 Total symbols processed:", results.get("total_symbols", "N/A"))
if "successful" in results and "total_symbols" in results:
    pct = (results["successful"] / results["total_symbols"] * 100) if results["total_symbols"] else 0
    print(f"✅ Successful: {results['successful']} ({pct:.1f}%)")
else:
    print("✅ Successful: N/A")
print(f"❌ Failed: {results.get('failed', 'N/A')}")
if "duration_minutes" in results:
    print(f"⏱️  Duration: {results['duration_minutes']:.1f} minutes")
    if results.get('total_symbols', 0) > 0 and results.get('duration_minutes', 0) > 0:
        efficiency = results['successful'] / results['duration_minutes']
        print(f"⚡ Processing efficiency: {efficiency:.1f} symbols/minute")
print(f"✅ Watermarks updated for {results.get('successful', 'N/A')} symbols")
print("   - FIRST_FISCAL_DATE set (if NULL)")
print("   - LAST_FISCAL_DATE updated to latest data")
print("   - LAST_SUCCESSFUL_RUN = current timestamp")
print("   - CONSECUTIVE_FAILURES reset to 0")
if results.get('suspended_marked', 0) > 0:
    print(f"🔒 Symbols marked as API_ELIGIBLE='SUS': {results['suspended_marked']}")
