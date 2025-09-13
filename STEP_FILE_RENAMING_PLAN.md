# Step-Based File Organization Plan

We will progressively introduce numbered step directories and wrapper scripts without breaking existing workflows.

## Step Prefix Convention
- step01_equity_downloads/
- step02_monthly_analysis/
- step03_monthly_comparisons/
- step04_derivatives_udiff/
- step05_... (placeholder for future)
...
- step15_... (future scope)

## Current Mapping (Initial 4 Steps)
| Step | Purpose | Existing Root Scripts (examples) | Planned Wrapper Name |
|------|---------|----------------------------------|----------------------|
| 01 | Equity Bhavcopy Downloads | nse_january_2025_downloader.py, nse_february_2025_downloader.py, ... | step01_download_january_2025.py (etc.) |
| 02 | Monthly Unique Analysis | unique_symbol_analysis_january_comprehensive.py, ... | step02_analyze_january.py (etc.) |
| 03 | Month Comparisons | january_february_increased_only.py, february_march_increased_only.py, ... | step03_compare_jan_feb.py (etc.) |
| 04 | F&O UDiFF Derivatives | nse_fo_udiff_real_endpoint_downloader.py | step04_download_udiff_feb2025.py |

## Wrapper Script Pattern
Each wrapper will:
```python
# stepXX_description_file.py
from original_module import main  # or appropriate entry
if __name__ == "__main__":
    main()
```

## Rationale
- Keeps original filenames for backward compatibility
- Introduces ordered workflow clarity (1→15)
- Enables future orchestration (pipeline runner referencing step numbers)

## Next Actions
- [ ] Generate wrapper scripts for Step 01 (months Jan–Sep 2025)
- [ ] Generate wrappers for Step 02 comprehensive analyses
- [ ] Generate wrappers for Step 03 comparisons
- [ ] Add derivative wrapper for February 2025 UDiFF
- [ ] Update README with step table

## Future Ideas
- Master `run_all_steps.py` orchestrator
- Metadata JSON enumerating steps, dependencies, outputs
- Validation hooks per step
