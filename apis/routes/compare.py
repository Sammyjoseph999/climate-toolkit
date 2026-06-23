import sys
from pathlib import Path
from fastapi import APIRouter, Depends

project_root = Path(__file__).parent.parent.resolve()
sys.path.insert(0, str(project_root))

from apis.dependencies import get_settings
from apis.schemas.responses import ClimateResponse, ComparePeriodsRequest, EnsembleCompareRequest

router = APIRouter()


@router.post("/periods", response_model=ClimateResponse)
async def compare_periods(request: ComparePeriodsRequest, settings=Depends(get_settings)):
    try:
        # periods.py exposes a single compare() entrypoint: it runs a baseline
        # period and a focal year and diffs them. Period 1 is the baseline
        # range; period 2's start year is the focal year.
        from climate_tookit.compare_periods.periods import compare

        baseline_start = int(request.period1_from.split("-")[0])
        baseline_end = int(request.period1_to.split("-")[0])
        focal_year = int(request.period2_from.split("-")[0])

        result = compare(
            location=(request.lat, request.lon),
            baseline_start=baseline_start,
            baseline_end=baseline_end,
            focal_year=focal_year,
            source=request.source,
        )

        if "error" in result:
            return ClimateResponse(
                status_code=400,
                status="REQUEST_UNSUCCESSFUL",
                message=f"Error comparing periods: {result.get('error')}",
                data=result
            )

        return ClimateResponse(
            status_code=200,
            status="REQUEST_SUCCESSFUL",
            message="Period comparison completed successfully",
            data=result
        )

    except Exception as e:
        return ClimateResponse(
            status_code=500,
            status="SERVICE_UNREACHABLE",
            message=f"Error comparing periods: {str(e)}",
            data=None
        )


@router.post("/ensemble", response_model=ClimateResponse)
async def compare_ensemble_periods(request: EnsembleCompareRequest, settings=Depends(get_settings)):
    """NEX-GDDP CMIP6 ensemble Baseline-LTM vs Future-LTM comparison."""
    try:
        from climate_tookit.compare_periods.ensemble_periods import ensemble_compare

        result = ensemble_compare(
            location=(request.lat, request.lon),
            baseline_start=request.baseline_start,
            baseline_end=request.baseline_end,
            future_start=request.future_start,
            future_end=request.future_end,
            scenario=request.scenario,
            fixed_season=request.fixed_season,
            models=request.models,
            verbose=False,
            max_workers=request.workers,
        )

        if "error" in result:
            return ClimateResponse(
                status_code=400,
                status="REQUEST_UNSUCCESSFUL",
                message=f"Error comparing ensemble periods: {result.get('error')}",
                data=result
            )

        return ClimateResponse(
            status_code=200,
            status="REQUEST_SUCCESSFUL",
            message="Ensemble period comparison completed successfully",
            data=result
        )

    except Exception as e:
        return ClimateResponse(
            status_code=500,
            status="SERVICE_UNREACHABLE",
            message=f"Error comparing ensemble periods: {str(e)}",
            data=None
        )