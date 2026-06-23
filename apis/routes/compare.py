import sys
from pathlib import Path
from fastapi import APIRouter, Depends

project_root = Path(__file__).parent.parent.resolve()
sys.path.insert(0, str(project_root))

from apis.dependencies import get_settings
from apis.schemas.responses import ClimateResponse, ComparePeriodsRequest

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