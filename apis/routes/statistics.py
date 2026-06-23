import sys
from pathlib import Path
from fastapi import APIRouter, Depends, HTTPException

project_root = Path(__file__).parent.parent.resolve()
sys.path.insert(0, str(project_root))

from apis.dependencies import get_settings
from apis.schemas.responses import ClimateResponse, StatisticsRequest

router = APIRouter()


@router.post("/analyze", response_model=ClimateResponse)
async def analyze_climate_statistics(request: StatisticsRequest, settings=Depends(get_settings)):
    try:
        from climate_tookit.climate_statistics.statistics import (
            analyze_climate_statistics as analyze_stats
        )

        # analyze_climate_statistics works on whole years and takes
        # start_year/end_year (not date_range); it does not accept
        # gap_days/min_season_days. Derive the years from the request range.
        start_year = int(request.date_from.split("-")[0])
        end_year = int(request.date_to.split("-")[0])

        result = analyze_stats(
            location_coord=(request.lat, request.lon),
            start_year=start_year,
            end_year=end_year,
            source=request.source,
            fixed_season=request.fixed_season,
            model=request.model,
            scenario=request.scenario
        )

        if "error" in result:
            return ClimateResponse(
                status_code=400,
                status="REQUEST_UNSUCCESSFUL",
                message=f"Error analyzing climate statistics: {result.get('error')}",
                data=result
            )

        return ClimateResponse(
            status_code=200,
            status="REQUEST_SUCCESSFUL",
            message="Climate statistics analysis completed successfully",
            data=result
        )

    except Exception as e:
        return ClimateResponse(
            status_code=500,
            status="SERVICE_UNREACHABLE",
            message=f"Error analyzing climate statistics: {str(e)}",
            data=None
        )