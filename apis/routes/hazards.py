import sys
from pathlib import Path
from fastapi import APIRouter, Depends

project_root = Path(__file__).parent.parent.resolve()
sys.path.insert(0, str(project_root))

from apis.dependencies import get_settings
from apis.schemas.responses import ClimateResponse, HazardsRequest

router = APIRouter()


@router.post("/calculate", response_model=ClimateResponse)
async def calculate_hazards(request: HazardsRequest, settings=Depends(get_settings)):
    try:
        from climate_tookit.calculate_hazards.hazards import calculate_hazards

        # Pass season options through as given. Forcing
        # season_start=date_from/season_end=date_to (the previous behaviour)
        # collapses a multi-year range into one giant "season"; omitting them
        # lets the module auto-detect (or use --fixed-season when supplied).
        result = calculate_hazards(
            crop_name=request.crop,
            location_coord=(request.lat, request.lon),
            date_from=request.date_from,
            date_to=request.date_to,
            source=request.source,
            gap_days=request.gap_days,
            min_season_days=30,
            fixed_season=request.fixed_season,
            season_start=request.season_start,
            season_end=request.season_end,
        )

        if "error" in result:
            return ClimateResponse(
                status_code=400,
                status="REQUEST_UNSUCCESSFUL",
                message=f"Error calculating hazards: {result.get('error')}",
                data=result
            )

        return ClimateResponse(
            status_code=200,
            status="REQUEST_SUCCESSFUL",
            message=f"Hazard analysis for {request.crop} completed successfully",
            data=result
        )

    except Exception as e:
        return ClimateResponse(
            status_code=500,
            status="SERVICE_UNREACHABLE",
            message=f"Error calculating hazards: {str(e)}",
            data=None
        )