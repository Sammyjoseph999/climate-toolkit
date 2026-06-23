import sys
from pathlib import Path
from typing import Optional, List
from fastapi import APIRouter, Depends, Query

project_root = Path(__file__).parent.parent.resolve()
sys.path.insert(0, str(project_root))

from apis.dependencies import get_settings
from apis.schemas.responses import ClimateResponse

router = APIRouter()


@router.post("/analyze", response_model=ClimateResponse)
async def analyze_climatology(
    lat: float = Query(..., description="Latitude"),
    lon: float = Query(..., description="Longitude"),
    start_year: int = Query(..., description="Start year"),
    end_year: int = Query(..., description="End year"),
    source: str = Query(..., description="Data source"),
    settings=Depends(get_settings)
):
    try:
        from climate_tookit.climatology.long_term_climatology import calculate_climatology

        result = calculate_climatology(
            location_coord=(lat, lon),
            start_year=start_year,
            end_year=end_year,
            source=source,
        )

        if "error" in result:
            return ClimateResponse(
                status_code=400,
                status="REQUEST_UNSUCCESSFUL",
                message=f"Error calculating climatology: {result.get('error')}",
                data=result
            )

        return ClimateResponse(
            status_code=200,
            status="REQUEST_SUCCESSFUL",
            message="Climatology analysis completed successfully",
            data=result
        )

    except Exception as e:
        return ClimateResponse(
            status_code=500,
            status="SERVICE_UNREACHABLE",
            message=f"Error calculating climatology: {str(e)}",
            data=None
        )
