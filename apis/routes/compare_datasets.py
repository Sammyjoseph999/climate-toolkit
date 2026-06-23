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
async def compare_datasets(
    lat: float = Query(..., description="Latitude"),
    lon: float = Query(..., description="Longitude"),
    start: str = Query(..., description="Start date YYYY-MM-DD"),
    end: str = Query(..., description="End date YYYY-MM-DD"),
    sources: List[str] = Query(..., description="List of data sources"),
    model: Optional[str] = Query(None, description="NEX-GDDP model"),
    scenario: Optional[str] = Query(None, description="NEX-GDDP scenario"),
    settings=Depends(get_settings)
):
    try:
        from climate_tookit.compare_datasets.compare_datasets import (
            compare_sources,
            print_report
        )

        results = compare_sources(
            sources=sources,
            lat=lat,
            lon=lon,
            start=start,
            end=end,
            nex_model=model or "MRI-ESM2-0",
            nex_scenario=scenario or "ssp245"
        )

        stats = print_report(results)

        if not results:
            return ClimateResponse(
                status_code=400,
                status="REQUEST_UNSUCCESSFUL",
                message="No data retrieved from selected sources",
                data=None
            )

        return ClimateResponse(
            status_code=200,
            status="REQUEST_SUCCESSFUL",
            message="Dataset comparison completed successfully",
            data=stats
        )

    except Exception as e:
        return ClimateResponse(
            status_code=500,
            status="SERVICE_UNREACHABLE",
            message=f"Error comparing datasets: {str(e)}",
            data=None
        )
