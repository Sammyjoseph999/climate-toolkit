import sys
from pathlib import Path
from typing import Optional
from fastapi import APIRouter, Depends, HTTPException, Query

project_root = Path(__file__).parent.parent.resolve()
sys.path.insert(0, str(project_root))

from apis.dependencies import get_settings
from apis.schemas.responses import ClimateResponse

router = APIRouter()


@router.post("/analyze", response_model=ClimateResponse)
async def analyze_seasons(
    lat: float = Query(..., description="Latitude"),
    lon: float = Query(..., description="Longitude"),
    start_year: int = Query(..., description="Start year"),
    end_year: int = Query(..., description="End year"),
    source: str = Query("auto", description="Data source"),
    fixed_season: Optional[str] = Query(None, description="Fixed season window (MM-DD:MM-DD)"),
    settings=Depends(get_settings)
):
    try:
        from climate_tookit.season_analysis.seasons import (
            fetch_and_analyze_years,
            fetch_and_analyze_years_fixed,
        )

        if fixed_season:
            from climate_tookit.season_analysis.seasons import parse_fixed_seasons
            fixed_defs = parse_fixed_seasons(fixed_season)
            seasons_dict, annual_dict = fetch_and_analyze_years_fixed(
                lat, lon,
                fixed_seasons=fixed_defs,
                start_year=start_year,
                end_year=end_year,
                source=source,
            )
        else:
            seasons_dict, annual_dict = fetch_and_analyze_years(
                lat, lon,
                start_year=start_year,
                end_year=end_year,
                source=source,
            )

        return ClimateResponse(
            status_code=200,
            status="REQUEST_SUCCESSFUL",
            message="Season analysis completed successfully",
            data={
                "seasons": seasons_dict,
                "annual": annual_dict,
                "location": {"lat": lat, "lon": lon},
                "source": source,
                "mode": "fixed" if fixed_season else "auto",
            }
        )

    except Exception as e:
        return ClimateResponse(
            status_code=500,
            status="SERVICE_UNREACHABLE",
            message=f"Error analyzing seasons: {str(e)}",
            data=None
        )
