import sys
from pathlib import Path
from typing import Optional, List
from fastapi import APIRouter, Depends, Query

project_root = Path(__file__).parent.parent.resolve()
sys.path.insert(0, str(project_root))

from apis.dependencies import get_settings
from apis.schemas.responses import ClimateResponse, EnsembleClimatologyRequest

router = APIRouter()


@router.post("/analyze", response_model=ClimateResponse)
async def analyze_climatology(
    lat: float = Query(..., description="Latitude"),
    lon: float = Query(..., description="Longitude"),
    start_year: int = Query(..., description="Start year"),
    end_year: int = Query(..., description="End year"),
    source: str = Query(..., description="Data source"),
    scenario: str = Query("ssp245", description="SSP scenario (only used when source=nex_gddp)"),
    settings=Depends(get_settings)
):
    try:
        # NEX-GDDP runs the CMIP6 ensemble (mirrors the CLI), which needs a
        # scenario; other sources use the single-series climatology.
        if source == "nex_gddp":
            from climate_tookit.climatology.long_term_climatology import (
                calculate_climatology_ensemble,
            )
            result = calculate_climatology_ensemble(
                location_coord=(lat, lon),
                start_year=start_year,
                end_year=end_year,
                scenario=scenario,
                verbose=False,
            )
        else:
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


@router.post("/ensemble", response_model=ClimateResponse)
async def ensemble_climatology(request: EnsembleClimatologyRequest, settings=Depends(get_settings)):
    """NEX-GDDP CMIP6 ensemble climatology, one result per requested scenario."""
    try:
        from climate_tookit.climatology.long_term_climatology import (
            calculate_climatology_ensemble,
        )

        all_results = {}
        any_ok = False
        for scenario in request.scenarios:
            res = calculate_climatology_ensemble(
                location_coord=(request.lat, request.lon),
                start_year=request.start_year,
                end_year=request.end_year,
                scenario=scenario,
                models=request.models,
                verbose=False,
                max_workers=request.workers,
            )
            all_results[scenario] = res
            if "error" not in res:
                any_ok = True

        if not any_ok:
            return ClimateResponse(
                status_code=400,
                status="REQUEST_UNSUCCESSFUL",
                message="Ensemble climatology failed for all scenarios",
                data=all_results
            )

        payload = all_results[request.scenarios[0]] if len(request.scenarios) == 1 else all_results
        return ClimateResponse(
            status_code=200,
            status="REQUEST_SUCCESSFUL",
            message="Ensemble climatology completed successfully",
            data=payload
        )

    except Exception as e:
        return ClimateResponse(
            status_code=500,
            status="SERVICE_UNREACHABLE",
            message=f"Error calculating ensemble climatology: {str(e)}",
            data=None
        )
