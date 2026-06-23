import sys
import os
from datetime import datetime
from pathlib import Path

from fastapi import APIRouter, Depends

project_root = Path(__file__).parent.parent.resolve()
sys.path.insert(0, str(project_root))
sys.path.insert(0, str(project_root / "climate_tookit" / "fetch_data" / "source_data"))

from apis.schemas.responses import (
    ClimateResponse,
    SourceInfo,
    DataFetchRequest,
)

router = APIRouter()

from apis.dependencies import get_settings
from sources.utils.models import ClimateDataset, ClimateVariable, SoilVariable
from source_data import SourceData
from sources.utils.settings import Settings


AVAILABLE_SOURCES = [
    SourceInfo(
        key="chirps",
        name="CHIRPS",
        description="Climate Hazards group InfraRed Precipitation with Station data",
        variables=["precipitation"],
    ),
    SourceInfo(
        key="agera_5",
        name="AgERA5",
        description="Agriculltural ERA5 - daily weather data for agriculture",
        variables=["precipitation", "max_temperature", "min_temperature"],
    ),
    SourceInfo(
        key="terraclimate",
        name="TerraClimate",
        description="TerraClimate - monthly climate data",
        variables=["precipitation", "max_temperature", "min_temperature", "soil_moisture", "wind_speed", "solar_radiation"],
    ),
    SourceInfo(
        key="imerg",
        name="IMERG",
        description="Integrated Multi-satellitE Retrievals for GPM",
        variables=["precipitation"],
    ),
    SourceInfo(
        key="tamsat",
        name="TAMSAT",
        description="TAMSAT - African rainfall data",
        variables=["precipitation"],
    ),
    SourceInfo(
        key="chirts",
        name="CHIRTS",
        description="Climate Hazards group InfraRed Temperature with Stations",
        variables=["max_temperature", "min_temperature"],
    ),
    SourceInfo(
        key="era_5",
        name="ERA5",
        description="ECMWF Reanalysis v5",
        variables=["precipitation", "max_temperature", "min_temperature", "wind_speed"],
    ),
    SourceInfo(
        key="nex_gddp",
        name="NEX-GDDP",
        description="NASA Earth Exchange Global Daily Downscaled Projections",
        variables=["precipitation", "max_temperature", "min_temperature"],
    ),
    SourceInfo(
        key="nasa_power",
        name="NASA POWER",
        description="NASA Prediction of Worldwide Energy Resources",
        variables=["precipitation", "max_temperature", "min_temperature", "soil_moisture", "wind_speed", "solar_radiation", "humidity"],
    ),
    SourceInfo(
        key="cmip_6",
        name="CMIP6",
        description="Coupled Model Intercomparison Project Phase 6",
        variables=["precipitation", "max_temperature", "min_temperature"],
    ),
    SourceInfo(
        key="soil_grid",
        name="SoilGrids",
        description="ISRIC SoilGrids - global soil data",
        variables=["bulk_density", "coarse_fragments", "ph", "sand_content", "clay_content", "organic_carbon", "silt_content"],
    ),
]


@router.get("/sources", response_model=ClimateResponse)
async def get_sources():
    sources_data = [source.model_dump() for source in AVAILABLE_SOURCES]
    return ClimateResponse(
        status_code=200,
        status="REQUEST_SUCCESSFUL",
        message="Available climate data sources",
        data={"sources": sources_data}
    )


@router.post("/fetch", response_model=ClimateResponse)
async def fetch_data(request: DataFetchRequest, settings=Depends(get_settings)):
    try:
        source_enum = getattr(ClimateDataset, request.source, None)
        
        if not source_enum:
            raise HTTPException(
                status_code=400,
                detail=f"Unknown source: {request.source}. Available sources: {[s.key for s in AVAILABLE_SOURCES]}"
            )

        variables = []
        for v in request.variables:
            if hasattr(ClimateVariable, v):
                variables.append(getattr(ClimateVariable, v))
            elif hasattr(SoilVariable, v):
                variables.append(getattr(SoilVariable, v))
            else:
                raise HTTPException(
                    status_code=400,
                    detail=f"Unknown variable: {v}"
                )

        date_from = datetime.strptime(request.date_from, "%Y-%m-%d").date()
        date_to = datetime.strptime(request.date_to, "%Y-%m-%d").date()

        source_data = SourceData(
            location_coord=(request.lat, request.lon),
            variables=variables,
            source=source_enum,
            date_from_utc=date_from,
            date_to_utc=date_to,
            settings=settings,
            model=request.model,
            scenario=request.scenario
        )

        climate_data = source_data.download()

        if request.format == "csv":
            data_dict = climate_data.to_dict(orient="records")
        else:
            data_dict = climate_data.to_dict(orient="records")

        return ClimateResponse(
            status_code=200,
            status="REQUEST_SUCCESSFUL",
            message=f"Successfully fetched {len(climate_data)} records from {request.source}",
            data={
                "records": data_dict,
                "count": len(climate_data),
                "columns": list(climate_data.columns),
                "source": request.source,
                "location": {"lat": request.lat, "lon": request.lon},
                "date_range": {"from": request.date_from, "to": request.date_to}
            }
        )

    except HTTPException:
        raise
    except Exception as e:
        return ClimateResponse(
            status_code=500,
            status="SERVICE_UNREACHABLE",
            message=f"Error fetching data: {str(e)}",
            data=None
        )