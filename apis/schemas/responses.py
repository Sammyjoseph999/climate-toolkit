from typing import Any, Literal, Optional
from pydantic import BaseModel, Field


StatusType = Literal["REQUEST_SUCCESSFUL", "REQUEST_UNSUCCESSFUL", "SERVICE_UNREACHABLE"]


class ClimateResponse(BaseModel):
    status_code: int = Field(default=200, description="HTTP status code")
    status: StatusType = Field(default="REQUEST_SUCCESSFUL", description="Response status")
    message: str = Field(default="", description="Human-readable message")
    data: Optional[dict[str, Any]] = Field(default=None, description="Response payload")


class SourceInfo(BaseModel):
    key: str = Field(description="Dataset identifier")
    name: str = Field(description="Dataset name")
    description: str = Field(description="Dataset description")
    variables: list[str] = Field(description="Available variables")


class DataFetchRequest(BaseModel):
    lat: float = Field(ge=-90, le=90, description="Latitude in decimal degrees")
    lon: float = Field(ge=-180, le=180, description="Longitude in decimal degrees")
    source: str = Field(description="Data source key (e.g., chirps, nasa_power)")
    variables: list[str] = Field(description="Variables to download")
    date_from: str = Field(description="Start date in YYYY-MM-DD format")
    date_to: str = Field(description="End date in YYYY-MM-DD format")
    model: Optional[str] = Field(default=None, description="Climate model (for NEX-GDDP)")
    models: Optional[list[str]] = Field(default=None, description="NEX-GDDP only: several models, or ['all']. Returns one series per model; overrides 'model'.")
    scenario: Optional[str] = Field(default=None, description="Emissions scenario (for NEX-GDDP)")
    format: Literal["csv", "json"] = Field(default="json", description="Output format")


class StatisticsRequest(BaseModel):
    lat: float = Field(ge=-90, le=90, description="Latitude in decimal degrees")
    lon: float = Field(ge=-180, le=180, description="Longitude in decimal degrees")
    date_from: str = Field(description="Start date in YYYY-MM-DD format")
    date_to: str = Field(description="End date in YYYY-MM-DD format")
    source: str = Field(description="Data source key")
    gap_days: int = Field(default=30, description="Consecutive dry days to end season")
    min_season_days: int = Field(default=30, description="Minimum season length in days")
    fixed_season: Optional[str] = Field(default=None, description="Fixed season window(s) 'MM-DD:MM-DD[,MM-DD:MM-DD]'; omit for auto-detect")
    model: Optional[str] = Field(default=None, description="Climate model (for NEX-GDDP)")
    scenario: Optional[str] = Field(default=None, description="Emissions scenario (for NEX-GDDP)")


class HazardsRequest(BaseModel):
    lat: float = Field(ge=-90, le=90, description="Latitude in decimal degrees")
    lon: float = Field(ge=-180, le=180, description="Longitude in decimal degrees")
    date_from: str = Field(description="Start date in YYYY-MM-DD format")
    date_to: str = Field(description="End date in YYYY-MM-DD format")
    source: str = Field(description="Data source key")
    crop: str = Field(description="Crop type (e.g., Maize, Beans, Rice)")
    gap_days: int = Field(default=7, description="Consecutive dry days threshold")
    fixed_season: Optional[str] = Field(default=None, description="Fixed season window(s) 'MM-DD:MM-DD[,MM-DD:MM-DD]'")
    season_start: Optional[str] = Field(default=None, description="Explicit season start (YYYY-MM-DD); pair with season_end")
    season_end: Optional[str] = Field(default=None, description="Explicit season end (YYYY-MM-DD); pair with season_start")
    model: Optional[str] = Field(default=None, description="Climate model (for NEX-GDDP)")
    scenario: Optional[str] = Field(default=None, description="Emissions scenario (for NEX-GDDP)")


class EnsembleHazardsRequest(BaseModel):
    lat: float = Field(ge=-90, le=90, description="Latitude in decimal degrees")
    lon: float = Field(ge=-180, le=180, description="Longitude in decimal degrees")
    crop: str = Field(description="Crop type (e.g., Maize, Beans, Millet)")
    start_year: int = Field(description="Start year")
    end_year: int = Field(description="End year")
    scenarios: list[str] = Field(default=["ssp245"], description="SSP scenarios, e.g. ['ssp245','ssp585']")
    models: Optional[list[str]] = Field(default=None, description="Subset of CMIP6 models; null = all 16")
    fixed_season: Optional[str] = Field(default=None, description="Fixed season 'MM-DD:MM-DD[,MM-DD:MM-DD]'; omit for auto-detect")
    workers: int = Field(default=0, description="Parallel fetch workers; 0 = auto")


class EnsembleClimatologyRequest(BaseModel):
    lat: float = Field(ge=-90, le=90, description="Latitude in decimal degrees")
    lon: float = Field(ge=-180, le=180, description="Longitude in decimal degrees")
    start_year: int = Field(description="Start year")
    end_year: int = Field(description="End year")
    scenarios: list[str] = Field(default=["ssp245"], description="SSP scenarios")
    models: Optional[list[str]] = Field(default=None, description="Subset of CMIP6 models; null = all 16")
    workers: int = Field(default=0, description="Parallel fetch workers; 0 = auto")


class EnsembleStatisticsRequest(BaseModel):
    lat: float = Field(ge=-90, le=90, description="Latitude in decimal degrees")
    lon: float = Field(ge=-180, le=180, description="Longitude in decimal degrees")
    start_year: int = Field(description="Start year")
    end_year: int = Field(description="End year")
    scenario: str = Field(default="ssp245", description="SSP scenario")
    fixed_season: Optional[str] = Field(default=None, description="Fixed season 'MM-DD:MM-DD[,MM-DD:MM-DD]'")
    models: Optional[list[str]] = Field(default=None, description="Subset of CMIP6 models; null = all 16")
    workers: int = Field(default=0, description="Parallel fetch workers; 0 = auto")


class EnsembleCompareRequest(BaseModel):
    lat: float = Field(ge=-90, le=90, description="Latitude in decimal degrees")
    lon: float = Field(ge=-180, le=180, description="Longitude in decimal degrees")
    baseline_start: int = Field(description="Baseline period start year")
    baseline_end: int = Field(description="Baseline period end year")
    future_start: int = Field(description="Future period start year")
    future_end: int = Field(description="Future period end year")
    scenario: str = Field(default="ssp245", description="SSP scenario")
    fixed_season: Optional[str] = Field(default=None, description="Fixed season 'MM-DD:MM-DD[,MM-DD:MM-DD]'")
    models: Optional[list[str]] = Field(default=None, description="Subset of CMIP6 models; null = all 16")
    workers: int = Field(default=0, description="Parallel fetch workers; 0 = auto")


class ComparePeriodsRequest(BaseModel):
    lat: float = Field(ge=-90, le=90, description="Latitude in decimal degrees")
    lon: float = Field(ge=-180, le=180, description="Longitude in decimal degrees")
    source: str = Field(description="Data source key")
    period1_from: str = Field(description="First period start date (YYYY-MM-DD)")
    period1_to: str = Field(description="First period end date (YYYY-MM-DD)")
    period2_from: str = Field(description="Second period start date (YYYY-MM-DD)")
    period2_to: str = Field(description="Second period end date (YYYY-MM-DD)")
    model: Optional[str] = Field(default=None, description="Climate model (for NEX-GDDP)")
    scenario: Optional[str] = Field(default=None, description="Emissions scenario (for NEX-GDDP)")