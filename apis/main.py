import sys
import os
from typing import Optional

from fastapi import FastAPI, Request, Depends
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import HTMLResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates

project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, project_root)
sys.path.insert(0, os.path.join(project_root, "climate_tookit", "fetch_data", "source_data"))

from apis.schemas.responses import ClimateResponse
from apis.routes import data, statistics, hazards, compare, seasons, climatology, compare_datasets

app = FastAPI(
    title="Climate Toolkit",
    description="A unified toolkit for retrieving climate data from CHIRPS, AGERA5, TerraClimate, IMERG, TAMSAT, CHIRTS, ERA5, NEX-GDDP, NASA POWER, CMIP6 and SoilGrids",
    version="1.0.0",
    docs_url="/docs",
    redoc_url="/redoc",
)

templates_dir = os.path.join(os.path.dirname(__file__), "templates")

# Disable cache to work around Python 3.14 compatibility issue
import jinja2
jinja_env = jinja2.Environment(
    loader=jinja2.FileSystemLoader(templates_dir),
    auto_reload=False
)
# Use a custom cache class that handles tuple keys
class NoCache:
    def __init__(self):
        self._cache = {}
    def get(self, key):
        # Convert tuple keys to strings
        if isinstance(key, tuple):
            key = str(key)
        return self._cache.get(key)
    def __setitem__(self, key, value):
        if isinstance(key, tuple):
            key = str(key)
        self._cache[key] = value
    def __contains__(self, key):
        if isinstance(key, tuple):
            key = str(key)
        return key in self._cache
    def clear(self):
        self._cache.clear()

jinja_env.cache = NoCache()
templates = Jinja2Templates(env=jinja_env)

static_dir = os.path.join(os.path.dirname(__file__), "static")
if os.path.exists(static_dir):
    app.mount("/static", StaticFiles(directory=static_dir), name="static")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    # Browsers reject wildcard origins combined with credentials; keep
    # credentials off so the "*" origin stays valid.
    allow_credentials=False,
    allow_methods=["*"],
    allow_headers=["*"],
)

app.include_router(data.router, prefix="/api/v1/data", tags=["Data"])
app.include_router(statistics.router, prefix="/api/v1/statistics", tags=["Statistics"])
app.include_router(hazards.router, prefix="/api/v1/hazards", tags=["Hazards"])
app.include_router(compare.router, prefix="/api/v1/compare", tags=["Compare"])
app.include_router(seasons.router, prefix="/api/v1/seasons", tags=["Seasons"])
app.include_router(climatology.router, prefix="/api/v1/climatology", tags=["Climatology"])
app.include_router(compare_datasets.router, prefix="/api/v1/compare-datasets", tags=["Compare Datasets"])


@app.get("/", tags=["Root"], response_class=HTMLResponse)
async def index(request: Request):
    context = {"request": request}
    return templates.TemplateResponse(request, "index.html", context)


@app.get("/fetch", tags=["UI"], response_class=HTMLResponse)
async def fetch_data_page(request: Request):
    return templates.TemplateResponse(request, "fetch_data.html", {"request": request, "result": None})


@app.post("/fetch", tags=["UI"], response_class=HTMLResponse)
async def fetch_data_page_post(request: Request):
    form_data = await request.form()
    
    variables = form_data.getlist("variables")
    if not variables:
        variables_str = form_data.get("variables", "")
        variables = [v.strip() for v in variables_str.split(",") if v.strip()]
    if not variables:
        variables = ["precipitation"]
    
    payload = {
        "lat": float(form_data.get("lat")),
        "lon": float(form_data.get("lon")),
        "source": form_data.get("source"),
        "variables": variables,
        "date_from": form_data.get("date_from"),
        "date_to": form_data.get("date_to"),
        "model": form_data.get("model") or None,
        "scenario": form_data.get("scenario") or None,
        "format": form_data.get("format", "json")
    }
    
    from sources.utils.models import ClimateDataset, ClimateVariable, SoilVariable
    from sources.utils.settings import Settings
    from datetime import datetime
    
    try:
        source_enum = getattr(ClimateDataset, payload["source"], None)
        if not source_enum:
            result = {"status_code": 400, "status": "REQUEST_UNSUCCESSFUL", "message": f"Unknown source: {payload['source']}", "data": None}
            return templates.TemplateResponse(request, "fetch_data.html", {"request": request, "result": result})
        
        vars_list = []
        for v in payload["variables"]:
            if hasattr(ClimateVariable, v):
                vars_list.append(getattr(ClimateVariable, v))
            elif hasattr(SoilVariable, v):
                vars_list.append(getattr(SoilVariable, v))
        
        if not vars_list:
            result = {"status_code": 400, "status": "REQUEST_UNSUCCESSFUL", "message": "No valid variables", "data": None}
            return templates.TemplateResponse(request, "fetch_data.html", {"request": request, "result": result})
        
        date_from = datetime.strptime(payload["date_from"], "%Y-%m-%d").date()
        date_to = datetime.strptime(payload["date_to"], "%Y-%m-%d").date()
        
        settings = Settings.load()
        
        from source_data import SourceData
        source_data = SourceData(
            location_coord=(payload["lat"], payload["lon"]),
            variables=vars_list,
            source=source_enum,
            date_from_utc=date_from,
            date_to_utc=date_to,
            settings=settings,
            model=payload["model"],
            scenario=payload["scenario"]
        )
        
        climate_data = source_data.download()
        data_dict = climate_data.to_dict(orient="records")
        
        result = {
            "status_code": 200,
            "status": "REQUEST_SUCCESSFUL",
            "message": f"Successfully fetched {len(climate_data)} records",
            "data": {
                "records": data_dict,
                "count": len(climate_data),
                "columns": list(climate_data.columns)
            }
        }
    except Exception as e:
        result = {"status_code": 500, "status": "SERVICE_UNREACHABLE", "message": str(e), "data": None}
    
    return templates.TemplateResponse(request, "fetch_data.html", {"request": request, "result": result})


@app.get("/statistics", tags=["UI"], response_class=HTMLResponse)
async def statistics_page(request: Request):
    return templates.TemplateResponse(request, "statistics.html", {"request": request, "result": None})


@app.post("/statistics", tags=["UI"], response_class=HTMLResponse)
async def statistics_page_post(request: Request):
    form_data = await request.form()
    
    payload = {
        "lat": float(form_data.get("lat")),
        "lon": float(form_data.get("lon")),
        "date_from": form_data.get("date_from"),
        "date_to": form_data.get("date_to"),
        "source": form_data.get("source"),
        "gap_days": int(form_data.get("gap_days", 30)),
        "min_season_days": int(form_data.get("min_season_days", 30)),
        "model": form_data.get("model") or None,
        "scenario": form_data.get("scenario") or None
    }
    
    try:
        sys.path.insert(0, os.path.join(project_root, "climate_tookit"))
        from climate_tookit.climate_statistics.statistics import analyze_climate_statistics as analyze_stats
        
        # Parse years from date strings
        date_from = payload["date_from"]
        date_to = payload["date_to"]
        start_year = int(date_from.split("-")[0])
        end_year = int(date_to.split("-")[0])
        
        result = analyze_stats(
            location_coord=(payload["lat"], payload["lon"]),
            start_year=start_year,
            end_year=end_year,
            source=payload["source"],
            model=payload["model"],
            scenario=payload["scenario"]
        )
        
        if "error" in result:
            result = {"status_code": 400, "status": "REQUEST_UNSUCCESSFUL", "message": result.get("error", "Error"), "data": result}
            stats_data = None
        else:
            result = {"status_code": 200, "status": "REQUEST_SUCCESSFUL", "message": "Analysis complete", "data": result}
            stats_data = result["data"]
    except Exception as e:
        result = {"status_code": 500, "status": "SERVICE_UNREACHABLE", "message": str(e), "data": None}
        stats_data = None
    
    return templates.TemplateResponse(request, "statistics.html", {"request": request, "result": result, "statistics": stats_data})


@app.get("/seasons", tags=["UI"], response_class=HTMLResponse)
async def seasons_page(request: Request):
    return templates.TemplateResponse(request, "seasons.html", {"request": request, "result": None})


@app.post("/seasons", tags=["UI"], response_class=HTMLResponse)
async def seasons_page_post(request: Request):
    form_data = await request.form()
    
    payload = {
        "lat": float(form_data.get("lat")),
        "lon": float(form_data.get("lon")),
        "source": form_data.get("source", "auto"),
        "start_year": int(form_data.get("start_year")),
        "end_year": int(form_data.get("end_year")),
        "fixed_season": form_data.get("fixed_season") or None,
    }
    
    try:
        sys.path.insert(0, os.path.join(project_root, "climate_tookit"))
        from climate_tookit.season_analysis.seasons import fetch_and_analyze_years, fetch_and_analyze_years_fixed
        
        if payload["fixed_season"]:
            from climate_tookit.season_analysis.seasons import parse_fixed_seasons
            fixed_defs = parse_fixed_seasons(payload["fixed_season"])
            seasons_dict, annual_dict = fetch_and_analyze_years_fixed(
                payload["lat"], payload["lon"],
                fixed_seasons=fixed_defs,
                start_year=payload["start_year"],
                end_year=payload["end_year"],
                source=payload["source"],
            )
        else:
            seasons_dict, annual_dict = fetch_and_analyze_years(
                payload["lat"], payload["lon"],
                start_year=payload["start_year"],
                end_year=payload["end_year"],
                source=payload["source"],
            )
        
        result = {"status_code": 200, "status": "REQUEST_SUCCESSFUL", "message": "Season analysis complete", "data": {
            "seasons": seasons_dict,
            "annual": annual_dict,
            "location": {"lat": payload["lat"], "lon": payload["lon"]},
            "source": payload["source"],
            "mode": "fixed" if payload["fixed_season"] else "auto",
        }}
    except Exception as e:
        result = {"status_code": 500, "status": "SERVICE_UNREACHABLE", "message": str(e), "data": None}
    
    return templates.TemplateResponse(request, "seasons.html", {"request": request, "result": result})


@app.get("/climatology", tags=["UI"], response_class=HTMLResponse)
async def climatology_page(request: Request):
    return templates.TemplateResponse(request, "climatology.html", {"request": request, "result": None})


@app.post("/climatology", tags=["UI"], response_class=HTMLResponse)
async def climatology_page_post(request: Request):
    form_data = await request.form()
    
    payload = {
        "lat": float(form_data.get("lat")),
        "lon": float(form_data.get("lon")),
        "source": form_data.get("source"),
        "start_year": int(form_data.get("start_year")),
        "end_year": int(form_data.get("end_year")),
    }
    
    try:
        sys.path.insert(0, os.path.join(project_root, "climate_tookit"))
        from climate_tookit.climatology.long_term_climatology import calculate_climatology
        
        result = calculate_climatology(
            location_coord=(payload["lat"], payload["lon"]),
            start_year=payload["start_year"],
            end_year=payload["end_year"],
            source=payload["source"],
        )
        
        if "error" in result:
            result = {"status_code": 400, "status": "REQUEST_UNSUCCESSFUL", "message": result.get("error", "Error"), "data": result}
        else:
            result = {"status_code": 200, "status": "REQUEST_SUCCESSFUL", "message": "Climatology analysis complete", "data": result}
    except Exception as e:
        result = {"status_code": 500, "status": "SERVICE_UNREACHABLE", "message": str(e), "data": None}
    
    return templates.TemplateResponse(request, "climatology.html", {"request": request, "result": result})


@app.get("/hazards", tags=["UI"], response_class=HTMLResponse)
async def hazards_page(request: Request):
    return templates.TemplateResponse(request, "hazards.html", {"request": request, "result": None})


@app.post("/hazards", tags=["UI"], response_class=HTMLResponse)
async def hazards_page_post(request: Request):
    form_data = await request.form()
    
    payload = {
        "lat": float(form_data.get("lat")),
        "lon": float(form_data.get("lon")),
        "date_from": form_data.get("date_from"),
        "date_to": form_data.get("date_to"),
        "source": form_data.get("source"),
        "crop": form_data.get("crop"),
        "gap_days": int(form_data.get("gap_days", 7)),
        "fixed_season": form_data.get("fixed_season") or None,
        "season_start": form_data.get("season_start") or None,
        "season_end": form_data.get("season_end") or None,
    }

    try:
        sys.path.insert(0, os.path.join(project_root, "climate_tookit"))
        from climate_tookit.calculate_hazards.hazards import calculate_hazards

        # Pass season options as given; omitting them lets the module
        # auto-detect rather than collapsing the whole range into one season.
        result = calculate_hazards(
            crop_name=payload["crop"],
            location_coord=(payload["lat"], payload["lon"]),
            date_from=payload["date_from"],
            date_to=payload["date_to"],
            source=payload["source"],
            gap_days=payload["gap_days"],
            min_season_days=30,
            fixed_season=payload["fixed_season"],
            season_start=payload["season_start"],
            season_end=payload["season_end"],
        )
        
        if "error" in result:
            result = {"status_code": 400, "status": "REQUEST_UNSUCCESSFUL", "message": result.get("error", "Error"), "data": result}
        else:
            result = {"status_code": 200, "status": "REQUEST_SUCCESSFUL", "message": f"Hazard analysis for {payload['crop']} complete", "data": result}
    except Exception as e:
        result = {"status_code": 500, "status": "SERVICE_UNREACHABLE", "message": str(e), "data": None}
    
    return templates.TemplateResponse(request, "hazards.html", {"request": request, "result": result})


@app.get("/compare-periods", tags=["UI"], response_class=HTMLResponse)
async def compare_periods_page(request: Request):
    return templates.TemplateResponse(request, "compare_periods.html", {"request": request, "result": None})


@app.post("/compare-periods", tags=["UI"], response_class=HTMLResponse)
async def compare_periods_page_post(request: Request):
    form_data = await request.form()
    
    payload = {
        "lat": float(form_data.get("lat")),
        "lon": float(form_data.get("lon")),
        "source": form_data.get("source"),
        "period1_from": form_data.get("period1_from"),
        "period1_to": form_data.get("period1_to"),
        "period2_from": form_data.get("period2_from"),
        "period2_to": form_data.get("period2_to"),
        "model": form_data.get("model") or None,
        "scenario": form_data.get("scenario") or None
    }
    
    try:
        sys.path.insert(0, os.path.join(project_root, "climate_tookit"))
        from climate_tookit.compare_periods.periods import compare
        
        baseline_start = int(payload["period1_from"].split("-")[0])
        baseline_end = int(payload["period1_to"].split("-")[0])
        focal_year = int(payload["period2_from"].split("-")[0])
        
        result = compare(
            location=(payload["lat"], payload["lon"]),
            baseline_start=baseline_start,
            baseline_end=baseline_end,
            focal_year=focal_year,
            source=payload["source"]
        )
        
        if "error" in result:
            result = {"status_code": 400, "status": "REQUEST_UNSUCCESSFUL", "message": result.get("error", "Error"), "data": result}
            comparison = None
        else:
            result = {"status_code": 200, "status": "REQUEST_SUCCESSFUL", "message": "Comparison complete", "data": result}
            comparison = result["data"]
    except Exception as e:
        result = {"status_code": 500, "status": "SERVICE_UNREACHABLE", "message": str(e), "data": None}
        comparison = None
    
    return templates.TemplateResponse(request, "compare_periods.html", {"request": request, "result": result, "comparison": comparison})


@app.get("/compare-datasets", tags=["UI"], response_class=HTMLResponse)
async def compare_datasets_page(request: Request):
    return templates.TemplateResponse(request, "compare_datasets.html", {"request": request, "result": None})


@app.post("/compare-datasets", tags=["UI"], response_class=HTMLResponse)
async def compare_datasets_page_post(request: Request):
    form_data = await request.form()
    
    payload = {
        "lat": float(form_data.get("lat")),
        "lon": float(form_data.get("lon")),
        "date_from": form_data.get("date_from"),
        "date_to": form_data.get("date_to"),
        "sources": form_data.getlist("sources"),
        "model": form_data.get("model") or None,
        "scenario": form_data.get("scenario") or None
    }
    
    try:
        sys.path.insert(0, os.path.join(project_root, "climate_tookit"))
        from climate_tookit.compare_datasets.compare_datasets import compare_sources, print_report
        
        results = compare_sources(
            sources=payload["sources"],
            lat=payload["lat"],
            lon=payload["lon"],
            start=payload["date_from"],
            end=payload["date_to"],
            nex_model=payload["model"],
            nex_scenario=payload["scenario"]
        )
        
        stats = print_report(results)
        
        if not results:
            result = {"status_code": 400, "status": "REQUEST_UNSUCCESSFUL", "message": "No data retrieved", "data": None}
        else:
            result = {"status_code": 200, "status": "REQUEST_SUCCESSFUL", "message": "Dataset comparison complete", "data": stats}
    except Exception as e:
        result = {"status_code": 500, "status": "SERVICE_UNREACHABLE", "message": str(e), "data": None}
    
    return templates.TemplateResponse(request, "compare_datasets.html", {"request": request, "result": result})


@app.get("/api", tags=["Root"])
async def api_root():
    return ClimateResponse(
        status_code=200,
        status="REQUEST_SUCCESSFUL",
        message="Climate Toolkit API. Visit /docs for API documentation.",
        data={
            "version": "1.0.0",
            "ui": "/",
            "endpoints": {
                "data": "/api/v1/data",
                "statistics": "/api/v1/statistics",
                "hazards": "/api/v1/hazards",
                "compare": "/api/v1/compare"
            }
        }
    )


@app.get("/health", tags=["Health"])
async def health_check():
    return ClimateResponse(
        status_code=200,
        status="REQUEST_SUCCESSFUL",
        message="Service is healthy",
        data={"status": "ok"}
    )
