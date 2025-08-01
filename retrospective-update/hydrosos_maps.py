import os
import subprocess
import warnings

os.environ["QT_QPA_PLATFORM"] = "offscreen"  # Do before importing QGIS

import processing
import numpy as np
import pandas as pd
import xarray as xr
from qgis.core import (
    QgsSymbol,
    QgsProject,
    QgsApplication,
    QgsVectorLayer,
    QgsRendererCategory,
    QgsCategorizedSymbolRenderer
)
from qgis.PyQt.QtGui import QColor
from cloud_logger import CloudLog

HYDROBASINS_FILE = os.getenv('HYDROBASINS_COMBINED')
HYBASID_TO_LINKNO_CSV = os.getenv('HYBASID_TO_LINKNO_CSV')
UNIQUE_HYBASIDS_CSV = os.getenv('UNIQUE_HYBASIDS_CSV')
DUPLICATED_HYBASIDS_CSV = os.getenv('DUPLICATED_HYBASIDS_CSV')
FLOW_CUTOFF_NC = os.getenv('FLOW_CUTOFF_NC')
FLOW_CUTOFF_NC_BY_HYBASID = os.getenv('FLOW_CUTOFF_NC_BY_HYBASID')
S3_HYDROSOS_DIR = os.getenv('S3_HYDROSOS_DIR')


def classify_flow(flow_value, cutoffs):
    if np.isnan(flow_value):
        return pd.NA
    for i, cutoff in enumerate(cutoffs):
        if flow_value <= cutoff:
            return i + 1  # 1-based class
    return 5


def update_hydrosos_maps(date_range: pd.DatetimeIndex,
                         s3_monthly_timesteps_zarr: str,
                         hydro_sos_dir: str,
                         credentials: str,
                         CL: CloudLog) -> None:
    """
    Please see `https://github.com/geoglows/hydrosos_maps/tree/main`
    """
    # Read the combined hydrobasins shapefile
    combined_gdf = gpd.read_file(HYDROBASINS_FILE)

    # Read river IDs
    river_ids = pd.read_csv(UNIQUE_HYBASIDS_CSV)["LINKNO"].dropna().unique()

    # Open GEOGloWS retrospective dataset
    monthly_ds = xr.open_zarr(s3_monthly_timesteps_zarr)

    # Filter dataset to only the matched river IDs
    filtered_monthly_ds = monthly_ds.sel(river_id=xr.DataArray(river_ids, dims="river_id"))

    # Open flow threshold dataset
    flow_thresh_ds = xr.open_dataset(FLOW_CUTOFF_NC)

    # Read river ID to HYBAS_ID mapping
    mapping_df = pd.read_csv(DUPLICATED_HYBASIDS_CSV)
    mapping_df = mapping_df.dropna(subset=['LINKNO', 'HYBAS_ID'])

    # Group LINKNOs by HYBAS_ID
    hybas_groups = mapping_df.groupby('HYBAS_ID')['LINKNO'].apply(list)

    # Open flow threshold dataset
    flow_thresh_by_hydrobas_ds = xr.open_dataset(FLOW_CUTOFF_NC_BY_HYBASID)

    # Color map
    class_color_map = {
        1.0: '#cd233f',  # red
        2.0: '#ffa885',  # peach
        3.0: '#e7e2bc',  # light yellow
        4.0: '#8eceee',  # light blue
        5.0: '#2c7dcd',  # medium blue
    }

    # QGIS setup
    QgsApplication.setPrefixPath(os.environ["CONDA_PREFIX"], True)  # for avoiding "Application path not initialized"

    app = QgsApplication([], False)
    app.initQgis()
    # Append the path where processing plugin can be found
    sys.path.append('/home/ubuntu/miniforge3/envs/update/share/qgis/python/plugins')
    processing.core.Processing.Processing.initialize()

    # Load the matched basins file
    matched_basins = pd.read_csv(HYBASID_TO_LINKNO_CSV)

    for year in date_range.strftime('%Y').tolist():
        for month in date_range.strftime('%m').tolist():
            # Compute flow classification CSV for this month/year
            date_str = f"{year}-{month}"
            year = int(year)
            month = int(month)
            CL.ping('RUNNING', f'Making-map-tiles-for-{date_str}')

            # Compute mean flow
            monthly_flow = (
                filtered_monthly_ds
                .sel(time=date_str)
                ["Q"]
                .mean(dim="time", skipna=True)
                .to_pandas()
            )

            # Get cutoffs for the month (1-based indexing for months in xarray)
            flow_cutoffs = flow_thresh_ds.sel(month=month)

            # Classify flows
            results = []
            for river_id in river_ids:
                try:
                    flow_value = monthly_flow.loc[river_id]
                    cutoff_vals = flow_cutoffs.sel(river_id=river_id)["flow_cutoff"].values
                    category = classify_flow(flow_value, cutoff_vals)
                    results.append({
                        'year': year,
                        'month': month,
                        'river_id': river_id,
                        'flow': flow_value,
                        'class': category
                    })
                except KeyError:
                    results.append({
                        'year': year,
                        'month': month,
                        'river_id': river_id,
                        'flow': pd.NA,
                        'class': pd.NA
                    })

            # Convert to DataFrame
            flow_df = pd.DataFrame(results)

            # Compute mean flow for the month
            flow_data = (
                monthly_ds
                .sel(time=date_str)
                ["Q"]
                .mean(dim="time", skipna=True)
            )

            # Compute duplicates
            results = []
            for hybas_id, linknos in hybas_groups.items():
                try:
                    linknos = [int(l) for l in linknos if pd.notna(l)]
                    flow_values = flow_data.sel(river_id=linknos).sum(dim="river_id", skipna=True).values.item()
                    cutoff_vals = flow_thresh_by_hydrobas_ds.sel(month=month, hybas_id=hybas_id)["flow_cutoff"].values
                    category = classify_flow(flow_values, cutoff_vals)

                    results.append({
                        'year': year,
                        'month': month,
                        'hybas_id': hybas_id,
                        'flow': flow_values,
                        'class': category
                    })
                except Exception as e:
                    results.append({
                        'year': year,
                        'month': month,
                        'hybas_id': hybas_id,
                        'flow': pd.NA,
                        'class': pd.NA
                    })

            flow_df2 = pd.DataFrame(results)

            # Merge flow classification with matched basins using river_id → LINKNO
            merged_df = pd.merge(flow_df, matched_basins, left_on="river_id", right_on="LINKNO", how="left")

            # Standardize the HYBAS ID column
            flow_df2 = flow_df2.rename(columns={"hybas_id": "HYBAS_ID"})
            merged_df = merged_df.drop(columns=["river_id"])

            # Concatenate the two DataFrames
            with warnings.catch_warnings():
                # TODO: pandas has a FutureWarning for concatenating DataFrames with Null entries
                warnings.filterwarnings("ignore", category=FutureWarning)
                combined_df = pd.concat([flow_df2, merged_df], ignore_index=True)

            # Merge with shapefile using HYBAS_ID (keep all hydrobasins)
            final_gdf = combined_gdf.merge(combined_df, on="HYBAS_ID", how="left")

            # Keep only selected columns
            final_gdf = final_gdf[["HYBAS_ID", "LINKNO", "flow", "class", "geometry"]]
            final_gdf['LINKNO'] = final_gdf['LINKNO'].astype('Int64')

            layer = QgsVectorLayer(final_gdf.to_json(to_wgs84=True), "", "ogr")
            if not layer.isValid():
                raise ValueError(f"Layer is not valid")

            # Apply categorized renderer
            categories = []
            for class_value, hex_color in class_color_map.items():
                symbol = QgsSymbol.defaultSymbol(layer.geometryType())
                symbol.setColor(QColor(hex_color))
                label = str(int(class_value))
                category = QgsRendererCategory(class_value, symbol, label)
                categories.append(category)

            renderer = QgsCategorizedSymbolRenderer('class', categories)
            layer.setRenderer(renderer)
            layer.triggerRepaint()

            QgsProject.instance().addMapLayer(layer)

            # Create individual output directory
            output_dir = os.path.join(hydro_sos_dir, "maps", f'year={year}', f'month={str(month).zfill(2)}')
            os.makedirs(output_dir, exist_ok=True)

            extent = layer.extent()
            crs = layer.crs()
            params = {
                'EXTENT': f"{extent.xMinimum()},{extent.xMaximum()},{extent.yMinimum()},{extent.yMaximum()}[{crs.authid()}]",
                'ZOOM_MIN': 0,
                'ZOOM_MAX': 6,
                'DPI': 96,
                'BACKGROUND_COLOR': QColor(0, 0, 0, 0),
                'ANTIALIAS': True,
                'TILE_FORMAT': 0,  # PNG
                'METATILESIZE': 4,
                'TILE_WIDTH': 256,
                'TILE_HEIGHT': 256,
                'TMS_CONVENTION': False,
                'HTML_TITLE': '',
                'HTML_ATTRIBUTION': '',
                'HTML_OSM': False,
                'OUTPUT_DIRECTORY': output_dir,
                'OUTPUT_HTML': 'TEMPORARY_OUTPUT'
            }

            # Create maptiles
            processing.run("native:tilesxyzdirectory", params)

            # Optionally remove the layer from project to keep things clean
            QgsProject.instance().removeMapLayer(layer)

            # Remove small files
            for dirpath, _, filenames in os.walk(output_dir):
                for filename in filenames:
                    if filename.lower().endswith('.png'):
                        file_path = os.path.join(dirpath, filename)
                        if os.path.getsize(file_path) <= 360:
                            os.remove(file_path)

            # Use s5cmd to sync the output directory to S3
            s3_path = f'{S3_HYDROSOS_DIR}/year={year}/month={str(month).zfill(2)}/'
            result = subprocess.run(
                f's5cmd '
                f'--credentials-file {credentials} --profile odp '
                f'sync '
                f'{output_dir} '
                f'{s3_path}',
                shell=True, capture_output=True, text=True,
            )
            if not result.returncode == 0:
                CL.ping('FAIL', f"Syncing-hydrosos-maps-to-S3-failed")
                exit()

            # Now remove the output directory
            shutil.rmtree(output_dir)

    app.exitQgis()
