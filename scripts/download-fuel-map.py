#!/usr/bin/env python3
"""
Script: eros_nlcd_download.py
Purpose: Demonstrates how to connect to the USGS EROS M2M API using an API token,
         parse a bounding box from a GeoJSON file, search for NLCD data within that
         bounding box, and download the resulting file.

Requirements:
  - pip install requests shapely
  - A valid EROS API token in your environment (EROS_API_TOKEN)

Usage:
  export EROS_API_TOKEN="your_api_key"
  python eros_nlcd_download.py --geojson region.geojson --out_dir /path/to/downloads

Note:
  - Replace 'NLCD2019_ID' with the correct dataset ID for NLCD in M2M.
  - Replace 'LANDSAT_ARCHIVE' (or 'HDDS', 'DP', etc.) with the correct node for NLCD.
  - If the dataset is distributed as one giant mosaic, you may get only a single scene.
"""

import os
import sys
import argparse
import requests
import json

from shapely.geometry import shape, MultiPolygon, Polygon
from shapely.ops import unary_union

# EROS (M2M) base URL
EROS_M2M_URL = "https://m2m.cr.usgs.gov/api/api/json/stable/"

# MOCK dataset & node definitions: you must update these for real usage
NLCD_DATASET_ID = "NLCD2019_ID"   # <-- Replace with the actual dataset ID for NLCD 2019
NLCD_NODE = "LANDSAT_ARCHIVE"     # <-- Or 'HDDS' or other node if that's where NLCD is stored

def get_api_key():
    """
    Reads the EROS_API_TOKEN environment variable.
    """
    token = os.environ.get('EROS_API_TOKEN', None)
    if not token:
        print("Error: EROS_API_TOKEN environment variable not set.")
        sys.exit(1)
    return token

def api_post(endpoint, payload, api_key):
    """
    Helper function to perform a POST request to the EROS M2M API,
    with JSON headers and the API key.
    """
    url = EROS_M2M_URL + endpoint
    headers = {
        'Content-Type': 'application/json',
        'X-Auth-Token': api_key
    }
    response = requests.post(url, data=json.dumps(payload), headers=headers, timeout=60)
    response.raise_for_status()
    return response.json()

def parse_geojson_bbox(geojson_file):
    """
    Parses a GeoJSON file, extracts all Polygon/MultiPolygon features,
    and returns the bounding box of their union in (min_lon, min_lat, max_lon, max_lat) format.
    """
    with open(geojson_file, 'r') as f:
        data = json.load(f)

    # Depending on the GeoJSON structure, we need to collect polygons from:
    # - FeatureCollection
    # - Single Feature
    # - Or a direct geometry (less common for standard GeoJSON)
    
    geometries = []
    
    if 'type' not in data:
        raise ValueError("Invalid GeoJSON: missing 'type' field.")
    
    if data['type'] == 'FeatureCollection':
        for feature in data['features']:
            geom = shape(feature['geometry'])
            geometries.append(geom)
    elif data['type'] == 'Feature':
        geom = shape(data['geometry'])
        geometries.append(geom)
    else:
        # Could be a direct geometry object
        geom = shape(data)
        geometries.append(geom)
    
    # Union all polygons
    if len(geometries) == 1:
        union_geom = geometries[0]
    else:
        union_geom = unary_union(geometries)

    if not union_geom.is_valid:
        union_geom = union_geom.buffer(0)  # attempt to fix geometry if needed

    # If the result is a polygon (or multi), we can get its bounds
    if isinstance(union_geom, (Polygon, MultiPolygon)):
        minx, miny, maxx, maxy = union_geom.bounds
        return (minx, miny, maxx, maxy)
    else:
        raise ValueError("GeoJSON does not contain valid polygon geometry.")

def search_nlcd(bbox, api_key, max_results=5):
    """
    Search the M2M API for NLCD items within a bounding box.
    bbox = (min_lon, min_lat, max_lon, max_lat)
    """
    minX, minY, maxX, maxY = bbox

    # The M2M 'search' request. 
    # Reference: https://m2m.cr.usgs.gov/api/docs/#search
    search_payload = {
        "datasetName": NLCD_DATASET_ID,
        "node": NLCD_NODE,
        "maxResults": max_results,
        "sortOrder": "ASC",
        "startingNumber": 1,
        "spatialFilter": {
            "filterType": "mBR",
            "lowerLeft": {
                "longitude": minX,
                "latitude": minY
            },
            "upperRight": {
                "longitude": maxX,
                "latitude": maxY
            }
        }
    }

    print(f"\n[INFO] Searching for dataset {NLCD_DATASET_ID} in bbox: {bbox}")
    search_result = api_post("scene-search", search_payload, api_key)
    return search_result

def download_scene(scene_entity_id, api_key, out_dir):
    """
    Download a scene by:
      1) Requesting available download options
      2) Staging the download
      3) Retrieving the direct link (if immediately available)
      4) Saving to out_dir
    """
    # 1) Check available downloads
    download_options_payload = {
        "datasetName": NLCD_DATASET_ID,
        "node": NLCD_NODE,
        "entityIds": [scene_entity_id]
    }
    download_opts = api_post("download-options", download_options_payload, api_key)
    
    if not download_opts or 'data' not in download_opts:
        print("[WARN] No download options returned for scene:", scene_entity_id)
        return None
    
    # If multiple items are returned, pick the first available
    product_id = None
    for item in download_opts['data']:
        if item.get('available'):
            product_id = item['id']  # or 'downloadId'
            break
    
    if not product_id:
        print("[WARN] No available products for scene:", scene_entity_id)
        return None
    
    # 2) Stage the download
    download_request_payload = {
        "downloads": [
            {
                "datasetName": NLCD_DATASET_ID,
                "entityId": scene_entity_id,
                "productId": product_id,
                "node": NLCD_NODE
            }
        ]
    }
    download_request_resp = api_post("download-request", download_request_payload, api_key)
    if not download_request_resp.get('data'):
        print("[ERROR] Could not stage download for scene:", scene_entity_id)
        return None
    
    available_downloads = download_request_resp['data'].get('availableDownloads', [])
    preparing_downloads = download_request_resp['data'].get('preparingDownloads', [])
    
    # If it's immediately available
    if available_downloads:
        download_url = available_downloads[0]['url']
    else:
        print("[INFO] Download is being prepared; no immediate link available.")
        if preparing_downloads:
            print("[INFO] Scenes in 'preparing' status:", preparing_downloads)
        return None
    
    # 3) Download the file
    os.makedirs(out_dir, exist_ok=True)
    local_filename = os.path.join(out_dir, f"{scene_entity_id}.tif")
    
    print(f"[INFO] Downloading scene to: {local_filename}")
    with requests.get(download_url, stream=True) as r:
        r.raise_for_status()
        with open(local_filename, 'wb') as f:
            for chunk in r.iter_content(chunk_size=8192):
                f.write(chunk)
    
    print("[INFO] Download complete:", local_filename)
    return local_filename

def main():
    parser = argparse.ArgumentParser(description="USGS EROS M2M API: Search and Download NLCD Scenes using a GeoJSON bounding box.")
    parser.add_argument("--geojson", type=str, required=True,
                        help="Path to a GeoJSON file containing polygon(s).")
    parser.add_argument("--out_dir", type=str, default="downloads",
                        help="Output directory for downloaded data.")
    args = parser.parse_args()
    
    # 1) Get API key from environment
    api_key = get_api_key()
    
    # 2) Parse bounding box from GeoJSON
    bbox = parse_geojson_bbox(args.geojson)
    print(f"[INFO] Parsed bounding box from GeoJSON: {bbox}")
    
    # 3) Search
    search_result = search_nlcd(bbox=bbox, api_key=api_key, max_results=10)
    if 'data' not in search_result:
        print("[ERROR] Unexpected search response:", search_result)
        sys.exit(1)
    
    if 'results' not in search_result['data'] or not search_result['data']['results']:
        print("[INFO] No scenes found in the specified region.")
        sys.exit(0)
    
    scenes = search_result['data']['results']
    print(f"[INFO] Found {len(scenes)} scene(s). Downloading up to {len(scenes)} scene(s).")
    
    # 4) Download each scene
    for idx, scene in enumerate(scenes):
        scene_id = scene['entityId']
        scene_name = scene.get('displayId', 'UnknownNLCDScene')
        print(f"  {idx+1}. Scene ID: {scene_id}  Name: {scene_name}")
        
        download_scene(scene_id, api_key, args.out_dir)

if __name__ == "__main__":
    main()
