#!/usr/bin/env python3
"""
Script: eros_nlcd_download.py
Purpose: Demonstrates how to connect to the USGS EROS M2M API using an API token,
         search for NLCD data within a bounding box, and download the resulting file.

Requirements:
  - pip install requests
  - A valid EROS API token in your environment (EROS_API_TOKEN)

Usage:
  export EROS_API_TOKEN="your_api_key"
  python eros_nlcd_download.py --bbox -120.5 35.0 -120.0 35.5 --out_dir /path/to/downloads

Note:
  Replace 'NLCD2019_ID' with the correct dataset identifier and possibly
  'LANDSAT_ARCHIVE' references with the correct 'node' for NLCD on EROS.
"""

import os
import sys
import argparse
import requests
import json

# EROS (M2M) base URL
EROS_M2M_URL = "https://m2m.cr.usgs.gov/api/api/json/stable/"

# MOCK dataset & node definitions: you must update these for real usage
NLCD_DATASET_ID = "NLCD2019_ID"   # <-- Replace with the actual dataset ID for NLCD 2019
NLCD_NODE = "LANDSAT_ARCHIVE"     # <-- Or 'HDDS' or other node if that's where NLCD is stored
PRODUCT_TYPE = "products"         # Sometimes called "productList", but depends on the dataset

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


def search_nlcd(bbox, api_key, max_results=5):
    """
    Search the M2M API for NLCD items within a bounding box.
    bbox = (min_long, min_lat, max_long, max_lat)
    """
    minX, minY, maxX, maxY = bbox

    # The M2M 'search' request typically looks like this:
    # Reference: https://m2m.cr.usgs.gov/api/docs/#search
    # Using minimal bounding rectangle (mBR).
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
      1) Requesting a download token or direct download link
      2) Saving the file to out_dir

    The M2M flow often requires:
      - "download-options" to see what's available
      - "download-request" to stage the file
      - then retrieve the link from "download-retrieve"
    """
    # 1) Check available downloads
    download_options_payload = {
        "datasetName": NLCD_DATASET_ID,
        "node": NLCD_NODE,
        "entityIds": [scene_entity_id]
    }
    download_opts = api_post("download-options", download_options_payload, api_key)
    
    # The returned structure typically includes product info. 
    # We pick the first or relevant product. This might vary for NLCD.
    if not download_opts:
        print("[WARN] No download options returned for scene:", scene_entity_id)
        return None
    
    # Each item in download_opts can have something like 'downloadId' or 'productId'
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
    if not download_request_resp['data']:
        print("[ERROR] Could not stage download for scene:", scene_entity_id)
        return None
    
    # The result might have a 'preparingDownloads' or 'availableDownloads' list.
    # Some items may need a "check again later" loop if the file is being prepared.
    available_downloads = download_request_resp['data'].get('availableDownloads', [])
    preparing_downloads = download_request_resp['data'].get('preparingDownloads', [])
    
    # If it's immediately available
    if available_downloads:
        download_url = available_downloads[0]['url']
    else:
        print("[INFO] Download is being prepared. This script doesn't implement a wait loop.")
        print("      You may need to poll 'download-retrieve' or revisit 'download-request'.")
        if preparing_downloads:
            print("[INFO] Items in preparing:", preparing_downloads)
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
    print("[INFO] Download complete.")
    return local_filename


def main():
    parser = argparse.ArgumentParser(description="USGS EROS M2M API: Search and Download NLCD Scenes.")
    parser.add_argument("--bbox", type=float, nargs=4, required=True,
                        help="Bounding box: minLon minLat maxLon maxLat")
    parser.add_argument("--out_dir", type=str, default="downloads",
                        help="Output directory for downloaded data.")
    args = parser.parse_args()
    
    # 1) Get API key from environment
    api_key = get_api_key()
    
    # 2) Search
    search_result = search_nlcd(bbox=args.bbox, api_key=api_key)
    if 'data' not in search_result:
        print("[ERROR] Unexpected search response:", search_result)
        sys.exit(1)
    
    if 'results' not in search_result['data'] or not search_result['data']['results']:
        print("[INFO] No scenes found in the specified bounding box.")
        sys.exit(0)
    
    # 3) Iterate over found scenes, attempt to download
    print(f"[INFO] Found {len(search_result['data']['results'])} scene(s). Downloading:")
    for idx, scene in enumerate(search_result['data']['results']):
        scene_id = scene['entityId']
        scene_name = scene.get('displayId', 'UnknownNLCDScene')
        print(f"  {idx+1}. Scene ID: {scene_id}  Name: {scene_name}")
        
        # Attempt download
        download_scene(scene_id, api_key, args.out_dir)


if __name__ == "__main__":
    main()
