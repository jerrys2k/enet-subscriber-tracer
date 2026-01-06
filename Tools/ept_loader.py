#!/usr/bin/env python3
"""
EPT (Engineering Planning Tool) Loader
- Load LTE/NR cell data from EPT spreadsheet
- Pickle caching for instant loads
- Lookup cell details by eNodeB ID and Cell ID
"""

import pandas as pd
import logging
import os
import glob
import time

logger = logging.getLogger(__name__)

EPT_DIR = "data"
PICKLE_LTE = "data/ept_lte.pkl"
PICKLE_NR = "data/ept_nr.pkl"
PICKLE_META = "data/ept_meta.pkl"

# Cache for EPT data
_LTE_CACHE = None
_NR_CACHE = None
_EPT_META = None


def get_latest_ept():
    """Find the latest EPT xlsx file in data directory"""
    patterns = [
        os.path.join(EPT_DIR, "*EPT*.xlsx"),
        os.path.join(EPT_DIR, "*ept*.xlsx"),
    ]
    files = []
    for pattern in patterns:
        files.extend(glob.glob(pattern))
    if files:
        return max(files, key=os.path.getmtime)
    return None


def convert_ept_to_pickle(xlsx_path=None):
    """Convert EPT xlsx to pickle for fast loading"""
    global _LTE_CACHE, _NR_CACHE, _EPT_META
    
    if xlsx_path is None:
        xlsx_path = get_latest_ept()
    
    if not xlsx_path or not os.path.exists(xlsx_path):
        logger.error(f"EPT file not found: {xlsx_path}")
        return False
    
    try:
        start = time.time()
        logger.info(f"Converting {os.path.basename(xlsx_path)} to pickle...")
        
        # Load LTE sheet
        lte_df = pd.read_excel(xlsx_path, sheet_name="LTE")
        lte_df.to_pickle(PICKLE_LTE)
        logger.info(f"  LTE: {len(lte_df)} cells")
        
        # Load NR sheet
        try:
            nr_df = pd.read_excel(xlsx_path, sheet_name="NR")
            nr_df.to_pickle(PICKLE_NR)
            logger.info(f"  NR: {len(nr_df)} cells")
        except Exception as e:
            logger.warning(f"  NR sheet not found: {e}")
            nr_df = pd.DataFrame()
            nr_df.to_pickle(PICKLE_NR)
        
        # Save metadata
        meta = {
            "source_file": os.path.basename(xlsx_path),
            "converted_at": pd.Timestamp.now().isoformat(),
            "lte_cells": len(lte_df),
            "nr_cells": len(nr_df),
            "lte_enodebs": lte_df['eNodeB ID'].nunique() if len(lte_df) > 0 else 0,
        }
        pd.to_pickle(meta, PICKLE_META)
        
        elapsed = time.time() - start
        logger.info(f"EPT converted in {elapsed:.1f}s")
        
        # Clear cache to force reload
        _LTE_CACHE = None
        _NR_CACHE = None
        _EPT_META = None
        
        return True
        
    except Exception as e:
        logger.error(f"Failed to convert EPT: {e}")
        return False


def load_ept(force_reload=False):
    """Load EPT data from pickle (fast) or xlsx (slow fallback)"""
    global _LTE_CACHE, _NR_CACHE, _EPT_META
    
    if _LTE_CACHE is not None and not force_reload:
        return _LTE_CACHE, _NR_CACHE
    
    # Try pickle first (instant load)
    if os.path.exists(PICKLE_LTE) and os.path.exists(PICKLE_NR):
        try:
            start = time.time()
            _LTE_CACHE = pd.read_pickle(PICKLE_LTE)
            _NR_CACHE = pd.read_pickle(PICKLE_NR)
            if os.path.exists(PICKLE_META):
                _EPT_META = pd.read_pickle(PICKLE_META)
            elapsed = time.time() - start
            logger.info(f"EPT loaded from pickle in {elapsed:.3f}s ({len(_LTE_CACHE)} LTE, {len(_NR_CACHE)} NR)")
            return _LTE_CACHE, _NR_CACHE
        except Exception as e:
            logger.warning(f"Pickle load failed, falling back to xlsx: {e}")
    
    # Fallback: convert xlsx to pickle
    xlsx_file = get_latest_ept()
    if xlsx_file:
        logger.info("No pickle cache, converting EPT xlsx...")
        convert_ept_to_pickle(xlsx_file)
        return load_ept()
    
    logger.error("No EPT file found")
    return None, None


def get_cell_details(enodeb_id, cell_id):
    """
    Get full cell details from EPT
    """
    lte_df, nr_df = load_ept()
    
    if lte_df is None:
        return None
    
    try:
        enodeb_id = int(enodeb_id)
        cell_id = int(cell_id)
    except (ValueError, TypeError):
        return None
    
    # Search LTE first
    match = lte_df[(lte_df['eNodeB ID'] == enodeb_id) & (lte_df['Cell ID'] == cell_id)]
    
    if len(match) > 0:
        row = match.iloc[0]
        return {
            "source": "EPT_LTE",
            "enodeb_id": int(row['eNodeB ID']),
            "enodeb_name": str(row['eNodeB Name']),
            "cell_id": int(row['Cell ID']),
            "cell_name": str(row['Cell Name']),
            "latitude": float(row['Latitude']) if pd.notna(row['Latitude']) else 0.0,
            "longitude": float(row['Longitude']) if pd.notna(row['Longitude']) else 0.0,
            "azimuth": int(row['Azimuth']) if pd.notna(row['Azimuth']) else None,
            "height": float(row['Height']) if pd.notna(row['Height']) else None,
            "technology": str(row.get('Technology', 'LTE')),
            "tac": int(row['TAC']) if pd.notna(row['TAC']) else None,
            "pci": int(row['PCI']) if pd.notna(row.get('PCI')) else None,
            "bandwidth": str(row.get('Bandwidth UL & DL (MHz)', '')),
            "frequency": str(row.get('DL Frequency', '')),
            "antenna_model": str(row.get('Antenna Model', '')),
            "sector_id": int(row['Sector ID']) if pd.notna(row.get('Sector ID')) else None,
        }
    
    # Try NR (5G) if not found in LTE
    if nr_df is not None and len(nr_df) > 0:
        match = nr_df[(nr_df['gNodeB ID'] == enodeb_id) & (nr_df['Cell ID'] == cell_id)]
        
        if len(match) > 0:
            row = match.iloc[0]
            return {
                "source": "EPT_NR",
                "enodeb_id": int(row['gNodeB ID']),
                "enodeb_name": str(row['gNodeB Name']),
                "cell_id": int(row['Cell ID']),
                "cell_name": str(row['Nr Cell Name']),
                "latitude": float(row['Latitude']) if pd.notna(row['Latitude']) else 0.0,
                "longitude": float(row['Longitude']) if pd.notna(row['Longitude']) else 0.0,
                "azimuth": int(row['Azimuth']) if pd.notna(row['Azimuth']) else None,
                "height": float(row['Height']) if pd.notna(row['Height']) else None,
                "technology": "5G NR",
                "tac": int(row['TAC']) if pd.notna(row['TAC']) else None,
                "pci": int(row['PCI']) if pd.notna(row.get('PCI')) else None,
            }
    
    # Not found - return just eNodeB info if available
    enodeb_match = lte_df[lte_df['eNodeB ID'] == enodeb_id]
    if len(enodeb_match) > 0:
        row = enodeb_match.iloc[0]
        return {
            "source": "EPT_ENODEB_ONLY",
            "enodeb_id": int(row['eNodeB ID']),
            "enodeb_name": str(row['eNodeB Name']),
            "cell_id": cell_id,
            "cell_name": f"Cell {cell_id} (not in EPT)",
            "latitude": float(row['Latitude']) if pd.notna(row['Latitude']) else 0.0,
            "longitude": float(row['Longitude']) if pd.notna(row['Longitude']) else 0.0,
            "azimuth": None,
            "height": float(row['Height']) if pd.notna(row['Height']) else None,
            "technology": "LTE",
            "tac": int(row['TAC']) if pd.notna(row['TAC']) else None,
        }
    
    return None


def get_azimuth_direction(azimuth):
    """Convert azimuth degrees to compass direction"""
    if azimuth is None:
        return "Unknown"
    
    directions = [
        (0, 22.5, "N"), (22.5, 67.5, "NE"), (67.5, 112.5, "E"),
        (112.5, 157.5, "SE"), (157.5, 202.5, "S"), (202.5, 247.5, "SW"),
        (247.5, 292.5, "W"), (292.5, 337.5, "NW"), (337.5, 360, "N")
    ]
    
    for low, high, direction in directions:
        if low <= azimuth < high:
            return direction
    return "N"


def get_ept_info():
    """Get info about loaded EPT file"""
    global _EPT_META, _LTE_CACHE, _NR_CACHE
    
    load_ept()
    
    if _EPT_META:
        return _EPT_META
    
    # Generate from cache if meta missing
    if _LTE_CACHE is not None:
        return {
            "source_file": "Unknown",
            "lte_cells": len(_LTE_CACHE),
            "nr_cells": len(_NR_CACHE) if _NR_CACHE is not None else 0,
            "lte_enodebs": _LTE_CACHE['eNodeB ID'].nunique(),
        }
    
    return {"source_file": None, "lte_cells": 0, "nr_cells": 0, "lte_enodebs": 0}


def delete_old_ept():
    """Delete old EPT xlsx files (keep only latest)"""
    patterns = [
        os.path.join(EPT_DIR, "*EPT*.xlsx"),
        os.path.join(EPT_DIR, "*ept*.xlsx"),
    ]
    files = []
    for pattern in patterns:
        files.extend(glob.glob(pattern))
    
    if len(files) <= 1:
        return 0
    
    # Sort by modification time, keep newest
    files.sort(key=os.path.getmtime, reverse=True)
    deleted = 0
    for f in files[1:]:
        try:
            os.remove(f)
            logger.info(f"Deleted old EPT: {os.path.basename(f)}")
            deleted += 1
        except Exception as e:
            logger.error(f"Failed to delete {f}: {e}")
    
    return deleted


def clear_cache():
    """Clear pickle cache to force re-conversion"""
    global _LTE_CACHE, _NR_CACHE, _EPT_META
    
    for f in [PICKLE_LTE, PICKLE_NR, PICKLE_META]:
        if os.path.exists(f):
            os.remove(f)
    
    _LTE_CACHE = None
    _NR_CACHE = None
    _EPT_META = None
    logger.info("EPT cache cleared")


# Test
if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format='%(message)s')
    
    # Force convert to measure time
    print("=== Converting EPT to Pickle ===")
    clear_cache()
    start = time.time()
    convert_ept_to_pickle()
    print(f"Conversion time: {time.time() - start:.1f}s")
    
    # Test load speed
    print("\n=== Loading from Pickle ===")
    # Clear in-memory cache
    _LTE_CACHE = None
    _NR_CACHE = None
    start = time.time()
    load_ept()
    print(f"Load time: {time.time() - start:.4f}s")
    
    # Test lookup
    print("\n=== Cell Lookup Test ===")
    start = time.time()
    result = get_cell_details(4555, 213)
    print(f"Lookup time: {time.time() - start:.4f}s")
    
    if result:
        print(f"\neNodeB: {result['enodeb_name']} ({result['enodeb_id']})")
        print(f"Cell: {result['cell_name']} ({result['cell_id']})")
        print(f"Location: {result['latitude']}, {result['longitude']}")
        print(f"Azimuth: {result['azimuth']}° ({get_azimuth_direction(result['azimuth'])})")
        print(f"Height: {result['height']}m")
        print(f"Technology: {result['technology']}")
    
    # EPT info
    print(f"\n=== EPT Info ===")
    info = get_ept_info()
    for k, v in info.items():
        print(f"  {k}: {v}")
