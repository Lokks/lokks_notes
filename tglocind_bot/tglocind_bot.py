from pyproj import Transformer
import h3
from pluscodes import encode 

def process_location(input_string):
    try:
        # Parse the input string
        lon, lat = map(float, input_string.split(","))
        
        # Transform to SRID 3857 using pyproj
        transformer = Transformer.from_crs("EPSG:4326", "EPSG:3857", always_xy=True)
        x, y = transformer.transform(lon, lat)
        
        # Get H3 index at level 11
        h3_index = h3.latlng_to_cell(lat, lon, 11)
        
        # Get Plus Code
        plus_code1 = encode(lat, lon)

        return {
            "srid_3857": (x, y),
            "h3_index_level_11": h3_index,
            "plus_code": plus_code1
        }
    except Exception as e:
        return {"error": str(e)}

# Example usage
if __name__ == "__main__":
    input_string = input("Enter longitude and latitude (comma-separated, e.g., 12.34,56.78): ")
    result = process_location(input_string)
    if "error" in result:
        print(f"Error: {result['error']}")
    else:
        print(f"SRID 3857 Coordinates: {result['srid_3857']}")
        print(f"H3 Index Level 11: {result['h3_index_level_11']}")
        print(f"Plus Code1: {result['plus_code']}")

