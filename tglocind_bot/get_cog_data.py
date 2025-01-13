import rasterio
from rasterio.io import MemoryFile
from rasterio.warp import transform
import boto3

def get_value_from_cog(s3_url, lon, lat):
    try:
        with rasterio.Env():
            with rasterio.open(s3_url) as dataset:
                # Transform lon, lat to the dataset's CRS
                dataset_crs = dataset.crs
                print(f"dataset_crs is {dataset_crs}")
                coords = transform(
                    "EPSG:4326", str(dataset_crs), [lon], [lat]
                )
                x, y = coords[0][0], coords[1][0]
                
                for val in dataset.sample([(x, y)]):
                    return val[0]
    except Exception as e:
        return f"Error: {e}"

# Example Usage
if __name__ == "__main__":
    
    # gdal_translate -r bilinear -of COG -co "BIGTIFF=YES" gebco_2022_n90.0_s0.0_w0.0_e90.0.tif gebco_2022_cog.tif
    s3_url = "s3://48ee2bf5caf7247cf6e397b3cf67e858/public/cog.tif"
    # lon, lat = 12.4924, 41.8902  # Rome, Italy
    lon, lat = 19.935640, 49.248643 # Zakopane
    
    altitude_value = get_value_from_cog(s3_url, lon, lat)
    print(f"Altitude value at ({lon}, {lat}): {altitude_value} meters")
