import os
from osgeo import gdal
import rasterio
from utils import get_all_tif_files, make_folder

class CropImage:
    def __init__(self, path_tif_file: str, size_crop_terrain: int, size_crop_middle: int, size_crop_pixle: int, path_to_save: str) -> None:
        
        self.root_dir = path_to_save
        self.path_tif_file = path_tif_file
        
        self.size_crop_terrain = size_crop_terrain
        self.size_crop_middle = size_crop_middle
        self.size_crop_pixle = size_crop_pixle
        
        make_folder(self.root_dir)
        print("✅ Created root folder")
        
        self.terrain_dir = os.path.join(self.root_dir, f"{self.size_crop_terrain}_{self.size_crop_terrain}")       
        make_folder(self.terrain_dir)
        print("✅ Created terrain crop folder")
       
        self.mid_dir = os.path.join(self.root_dir, f"{self.size_crop_middle}_{self.size_crop_middle}")  
        make_folder(self.mid_dir)
        print("✅ Created middle crop folder")
            
        self.pixle_dir = os.path.join(self.root_dir, f"{self.size_crop_pixle}_{self.size_crop_pixle}")  
        make_folder(self.pixle_dir)
        print("✅ Created pixel crop folder")
    

    
    def _crop_tiff(self, 
                   input_filename: str, 
                   output_file: str, 
                   x_off: int, 
                   y_off: int, 
                   x_size: int, 
                   y_size: int) -> bool:
        """
        Crop the TIFF file using gdal_translate.
        """
        com_string = f"gdal_translate -of GTIFF -srcwin {x_off} {y_off} {x_size} {y_size} {input_filename} {output_file}.tif"
        result = os.system(com_string)
        return result == 0
    
    def _crop_image(self, 
                    input_filename: str, 
                    output_filename: str, 
                    out_path: str, 
                    crop_size: int, 
                    xsize: int, 
                    ysize: int) -> None:
        """
        Crop the input TIFF file into tiles of specified size.
        """
        counter = 1
        for i in range(0, xsize, crop_size):
            for j in range(0, ysize, crop_size):
                out_file = os.path.join(out_path, f"{output_filename}{counter}")
                if not self._crop_tiff(input_filename, out_file, i, j, crop_size, crop_size):
                    print(f"Failed to create tile {out_file}")
                counter += 1
                
    def _handle_middle(self) -> None:
        all_tif_files = get_all_tif_files(self.terrain_dir)
        
        self.path_save_30_30 = self.mid_dir
        make_folder(self.path_save_30_30)
        
        
        for path, name in all_tif_files:

            id = name.split(".tif")[0]  
            xsize, ysize = self._get_size_tif(path) 
            self._crop_image(path, f"{id}_", self.path_save_30_30, self.size_crop_middle, xsize, ysize)

    def _handle_pixle(self) -> None:
        all_tif_files = get_all_tif_files(self.path_save_30_30)
        print(len(all_tif_files))
        
        self.path_save_10_10 = self.pixle_dir
        make_folder(self.path_save_10_10)
        
        
        for path, name in all_tif_files:

            id = name.split(".tif")[0]  
            xsize, ysize = self._get_size_tif(path) 
            self._crop_image(path, f"{id}_", self.path_save_10_10, self.size_crop_pixle, xsize, ysize)

    def _get_size_tif(self, path: str) -> tuple:
        ds = gdal.Open(path)
        band = ds.GetRasterBand(1)
        xsize = band.XSize
        ysize = band.YSize
        return xsize, ysize
    

        
    def pipeline_crop(self) -> None:
        xsize, ysize = self._get_size_tif(self.path_tif_file)
        self._crop_image(self.path_tif_file, "", self.terrain_dir, self.size_crop_terrain, xsize, ysize)
        self._handle_middle()
        self._handle_pixle()
