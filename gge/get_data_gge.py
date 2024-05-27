import ee
import geemap
import geopandas as gpd
import pandas as pd
import json
import time
import os


ee.Authenticate()
ee.Initialize(project='ee-parvvaresh')


class EarthEngineData:
    
    def __init__(self,
               aoi : json) -> None:
        self.aoi = aoi

    """
        In this section, we deal with the processing of satellites

    """
    def load_sentinel1(self,
                     start_date : str,
                     end_date : str) -> None:
        """

            The _load_sentinel1 method is a class method that loads Sentinel-1 imagery data from the Google Earth Engine, filtered by a specified date range and area of interest (AOI). The method performs the following tasks:

            Initializes an image collection of Sentinel-1 GRD images.
            Filters this collection to include images within a specified date range  ex : (May 1, 2023, to October 15, 2023).
            Further filters the collection to include only images that intersect with the defined AOI.

    """
        self.sentinel1 = ee.ImageCollection('COPERNICUS/S1_GRD') \
                            .filterDate(start_date, end_date) \
                            .filterBounds(self.aoi)

    def get_sentinel1(self) -> ee.imagecollection.ImageCollection :
        """
            Returns the filtered image collection.
        """
        return self.sentinel1


    def load_sentinel2(self,
                     start_date : str,
                     end_date : str) -> None:
        """
            The load_sentinel2 method is designed to load and filter Sentinel-2 imagery
            data from the Google Earth Engine based on specific criteria:
            cloud cover percentage, geographic location, and date range.
        """
        self.sentinel2 = ee.ImageCollection('COPERNICUS/S2') \
                            .filter(ee.Filter.lte('CLOUDY_PIXEL_PERCENTAGE', 10)) \
                            .filterBounds(self.aoi) \
                            .filterDate(start_date, end_date)


    def get_sentinel1(self) -> ee.imagecollection.ImageCollection :
        """
            Returns the filtered image collection.
        """
        return self.sentinel2


    def process_sentinel1(self):
      

        vvIw = self.sentinel1.filter(ee.Filter.listContains('transmitterReceiverPolarisation', 'VV')).select(['VV']) \
            .filter(ee.Filter.eq('instrumentMode', 'IW'))
        vvIwAsc = vvIw.filter(ee.Filter.eq('orbitProperties_pass', 'ASCENDING'))
        vvIwDesc = vvIw.filter(ee.Filter.eq('orbitProperties_pass', 'DESCENDING'))

        vhIw = self.sentinel1.filter(ee.Filter.listContains('transmitterReceiverPolarisation', 'VH')).select(['VH']) \
            .filter(ee.Filter.eq('instrumentMode', 'IW'))
        vhIwAsc = vhIw.filter(ee.Filter.eq('orbitProperties_pass', 'ASCENDING'))
        vhIwDesc = vhIw.filter(ee.Filter.eq('orbitProperties_pass', 'DESCENDING'))

        def mosaic_by_date(imcol):
            imlist = imcol.toList(imcol.size())
            unique_dates = imlist.map(lambda im: ee.Image(im).date().format("YYYY-MM-dd")).distinct()
            mosaic_imlist = unique_dates.map(lambda d: imcol.filterDate(ee.Date(d), ee.Date(d).advance(2, "day")).mosaic().set({
                    "system:time_start": ee.Date(d).millis(),
                    "system:id": ee.Date(d).format("YYYY-MM-dd")
                }))
            return ee.ImageCollection(mosaic_imlist)

        vvIwAsc2 = mosaic_by_date(vvIwAsc)
        vhIwAsc2 = mosaic_by_date(vhIwAsc)
        vvIwDesc2 = mosaic_by_date(vvIwDesc)
        vhIwDesc2 = mosaic_by_date(vhIwDesc)

        vvIwAsc2_img = vvIwAsc2.toBands()
        vhIwAsc2_img = vhIwAsc2.toBands()
        vvIwDesc2_img = vvIwDesc2.toBands()
        vhIwDesc2_img = vhIwDesc2.toBands()

        stack_sentinel1 = vvIwAsc2_img.addBands(vhIwAsc2_img).addBands(vvIwDesc2_img).addBands(vhIwDesc2_img)

        self.stack_sentinel1 = stack_sentinel1.clip(self.aoi)

        return stack_sentinel1



    def process_sentinel2(self,
                           start_date : str):

        interval = 5
        increment = 'day'
        start = start_date
        startDate = ee.Date(start)
        secondDate = startDate.advance(interval, increment).millis()
        increase = secondDate.subtract(startDate.millis())
        list_dates = ee.List.sequence(startDate.millis(), ee.Date('2023-10-15').millis(), increase)

        colsentinel2 = self.sentinel2

        def daily_composite(date):
            date = ee.Date(date)
            image = colsentinel2.filterDate(date, date.advance(interval, increment)).mean().clip(self.aoi).set('system:time_start', date.millis())
            return image

        SENTINEL2_10DAY = ee.ImageCollection.fromImages(list_dates.map(daily_composite))
        filteredCollection = SENTINEL2_10DAY.filter(ee.Filter.listContains('system:band_names', 'B2'))

        def calculate_indices(image):
            ndvi = image.normalizedDifference(['B8', 'B4']).rename('NDVI').clip(self.aoi)
            evi = image.expression('2.5 * ((NIR - Red) / (NIR + 6 * Red - 7.5 * Blue + 1))', {
                            'NIR': image.select('B8'), 'Red': image.select('B4'), 'Blue': image.select('B2')
                        }).rename('EVI').clip(self.aoi)
            savi = image.expression('((NIR - Red) / (NIR + Red + L)) * (1 + L)', {
                            'NIR': image.select('B8'), 'Red': image.select('B4'), 'L': 0.5
                        }).rename('SAVI').clip(self.aoi)

            indicesImage = ee.Image.cat([ndvi, evi, savi])
            return indicesImage.copyProperties(image, image.propertyNames())

        indicesImage = filteredCollection.map(calculate_indices)
        self.indicesImageallband = indicesImage.toBands()


    def combine_sentinel1_sentinel2(self, stack_sentinel1, indicesImageallband) -> None:
        """

        """

        combined_features = stack_sentinel1.addBands(indicesImageallband)
        
        self.maineMODFeatures = combined_features.reduceRegions(
            collection=self.aoi,
            reducer=ee.Reducer.mean(),
            scale=10
        )



    def export_data(self, maineMODFeatures, output_dir):
        exportParams = {
            'collection': maineMODFeatures,
            'description': 'maineMODFeatures_Abdanan_03_1015_10M2',
            'folder': 'classified_ALFAALFA',
            'fileFormat': 'CSV'
        }
        export_task = ee.batch.Export.table.toCloudStorage(**exportParams)

        # Set the output path in the local filesystem
        output_path = os.path.join(output_dir, 'maineMODFeatures_Abdanan_03_1015_10M2.csv')
        task_config = {
            'bucket': 'your_bucket_name',
            'fileNamePrefix': output_path,
            'fileFormat': 'CSV'
        }

        export_task.start(task_config)

        # Wait for the export task to complete
        while export_task.active():
            # Check the status of the export task
            status = export_task.status()
            print("Export task status:", status)
            time.sleep(10)

        status = export_task.status()
        print("Export task status:", status)
        
    
    
    def pipline(self,
            start_date : str,
            finish_data : str,
            path_to_save : str) -> None:
        
        self.load_sentinel1(start_date, finish_data)
        self.load_sentinel2(start_date, finish_data)
        self.process_sentinel1()
        self.model.process_sentinel2(start_date)
        self.combine_sentinel1_sentinel2(self.stack_sentinel1, self.indicesImageallband)
        self.export_data(self.maineMODFeatures)



with open('/home/reza/Downloads/KABUDARAHANG_SAMPLE.geojson', 'r') as f:
    geoJSON = json.load(f)

coords = [[feature['geometry']['coordinates'][0][0], feature['geometry']['coordinates'][0][1]] for feature in geoJSON['features']]
aoi = ee.Geometry.Polygon(coords)


model = EarthEngineData(aoi)
model.pipline("2023-01-01", "2023-12-01", "/home/reza/Desktop")