"""
Name: gtfs_data.py
Purpose: load gtfs files into relevant pandas dataframes and data types as needed


Author: Darren Conly
Last Updated: Feb 2023
Updated by: 
Copyright:   (c) SACOG
Python Version: 3.x
"""
import os

import pandas as pd
import geopandas as gpd

class GTFSData(object):
    
    def __init__(self, gtfs_dir, data_year, use_shapestxt=True):
        
        os.chdir(gtfs_dir)

        self.data_year = data_year
        self.use_shapestxt = use_shapestxt
        
        # standard gtfs input files
        self.txt_agency = 'agency.txt'
        self.txt_trips = 'trips.txt'
        self.txt_routes = 'routes.txt'
        self.txt_stops = 'stops.txt'
        self.txt_stoptimes = 'stop_times.txt'
        self.txt_shapes = 'shapes.txt'
        self.txt_calendar = 'calendar.txt'

        # load non-spatial data to pandas dataframes
        self.df_agency = self.txt_to_df(self.txt_agency)
        # self.df_calendar = self.txt_to_df(self.txt_calendar)
        self.df_trips = self.txt_to_df(self.txt_trips)  
        self.df_stoptimes = self.txt_to_df(self.txt_stoptimes) 

        # load data with spatial attribs to geodataframes
        self.gdf_stops = self.txt_to_df(self.txt_stops, f_lat=self.f_pt_lat, f_lon=self.f_pt_lon)
        if self.use_shapestxt:
            self.gdf_lineshps = self.make_lineshp_gdf()
        
        # shape.txt cols
        self.f_shapeid = 'shape_id'
        self.f_pt_seq = 'shape_pt_sequence'
        self.f_pt_lat = 'shape_pt_lat'
        self.f_pt_lon = 'shape_pt_lon'
        
        # agency.txt cols
        self.f_agencyid = 'agency_id'
        self.f_agencyname = 'agency_name'

        # get agency name from GTFS file
        self.agency = self.df_agency[self.f_agencyname][0] #agency name
        self.agency_formatted = self.remove_forbidden_chars(self.agency)
        
        # route-level cols
        self.f_routeid = 'route_id'
        self.f_routesname = 'route_short_name'
        self.f_routelname = 'route_long_name'
        
        # trip-level cols
        self.f_tripid = 'trip_id'
        self.f_tripdir = 'direction_id'

        # stop-level cols
        self.f_stopid = 'stop_id'
        self.f_stopseq = 'stop_sequence'
        self.f_stoplat = 'stop_lat'
        self.f_stoplon = 'stop_lon'
        
        # stop-time columns
        self.f_depart_time = 'departure_time'
        self.f_arrive_time = 'arrival_time'

        # calendar cols
        self.f_svc_id = 'service_id'  # for joining to trip table
        self.start_date = 'start_date'
        self.end_date = 'end_date'

        
        # when selecting service types from which to select trips, only get ones that happen on all weekdays
        # ideally need way to get filter out holiday svc, but different operators have difference
        # ways of doing this.
        self.weekdays = ['monday','tuesday','wednesday','thursday','friday']
        
        # gis geometry fields
        self.f_geom = 'geometry'
        self.f_shpsrc = "shp_source"
        
        #GTFS initially set as WGS1984 coord system
        self.epsg_wgs = 4326
        
        #SACOG's system: NAD_1983_StatePlane_California_II_FIPS_0402_Feet
        self.epsg_sacog = 2226

        
    
    #=================DEFINE FUNCTIONS=================================
    def txt_to_df(self, in_txt, usecolumns=None, txt_delim=',', f_lat=None, f_lon=None):
        '''reads in txt or csv file to pandas df'''
        df = pd.read_csv(in_txt, usecols=usecolumns)

        if f_lat and f_lon:
            df = gpd.GeoDataFrame(df, geometry=gpd.points_from_xy(df[self.stop_lon], df[self.stop_lat))

        return df

    def make_lineshp_gdf(self):
        gdf = self.txt_to_df(self.txt_shapes) # load all shape points to gdf
        gdf = gdf.sort_values(by=[self.f_shapeid, self.f_pt_seq]) # sort by shape id and point sequence

        # convert shapepoints into lines, 1 line for each shape_id
        gdf = gdf.groupby(self.f_shapeid)[self.f_geom].apply(lambda x: LineString(x.tolist()))

        return gdf

    
    def remove_forbidden_chars(self, in_str):
        '''Replaces forbidden characters with acceptable characters'''
        repldict = {"&":'And','%':'pct','/':'_', ' ':'', '-':'', '(':'_',
                    ')':'_'}

        out_str = in_str
        for old, new in repldict.items():
            if old in out_str:
                out_str = out_str.replace(old, new)
            else:
                continue
        return out_str



if __name__ == '__main__':
    