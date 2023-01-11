"""
Name: gtfs_processor_latest.py
Things you can do with this script:
    1 - Create GIS point/line layers of stop points and/or trip line shapes 
        (based on GTFS shapes.txt file)
    2 - Create pandas dataframe (exportable to CSV) summarizing transit 
        service (e.g. VSH) by line and service type.

    You can also specify whether you want "all day" service represented in outputs,
    or just specific hours of service (e.g., 7am-9am only)

    As of May 2022, this script does NOT let you specify which days of the week (service_id) you
    want service captured for.
          
Author: Darren Conly
Last Updated: May 2022
Updated by: <name>
Copyright:   (c) SACOG
Python Version: 3.x
"""
import pdb
import os
import datetime as dt

import arcpy
import pandas as pd
import numpy as np

#===============================input info=============================

class MakeGTFSGISData(object):
    arcpy.env.overwriteOutput = True
    
    def __init__(self, gtfs_dir, gis_workspace, data_year):
        arcpy.env.workspace = gis_workspace
        self.workspace = gis_workspace
        
        os.chdir(gtfs_dir)

        self.data_year = data_year
        
        # standard gtfs input files
        self.txt_agency = 'agency.txt'
        self.txt_trips = 'trips.txt'
        self.txt_routes = 'routes.txt'
        self.txt_stops = 'stops.txt'
        self.txt_stoptimes = 'stop_times.txt'
        self.txt_shapes = 'shapes.txt'
        self.txt_calendar = 'calendar.txt'

        # inputs as dataframs
        self.df_agency = self.txt_to_df(self.txt_agency)
        # self.df_calendar = self.txt_to_df(self.txt_calendar)
        self.df_routes = self.txt_to_df(self.txt_routes)
        self.df_trips =self.txt_to_df(self.txt_trips)
        
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

        # other fields
        self.f_tripcnt_day = 'tripcnt_day' # count of weekday trips made for given route shape
        self.f_lines_stop = 'stoplines' #semicolon-delimited list of lines (route short names) serving a stop
        
        # when selecting service types from which to select trips, only get ones that happen on all weekdays
        # ideally need way to get filter out holiday svc, but different operators have difference
        # ways of doing this.
        self.weekdays = ['monday','tuesday','wednesday','thursday','friday']
        
        # gis geometry fields
        self.f_esri_shape = 'SHAPE@'
        self.f_esri_shapelen = 'SHAPE@LENGTH'
        self.f_shpsrc = "shp_source"
        
        #GTFS initially set as WGS1984 coord system
        self.spatialref_wgs = arcpy.SpatialReference(4326)
        
        #SACOG's system: NAD_1983_StatePlane_California_II_FIPS_0402_Feet
        self.sacog_projexn = arcpy.SpatialReference(2226)

        
    
    #=================DEFINE FUNCTIONS=================================
    def txt_to_df(self, in_txt, usecolumns=None, txt_delim=','):
        '''reads in txt or csv file to pandas df'''
        out_df = pd.read_csv(in_txt, usecols=usecolumns)
        return out_df
    
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

    # for each GTFS trip shape, get count of its trips per non-holiday weekday, route name, trip direction
    def agg_to_tripshp(self):
        pd.set_option('mode.chained_assignment', None) # turn off pandas SettingWithCopyWarning
        
        # from trips table, get count of trips grouped by service_id, route_id, shape_id, direction_id
        trip_cols = [self.f_routeid, self.f_svc_id, self.f_shapeid, self.f_tripid, self.f_tripdir]
        df_trips = self.df_trips[trip_cols]
        
        df_trips.loc[pd.isnull(df_trips[self.f_tripdir]), self.f_tripdir] = 0 # set null direction vals to zero

        trips_x_shapedir = df_trips.groupby([self.f_shapeid, self.f_routeid, self.f_svc_id, self.f_tripdir]) \
                           .count().reset_index()

        trips_x_shapedir = trips_x_shapedir.rename(columns = {self.f_tripid: self.f_tripcnt_day})

        # join to routes table based on route_id, adding columns for agency_id, route_short_name,
        # route_long_name
        df_routes = self.df_routes[[self.f_agencyid, self.f_routeid, self.f_routesname, self.f_routelname]]
        df_out = df_routes.merge(trips_x_shapedir, on=self.f_routeid) \
                 .merge(self.df_agency, on=self.f_agencyid)
        
        # FUTURE - filter to only get service ids that correspond to weekday service

        # resulting dataframe fields: shape_id, agency, route id, route name, route long name, trip direction, trip count 
        df_out = df_out[[self.f_shapeid, self.f_agencyname, self.f_routeid, self.f_routesname, self.f_routelname, \
                         self.f_svc_id, self.f_tripdir, self.f_tripcnt_day]]

        return df_out
    
    
        
    
    
    #make line shape for each route shape--this returns all versions of a route's geometry
    def make_trip_shp(self):
        '''line shapes with data on count of trips following each shape, further
        broken out by route, service id and trip direction'''
        
        data_df = self.agg_to_tripshp()
        
        
        fc_linshps = "GTFSLines_{}{}".format(self.agency_formatted, self.data_year)
        
        if (arcpy.Exists(fc_linshps)):
        	arcpy.Delete_management(fc_linshps)
        	
        #add route file fields
        arcpy.CreateFeatureclass_management(self.workspace, fc_linshps,"POLYLINE","","","", self.sacog_projexn)
        arcpy.AddField_management(fc_linshps, self.f_shpsrc, "TEXT", "", "", 20) # whether shape from shapes.txt or stoptimes.txt
        arcpy.AddField_management(fc_linshps, self.f_shapeid, "TEXT", "", "", 40)
        arcpy.AddField_management(fc_linshps, self.f_agencyname , "TEXT", "", "", 40)
        arcpy.AddField_management(fc_linshps, self.f_routeid , "TEXT", "", "", 50)
        arcpy.AddField_management(fc_linshps, self.f_routesname , "TEXT", "", "", 20)
        arcpy.AddField_management(fc_linshps, self.f_routelname , "TEXT", "", "", 100)
        arcpy.AddField_management(fc_linshps, self.f_svc_id , "TEXT", "", "", 50)
        arcpy.AddField_management(fc_linshps, self.f_tripdir,"TEXT","","", 1)
        arcpy.AddField_management(fc_linshps, self.f_tripcnt_day, "SHORT",)
        
        data_records = data_df.to_dict('records') # [{colA:val1, ColB: val1},{ColA:val2, ColB:val2}...]

        data_fields = [col for col in data_df.columns]
        
        route_cur = arcpy.da.InsertCursor(fc_linshps,[self.f_esri_shape, self.f_shpsrc] + data_fields)

        print("writing rows to feature class {}...".format(fc_linshps))
        
        df_shapes_aug = self.augment_shpstbl() # df fields = shape id, lat, long, sequence, shape source
        
        for row in data_records:

            shape_id = row[self.f_shapeid]
            #put points into order so trip line draws correctly
            thisshape_df = df_shapes_aug[df_shapes_aug[self.f_shapeid] == shape_id] \
                           .sort_values(by = self.f_pt_seq)

            shp_source = list(thisshape_df[self.f_shpsrc])[0]

            lats = list(thisshape_df[self.f_pt_lat])
            lons = list(thisshape_df[self.f_pt_lon])
            array = arcpy.Array()
            pt = arcpy.Point()
            for idx in range(0, len(lats)):   
                pt.X = float(lons[idx])
                pt.Y = float(lats[idx])
                array.add(pt)    
            polyline = arcpy.Polyline(array, self.spatialref_wgs)
            if self.sacog_projexn != self.spatialref_wgs:
                polyline = polyline.projectAs(self.sacog_projexn)

            data_vals = [row[f.name] for f in arcpy.ListFields(fc_linshps) if f.name in data_fields]
            row_vals = [polyline, shp_source] + data_vals
            # pdb.set_trace()
            
            route_cur.insertRow(tuple(row_vals))

        del route_cur

    def make_stop_pts(self):
        '''feature class of stop points, with fields for:
            stop id, agency, service id, tripcount.
        
        In future, consider making more granular, e.g., tripcount x line'''


        # cols needed for making table, from appropraite GTFS input files
        cols_stops = [self.f_stopid, self.f_stoplat, self.f_stoplon]
        cols_stoptimes = [self.f_stopid, self.f_tripid, self.f_stopseq]

        # make dataframe of stops with count of trips, by service type, at each stop.
        df_stops = self.txt_to_df(self.txt_stops, cols_stops)
        df_stoptimes = self.txt_to_df(self.txt_stoptimes, cols_stoptimes)
        df_trips = self.df_trips[[self.f_tripid, self.f_routeid, self.f_svc_id]]
        df_routes = self.df_routes[[self.f_routeid, self.f_routesname]]

        df_st = df_stops.merge(df_stoptimes, on = self.f_stopid) \
                .merge(df_trips, on=self.f_tripid) \
                .merge(df_routes, on=self.f_routeid)

        # add column with name of agency, in case you merge stop files together.
        df_st[self.f_agencyname] = self.agency
        
        # make dataframe (via dict) that results in field in main df that lists all lines using a stop
        stop_records1 = df_st[[self.f_stopid, self.f_routesname]].to_dict('records')
        
        stop_linelist_dict = {}  # {stop_id: [line1, line2...]}
        for d in stop_records1:
            stop_id = d[self.f_stopid]
            routename = str(d[self.f_routesname])
            if stop_linelist_dict.get(stop_id) is None:
                stop_linelist_dict[stop_id] = [routename]
            elif routename in stop_linelist_dict[stop_id]:
                continue
            else:
                stop_linelist_dict[stop_id].append(routename)
                    
        
        stop_linelist_dict2 = {k: ';'.join(v) for k, v in stop_linelist_dict.items()}
        df_stop_linelist = pd.DataFrame.from_dict(stop_linelist_dict2, orient='index').reset_index()
        df_stop_linelist = df_stop_linelist.rename(columns={0:'stoplines'})
        

        groupby_cols = [self.f_stopid, self.f_agencyname, self.f_svc_id, self.f_stoplat, self.f_stoplon]
        out_cols = groupby_cols + [self.f_stopseq]
        df_stops_out = df_st[out_cols].groupby(groupby_cols).count().reset_index()
        df_stops_out = df_stops_out.merge(df_stop_linelist, left_on=self.f_stopid, right_on='index')
        del df_stops_out['index']
        
        df_stops_out = df_stops_out.rename(columns = {self.f_stopseq: self.f_tripcnt_day})
        

        # write to point feature class
        fc_stop_pts = "GTFSstops_{}{}".format(self.agency_formatted, self.data_year)

        if (arcpy.Exists(fc_stop_pts)):
            arcpy.Delete_management(fc_stop_pts)
        	
        #add feature class fields
        arcpy.CreateFeatureclass_management(self.workspace, fc_stop_pts,"POINT","","","", self.sacog_projexn)
        arcpy.AddField_management(fc_stop_pts, self.f_stopid, "TEXT", "", "", 40)
        arcpy.AddField_management(fc_stop_pts, self.f_agencyname , "TEXT", "", "", 40)
        arcpy.AddField_management(fc_stop_pts, self.f_svc_id , "TEXT", "", "", 50)
        arcpy.AddField_management(fc_stop_pts, self.f_tripcnt_day, "SHORT",)
        arcpy.AddField_management(fc_stop_pts, self.f_lines_stop , "TEXT", "", "", 140)
        
        data_records = df_stops_out.to_dict('records') # [{colA:val1, ColB: val1},{ColA:val2, ColB:val2}...]

        data_fields = [col for col in df_stops_out.columns]
        output_fields = [f.name for f in arcpy.ListFields(fc_stop_pts) if f.name in data_fields]
        
        stoppnt_cur = arcpy.da.InsertCursor(fc_stop_pts,[self.f_esri_shape] + output_fields)
        # pdb.set_trace()

        print("writing stop points to feature class {}...".format(fc_stop_pts))
        for row in data_records:

            lat = row[self.f_stoplat]
            lon = row[self.f_stoplon]
            pt = arcpy.Point(lon, lat)  
            stop_point = arcpy.PointGeometry(pt, self.spatialref_wgs)
            if self.sacog_projexn != self.spatialref_wgs:
                stop_point = stop_point.projectAs(self.sacog_projexn)

            try:
                data_vals = [row[fname] for fname in output_fields]
                row_vals = [stop_point] + data_vals
                stoppnt_cur.insertRow(tuple(row_vals))
            except RuntimeError:
                print("Could not insert value {} because it is long. Skipping to next value..." \
                      .format(row[self.f_lines_stop]))
                    
                row[self.f_lines_stop] = "NA"
                data_vals = [row[fname] for fname in output_fields]
                row_vals = [stop_point] + data_vals
                

        del stoppnt_cur
    
    def augment_shpstbl(self):
        '''if shapes.txt file does not have the shape for a route, then use its
        stop times and locations to build lines. Lines will not be as accurate, but it is better than nothing.
        
        if there is no shapes.txt table, then just replace it entirely with the stoptime-sourced
        shapes
        
        '''
        
        # make a "pseudo shapes table" with fields normally in shapes.txt, but built from
        # stop_times
        
        # make table with trip_id, shape_id, stop_id, stop_lat, stop_long, stop_seq
        # by joining trips.txt and stop_times.txt tables
        stopcols = [self.f_stopid, self.f_stoplat, self.f_stoplon]
        df_stops = self.txt_to_df(self.txt_stops, usecolumns=stopcols)
        
        stoptime_cols = [self.f_tripid, self.f_stopid, self.f_stopseq]
        df_stoptimes = self.txt_to_df(self.txt_stoptimes, usecolumns=stoptime_cols)
        
        df_merge = self.df_trips.merge(df_stoptimes, on=self.f_tripid) \
            .merge(df_stops, on=self.f_stopid)
            
        
        # drop trip_id column, get unique shape_id values with stop points and sequences,
        # effectively replicating format of shapes.txt but instead built from stop locations
        shptbl_cols = [self.f_shapeid, self.f_stoplat, self.f_stoplon, self.f_stopseq]
        df_shpfrmstops = df_merge[shptbl_cols].drop_duplicates()
        
        # rename so that columns match those in shapes.txt
        field_rename_dict = {self.f_stoplat: self.f_pt_lat, self.f_stoplon: self.f_pt_lon,
                             self.f_stopseq: self.f_pt_seq}
        df_shpfrmstops = df_shpfrmstops.rename(columns=field_rename_dict)
        
        df_shpfrmstops = df_shpfrmstops.sort_values([self.f_shapeid, self.f_pt_seq])
        
        df_shpfrmstops[self.f_shpsrc] = "stoptimestxt"
        
        
        # try making shapes df from shapes table. If successful, fill in missing
        # shapes with stoptime-based shapes. If fail, just use stoptime-based shapes
        try:
            shptxtcols = [self.f_shapeid, self.f_pt_lat, self.f_pt_lon, self.f_pt_seq]
            df_shapes = self.txt_to_df(self.txt_shapes, usecolumns=shptxtcols)
            df_shapes[self.f_shpsrc] = 'shapestxt'
            
            shptxt_shpids = df_shapes[self.f_shapeid].unique()  # shape ids in shapes.txt
            
            tripshp_ids = self.df_trips[self.f_shapeid].unique() # shape ids in the trip table

            for shpid in tripshp_ids:
                if shpid not in shptxt_shpids:  # if shape id missing from shapes.txt, then add it from the stop-times based shape.
                    shp_to_append = df_shpfrmstops.loc[df_shpfrmstops[self.f_shapeid] == shpid]
                    df_shapes.append(shp_to_append)
                else:
                    continue
        except FileNotFoundError:
            print("Could not find shapes.txt file. Using stop points to draw shapes." \
                  "lines may not follow actual streets.")
            df_shapes = df_shpfrmstops # if no shapes.txt, then use stoptimes-derived line shapes
        
        # output fields = shape id, lat, long, sequence, shape source
        return df_shapes
        
                
        
        
        
    def fix_time_stamp(self, str_time_in):
        '''if time stamp hour is greater than 23, it fixes it so hours only
        range from zero to 23'''

        try:
            str_split = [int(x) for x in str_time_in.split(':')]
            hour = str_split[0]
            mins = str_split[1]
            secs = str_split[2]
            
            str_date = dt.datetime.strftime(dt.datetime.now(),'%Y-%m-%d')
            
            if hour > 23:
                
                hourfixed = hour - 24
                tstamp_fixed = '{} {}:{}:{}'.format(str_date, hourfixed, mins, secs)
            else:
                tstamp_fixed = '{} {}:{}:{}'.format(str_date, hour, mins, secs)
                
            return tstamp_fixed  
        except AttributeError:
            pass
            
        
    def get_prd_opdata(self, str_prd_start, str_prd_end, use_whole_day=False, groupby_attrs=[]):
        '''for a given time period, get:
            -count of trips starting within time period
            -average headway within period
            -veh svc hours within period
        INPUTS:
            -prd_start = start time, formatted as "hh:mm:ss", 24hr time
            -prd_start = period end time, formatted as "hh:mm:ss", 24hr time
            
        ***NEXT STEP - NEED TO FILTER OUT TO ONLY INCLUDE WEEKDAY SERVICE TYPE OR
        TO HAVE GOOD WAY OF GROUPING TOGETHER ALL WEEKDAY SERVICE TYPES***
        '''
        
        # make col name for period values
        str_tstart = ''.join(str_prd_start.split(':')[:2])
        str_tend = ''.join(str_prd_end.split(':')[:2])
        f_prdprefix = "{}_{}".format(str_tstart, str_tend)
        
        # define field names
        f_tripstart = 'trip_start_time' #will be departure time formatted as time intead of string
        f_st_first_trip = 'st_first_trip'
        f_st_last_trip = 'st_last_trip'
        f_svc_span_mins = 'svc_span_mins'
        f_tripend = 'trip_end_time'
        f_trip_ttmins = 'tt_mins{}'.format(f_prdprefix) # travel time in minutes for trip
        f_headway = 'headway{}'.format(f_prdprefix)
        f_headway_day = 'hdwy_fullday'
        f_vsh = 'veh_svc_hrs{}'.format(f_prdprefix)
        f_tripcnt_prd = 'tripcnt{}'.format(f_prdprefix)

        
        # convert time strings to datetime objects and get duration of service period in minutes
        ts_prd_start = dt.datetime.strptime(str_prd_start, '%H:%M:%S')
        ts_prd_end = dt.datetime.strptime(str_prd_end, '%H:%M:%S')
        prd_dur_mins = (ts_prd_end - ts_prd_start).seconds / 60 # duration of time period in minutes
        
        
        #convert datetimes to time
        ts_prd_start = ts_prd_start.time()
        ts_prd_end = ts_prd_end.time()
        
        # make stoptimes dataframe with specified columns
        cols = [self.f_tripid, self.f_depart_time, self.f_arrive_time, self.f_stopseq]
        df_1 = self.txt_to_df(self.txt_stoptimes, usecolumns=cols) 
        
        # some RT time stamps have midnight hour as 24, not zero. this fixes it.
        for time_field in (self.f_depart_time, self.f_arrive_time):
            df_1[time_field] = df_1[time_field].apply(lambda x: self.fix_time_stamp(x))
            
            
        # create columes where times formatted as times instead of strings
        df_1[f_tripstart] = pd.to_datetime(df_1[self.f_depart_time])
        df_1[f_tripend] = pd.to_datetime(df_1[self.f_arrive_time])
        
        
        # filter to only get departure times within specified time period
        df_1 = df_1.loc[(df_1[f_tripstart].dt.time >= ts_prd_start) \
                                          & (df_1[f_tripstart].dt.time < ts_prd_end)]
        
        # make df of each trip's end time
        df_tripends = df_1[[self.f_tripid, f_tripend, self.f_stopseq]] \
            .groupby(self.f_tripid).max().reset_index()
        
        
        # filter to only get records for departing first stop of trip
        first_stop = df_1[self.f_stopseq].min()
        df_tripstarts = df_1.loc[df_1[self.f_stopseq] == first_stop]
        
        # add end point data to df of trip starts, resulting in table with trip id, trip start time, trip end time
        cols = [self.f_tripid, f_tripstart]
        df_startend = df_tripstarts[cols].merge(df_tripends, on=self.f_tripid)
        
        # get each trip's end-to-end travel time as time delta as float number
        df_startend[f_trip_ttmins] = df_startend[f_tripstart] - df_startend[f_tripend] 
        df_startend[f_trip_ttmins] = df_startend[f_trip_ttmins].dt.seconds/3600
        

        # merges to tag route and trip data to each stop time
        df_merged = df_startend.merge(self.df_trips, on=self.f_tripid) \
            .merge(self.df_routes, on=self.f_routeid)
            
        
        # group by specified attribs
        gbcols = [self.f_routeid, self.f_shapeid, self.f_tripdir, self.f_svc_id]
        out_cols = gbcols + [self.f_routesname, f_tripstart, f_trip_ttmins]
        
        df_groupby = df_merged[out_cols].groupby(gbcols)
        
        #get count of trips and sum of travel time in period
        aggcol_namedict = {'count':f_tripcnt_prd, "min": f_st_first_trip, "max": f_st_last_trip}
        aggfunc_names = [k for k, v in aggcol_namedict.items()]
        
        df_tripstats = df_groupby[f_tripstart].agg(aggfunc_names) \
            .rename(columns={'count':f_tripcnt_prd, "min": f_st_first_trip, "max": f_st_last_trip})
        
        # get sum tt_mins for VSH
        df_triptt = df_groupby[f_trip_ttmins].agg('sum')
        
        
        # join VSH and trip starts tables into one
        df_out = df_tripstats.join(df_triptt).reset_index()
   
        
        # get total time between first and last trips of day for service/direction
        # and average headway for the entire day
        df_out[f_svc_span_mins] = df_out[f_st_last_trip] - df_out[f_st_first_trip]
        df_out[f_svc_span_mins] = df_out[f_svc_span_mins].dt.seconds / 60
        if use_whole_day:
            df_out[f_headway_day] = df_out[f_svc_span_mins] / df_out[f_tripcnt_prd]
            
        df_out[self.f_svc_id] = df_out[self.f_svc_id].astype('str') # to be consistent dtype with GIS feature class
        df_out[self.f_shapeid] = df_out[self.f_shapeid].astype('str') # to be consistent dtype with GIS feature class
        
        df_out[f_vsh] = df_out[f_trip_ttmins] / 60 # get veh svc hours
        df_out = df_out.rename(columns={f_tripstart: f_tripcnt_prd})
        del df_out[f_trip_ttmins]

        # calculate headway in mins for specific period, not whole day, if whole day was specified.
        if use_whole_day is False:
            df_out[f_headway] = prd_dur_mins / df_out[f_tripcnt_prd]  
        
        # attach calendar dates to output, if calendar.txt is available
        try:
            cols_cal = [self.f_svc_id, self.start_date, self.end_date]
            df_cal = pd.read_csv(self.txt_calendar)[cols_cal]
            df_cal[self.f_svc_id] = df_cal[self.f_svc_id].astype('str')
            
            df_out = df_out.merge(df_cal, on=self.f_svc_id)
        except:
            print("warning: calendar.txt not found. Output will not contain info on when GTFS data cover.")
            pass
        
        return df_out
        
        
    
#=======================RUN FUNCTIONS======================================
if __name__ == '__main__':
    # folder containing GTFS text files
    gtfs_folder = r'Q:\SACSIM19\2020MTP\transit\Sidewalk Labs\OperatorData_SWL\YoloBus\2019_3Summer\yolocounty-ca-us_eff 7-1-2019'
    # gtfs_folder = r'Q:\SACSIM19\2020MTP\transit\Sidewalk Labs\OperatorData_SWL\SRTD\2020_4Fall\google_transit'
    
    # ESRI file geodatabase you want output files to appear in
    gis_fgdb = r'Q:\SACSIM23\Network\SM23GIS\SM23Testing.gdb'
    
    # Year flag only used in output feature class and file names, not used for any calculations
    # No specific format needed, but be as concise as possible
    year = 'Fall2020' 
    
    # Parameters to determine what time of day you want to summarize service for
    # Enter as 'hh:mm:ss' using 24-hour time
    # If you are using all day service, you do not have to edit these parameters.
    start_time = '15:00:00' # starting at or after this time 
    end_time = '17:59:00' # and ending before this time
    
    # instead of getting op data for specified period of day
    # This overrides the start_time and end_time variable values.
    use_entire_day = True 

    # only applicable if outputting to GIS. Indicate if you want lines, stops, or both in outputs
    make_trip_shps = True # whether to make GIS lines for each trip shape
    make_stop_pt_shps = True # whether to make GIS point file of operator's stop locations
    
    #----------BEGIN SCRIPT-----------------
    
    if use_entire_day:
        start_time = '00:00:00' # starting at or after this time
        end_time = '23:59:00' # and ending before this time
    
    str_tstart = ''.join(start_time.split(':')[:2])
    str_tend = ''.join(end_time.split(':')[:2])
    f_prdprefix = "{}_{}".format(str_tstart, str_tend)
    
    opdir = os.path.dirname(gtfs_folder)

    output_type = input("choose output type (csv, gis): ")
    output_type = output_type.lower()
    
    gtfso = MakeGTFSGISData(gtfs_folder, gis_fgdb, year)
    
    if output_type not in ('csv', 'gis'):
        raise Exception("Invalid output type. Please enter either 'gis' or 'csv'.")
    elif output_type == 'gis':
        if make_trip_shps:
            gtfso.make_trip_shp()
        if make_stop_pt_shps:
            gtfso.make_stop_pts()
        print(f"Success! Results are in {gis_fgdb}")
    else:
        df = gtfso.get_prd_opdata(start_time, end_time, use_entire_day, groupby_attrs=['route_id', 'route_short_name', 'shape_id'])
        out_csv = os.path.join(opdir,"gtfs_{}_opdata{}.csv".format(os.path.basename(opdir), f_prdprefix))
        df.to_csv(out_csv, index=False)
        print(f"Success! Results are in {out_csv}")
