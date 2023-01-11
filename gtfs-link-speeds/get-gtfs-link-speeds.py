"""
Name: get-gtfs-link-speeds.py
Purpose: For each trip on each route in a GTFS dataset, compute the time it takes to
    travel between each pair of stops.


Author: Darren Conly
Last Updated: Feb 2023
Updated by: 
Copyright:   (c) SACOG
Python Version: 3.x
"""



"""
# Desired outputs:
   1 - "All data" table with following data for each row:
    ***each link will have multiple records for each route***
        -link_id (concat of begin_stop_id and end_stop_id)
        -begin_stop_id
        -end_stop_id
        -link_distance (distance from begin_stop_id to end_stop_id)
        -link_dist_src (either from "shapes.txt" or orthogonal distance between stops)
        -agency
        -route name
        -route directions
        -shape id
        -trip id
        -begin_stop_id
        -end_stop_id
        -begin_stop_departure
        -end_stop_arrival
        -link_travel_time
        -link_speed
        -ss_time_prd (which SACSIM time period the trip left the begin_stop in)

    2 - "Data for GIS" table
        ***Ideally, each unique link_id will have only 1 record assocaited with it.
        WARNING that 2 different link_ids may overlap completetly. Fields ideally include:
        -link_geometry (for GIS mapping)
        -link_id (concat of begin_stop_id and end_stop_id)
        -begin_stop_id
        -end_stop_id
        -link_distance (distance from begin_stop_id to end_stop_id)
        -link_dist_src (either from "shapes.txt" or orthogonal distance between stops)
        -for each ss_time_prd:
            -spd_min
            -spd_max
            -spd_mean
            -spd_stdev

"""

# 1 - load all trips, stops, and lines into geodataframes from raw GTFS files
    # use gtfs_data.py class to load data into instance of that class

# 2 - line splitting process--*for each trip in trips.txt*:
    # get shape geometry for trip (TBD what to do if no shapes.txt)
    # make gdf of trip's stop-time points snapped to trip's shape (or just stops.txt to save memory)
    # make gdf of line segments split at snapped stop points (i.e., make links), now need begin_stop_id and end_stop_id
    # for each link:
        # select records from stop-times gdf where
            # trip_id matches current trip ID
            # points touch the link (or are very close to it)
                # to save memory, could do spatial select from stops (not stop-times) then do join of selection based on stop id
        # sort the stop-times records by stop sequence, ascending
        # appropriately set value's for trip's begin_stop_id, end_stop_id, departure time, end time
        # comput link distance in miles
        # compute link speed in MPH

    





if __name__ == '__main__':
    