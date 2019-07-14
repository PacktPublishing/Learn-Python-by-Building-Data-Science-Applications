from sqlalchemy import String, Integer, Text

COLUMNS_RAW = [([el, String(64)], {}) for el in  [
    "address_type",
    "agency",
    "agency_name",
    "bbl",
    "borough",
    "bridge_highway_direction",
    "bridge_highway_name",
    "bridge_highway_segment",
    "city",
    "closed_date",
    "community_board",
    "complaint_type",
    "created_date",
    "cross_street_1",
    "cross_street_2",
    "descriptor",
    "due_date",
    "facility_type",
    "incident_address",
    "incident_zip",
    "intersection_street_1",
    "intersection_street_2",
    "landmark",
    "latitude",
    "location_type",
    "longitude",
    "open_data_channel_type",
    "park_borough",
    "park_facility_name",
    "road_ramp",
    "status",
    "street_name",
    "taxi_company_borough",
    "taxi_pick_up_location",
    "x_coordinate_state_plane",
    "y_coordinate_state_plane",
    "location"
]
]

COLUMNS_RAW.append((["unique_key", String(64)], {"primary_key": True}))
for col in "resolution_action_updated_date", "resolution_description":
    COLUMNS_RAW.append(([col, Text()], {}))

TOP = [(['date',   String(64)], {}),
       (['boro',   String(64)], {}),
       (['metric', String(64)], {}),
       (['value',  Integer()], {})
       ]
    
