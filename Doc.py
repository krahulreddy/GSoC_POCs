class Doc:
    # def __init__(
    #         self,
    #         place_id,
    #         osm_id,
    #         osm_type,
    #         name,
    #         address,
    #         country_code,
    #         housenumber,
    #         postcode
    # ):
    #     self.place_id = place_id
    #     self.osm_id = osm_id
    #     self.osm_type = osm_type
    #     self.name = name
    #     self.address = address
    #     self.country_code = country_code
    #     self.housenumber = housenumber
    #     self.postcode = postcode

    #     self.values = (place_id, osm_id, osm_type, name, address, \
    #     country_code, housenumber, postcode)
    #     self.keys = ("place_id", "osm_id", "osm_type", "name", "address", \
    #     "country_code", "housenumber", "postcode")

    def __init__(self, record):
        self.record = record
        self.place_id = record['place_id']
        self.osm_id = record['osm_id']
        self.osm_type = record['osm_type']
        self.name = record['name']
        self.address = record['address']
        self.country_code = record['country_code']
        self.housenumber = record['housenumber']
        self.postcode = record['postcode']
