class Doc:
    def __init__(
            self,
            place_id,
            osm_id,
            osm_type,
            name,
            address,
            country_code,
            housenumber,
            postcode
    ):
        self.place_id = place_id
        self.osm_id = osm_id
        self.osm_type = osm_type
        self.name = name
        self.address = address
        self.country_code = country_code
        self.housenumber = housenumber
        self.postcode = postcode
