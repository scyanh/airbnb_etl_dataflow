from typing import NamedTuple


class AirbnbData(NamedTuple):
    id: str
    name: str
    host_id: str
    host_name: str
    neighbourhood_group: str
    neighbourhood: str
    latitude: str
    longitude: str
    room_type: str
    price: str
    minimum_nights: str
    number_of_reviews: str
    last_review: str
    reviews_per_month: str
    calculated_host_listings_count: str
    availability_365: str
    number_of_reviews_ltm: str
    license: str
    price_type: str = ''
    neighborhood_avg_price: float = 0
    price_usd: float = 0
    lr_year: str = ''
    lr_month: str = ''
    lr_day: str = ''
