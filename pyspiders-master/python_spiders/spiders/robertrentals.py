import requests
import scrapy
from ..helper import *
from ..loaders import ListingLoader
import json
import math


class Greenrockpm(scrapy.Spider):
    name = 'robertsrentals'
    allowed_domains = ['www.robertsrentals.net']
    execution_type = 'testing'
    country = 'canada'
    locale = 'en'
    external_source = f"{name.capitalize()}_PySpider_{country}_{locale}"
    position=1

    def start_requests(self):

        start_urls = [
            'https://www.robertsrentals.net/_dm/s/rt/actions/sites/76efa36a/collections/appfolio-listings/ENGLISH?_=1638180989323'
                     ]
        for url in start_urls:
            yield scrapy.Request(url=url, callback=self.parse)

    def parse(self, response, **kwargs):
        resp = json.loads(response.body)
        items= resp.get("value")
        items=json.loads(items)
        parking=None
        washing_machine=None
        swimming_pool=None

        for i in range (len(items)):
            item=items[i].get("data")
            imgs = item.get("photos")
            item_loader = ListingLoader(response=response)
            # address=item.get("full_address")
            images=[next(iter(d.values())) for d in imgs]
            room_count=int(item.get("bedrooms"))
            bathroom_count=math.floor(item.get("bathrooms"))
            available_date=item.get("available_date")
            landlord_number=item.get('portfolio_phone_number')
            description=item.get('marketing_description').replace("Visit www.RobertsRentals.Net for a video tour!","")
            if "parking " or "garage" in description.lower() :
                parking=True
            if "washer" or "laundry"in description.lower():
                washing_machine=True
            if "pool" in description.lower() :
                swimming_pool=True

            landlord_email=item.get('contact_email_address')
            longitude=item.get('address_longitude')
            latitude = item.get('address_latitude')
            rent=item.get("market_rent")
            external_id=item.get("property_uid")
            square_meters=item.get('square_feet')
            title=item.get('marketing_title').split("-")[0]
            external_link="https://www.robertsrentals.net/listings/detail/"+item.get("listable_uid")
            deposit=int(item.get("deposit"))
            zipcode, city, address = extract_location_from_coordinates(float(longitude), float(latitude))

            # # # MetaData
            item_loader.add_value("external_link", external_link)  # String
            item_loader.add_value("external_source", self.external_source)  # String
            item_loader.add_value("external_id", external_id)  # String
            item_loader.add_value("position", self.position)  # Int
            item_loader.add_value("title", title)  # String
            item_loader.add_value("description", description)  # String
            # # # Property Details
            item_loader.add_value("city", city)  # String
            item_loader.add_value("zipcode", zipcode)  # String
            item_loader.add_value("address", address)  # String
            item_loader.add_value("latitude", str(latitude))  # String
            item_loader.add_value("longitude", str(longitude))  # String
            # # item_loader.add_value("floor", floor)  # String
            item_loader.add_value("property_type","house")  # String => ["apartment", "house", "room", "student_apartment", "studio"]
            item_loader.add_value("square_meters", int(square_meters) ) # Int
            item_loader.add_value("room_count", room_count)  # Int
            item_loader.add_value("bathroom_count", bathroom_count)  # Int
            #
            item_loader.add_value("available_date", available_date)  # String => date_format
            #
            # # item_loader.add_value("pets_allowed", pets_allowed) # Boolean
            # # item_loader.add_value("furnished", furnished) # Boolean
            item_loader.add_value("parking", parking)  # Boolean
            # # item_loader.add_value("elevator", elevator)  # Boolean
            # # item_loader.add_value("balcony", balcony)  # Boolean
            # # item_loader.add_value("terrace", terrace)  # Boolean
            item_loader.add_value("swimming_pool", swimming_pool) # Boolean
            item_loader.add_value("washing_machine", washing_machine)  # Boolean
            # item_loader.add_value("dishwasher", dishwasher)  # Boolean
            #
            # # # Images
            item_loader.add_value("images", images)  # Array
            item_loader.add_value("external_images_count", len(images))  # Int
            # # item_loader.add_value("floor_plan_images", floor_plan_images)  # Array
            #
            # # # Monetary Status
            item_loader.add_value("rent", int(rent))  # Int
            item_loader.add_value("deposit", int(deposit)) # Int
            # # item_loader.add_value("prepaid_rent", prepaid_rent) # Int
            # # item_loader.add_value("utilities", utilities)  # Int
            item_loader.add_value("currency", "CAD")  # String
            # #
            # # item_loader.add_value("water_cost", water_cost) # Int
            # # item_loader.add_value("heating_cost", heating_cost) # Int
            #
            # # item_loader.add_value("energy_label", energy_label)  # String
            #
            # # # LandLord Details
            item_loader.add_value("landlord_name", "Bev Roberts Rentals")  # String
            item_loader.add_value("landlord_phone", "(919) 609-3131")  # String
            item_loader.add_value("landlord_email", landlord_email) # String
            #
            self.position += 1
            yield item_loader.load_item()

            #
