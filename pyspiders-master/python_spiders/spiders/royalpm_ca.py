# -*- coding: utf-8 -*-
# Author: Abdulrahman Abbas
import json

import scrapy

from ..helper import string_found, extract_location_from_coordinates, extract_location_from_address
from ..loaders import ListingLoader


class RoyalpmCaSpider(scrapy.Spider):
    name = "royalpm_ca"
    start_urls = [
        'https://wix.shareiiit.com/realEstate/ppp?pageId=p0spr&compId=TPASection_j7bsl2y8&viewerCompId=TPASection_j7bsl2y8&siteRevision=2726&viewMode=site&deviceType=desktop&locale=en&regionalLanguage=en&width=980&height=1505&instance=ewBrPUcePUajbqArIqPUzkZsE3zHxGsAiFd6orltkCk.eyJpbnN0YW5jZUlkIjoiMmJiMWNiMGMtOWQwMi00MTU4LTk1YzYtNzhmNDQ2MGU5MWU4IiwiYXBwRGVmSWQiOiIxM2FhMmZkMi0xZDc0LTE0NjUtNDYzNC01YTM5NGQwNzBhZWUiLCJzaWduRGF0ZSI6IjIwMjEtMTItMTJUMjM6MTU6NTguNDg0WiIsInZlbmRvclByb2R1Y3RJZCI6InByZW1pdW0iLCJkZW1vTW9kZSI6ZmFsc2UsImFpZCI6IjE0OTNkZmZhLWJmN2UtNGE2Zi04NTk0LWQ4MzA0YWQxZTIxMCIsInNpdGVPd25lcklkIjoiNTg4ODJlYjgtYThkZS00NjI2LTg0MWUtMGVjZDViZjNmNmI4In0&commonConfig={%22brand%22:%22wix%22,%22bsi%22:%22474275c8-9322-4143-a0fc-87606f92b737|15%22,%22BSI%22:%22474275c8-9322-4143-a0fc-87606f92b737|15%22}&target=_top&section-url=https://www.royalpm.ca/royal-rentals/&vsi=a714f458-1b7b-44de-8e2d-26139364e5c7',
    ]
    country = 'canada'  # Fill in the Country's name
    locale = 'ca'  # Fill in the Country's locale, look up the docs if unsure
    external_source = "{}_PySpider_{}_{}".format(
        name.capitalize(), country, locale)
    execution_type = 'testing'
    position = 1

    # 1. SCRAPING level 1
    def start_requests(self):
        for url in self.start_urls:
            yield scrapy.Request(url, callback=self.parse)

    # 2. SCRAPING level 2
    def parse(self, response, **kwargs):
        json_data = response.xpath('//script[9]//text()').get().split('function')[0].split("eval")[-1].replace("((",
                                                                                                               "").replace(
            "));", '')
        data = json.loads(json_data)
        for item in data:
            pro_id = item["id"]
            urls = f'https://www.royalpm.ca/royal-rentals/propE{pro_id}'
            yield scrapy.Request(url=urls, callback=self.populate_item, meta={"item": item})

    # 3. SCRAPING level 3
    def populate_item(self, response):

        data = response.meta['item']
        title = data["title"]
        rent = data["price"]
        images = data["images"]

        pro_images = []
        for image in images:
            pro_images.append(image["uri"])

        external_id = data["data"]["id1422289483499"]["val"]["val"].replace("#", "")
        landlord_email = data["data"]["id1422289506220"]["val"]["val"]
        description = data["data"]['id1422289759907']["val"]["val"]
        room_count = data["data"]['id1422289669592']["val"]["val"]
        bathroom_count = data["data"]['id1422289688432']["val"]["val"]

        location = data["data"]["id1422273614114"]["val"]["val"]
        if location.isalpha() is False:
            location = title

        longitude, latitude = extract_location_from_address(location)
        zipcode, city, address = extract_location_from_coordinates(longitude, latitude)

        is_balcony = data["data"]["id1422290117358"]["isFilled"]
        is_parking = data["data"]["id1422290151855"]["isFilled"]
        landry = data["data"]["id1517245645309"]["isFilled"]

        balcony = False
        if string_found(['balcony'], description) or is_balcony is True:
            balcony = True

        washing_machine = False
        if string_found(['balcony'], description) or landry is True:
            washing_machine = True

        parking = False
        if string_found(['balcony'], description) or is_parking is True:
            parking = True

        if "Office" not in title:
            # # MetaData
            item_loader = ListingLoader(response=response)

            item_loader.add_value("external_link", response.url)  # String
            item_loader.add_value("external_source", self.external_source)  # String

            item_loader.add_value("external_id", external_id)  # String
            item_loader.add_value("position", self.position)  # Int
            item_loader.add_value("title", title)  # String
            item_loader.add_value("description", description)  # String

            # # Property Details
            item_loader.add_value("city", city)  # String
            item_loader.add_value("zipcode", zipcode)  # String
            item_loader.add_value("address", location)  # String
            item_loader.add_value("latitude", str(latitude))  # String
            item_loader.add_value("longitude", str(longitude))  # String
            # item_loader.add_value("floor", floor) # String
            item_loader.add_value("property_type", "apartment")  # String
            # item_loader.add_value("square_meters", square_meters) # Int
            item_loader.add_value("room_count", room_count[-1])  # Int
            item_loader.add_value("bathroom_count", bathroom_count)  # Int

            # item_loader.add_value("available_date", available_date) # String => date_format

            # item_loader.add_value("pets_allowed", pets_allowed) # Boolean
            # item_loader.add_value("furnished", furnished) # Boolean
            item_loader.add_value("parking", parking)  # Boolean
            # item_loader.add_value("elevator", elevator) # Boolean
            item_loader.add_value("balcony", balcony)  # Boolean
            # item_loader.add_value("terrace", terrace) # Boolean
            # item_loader.add_value("swimming_pool", swimming_pool) # Boolean
            item_loader.add_value("washing_machine", washing_machine)  # Boolean
            # item_loader.add_value("dishwasher", dishwasher) # Boolean

            # # Images
            item_loader.add_value("images", pro_images)  # Array
            item_loader.add_value("external_images_count", len(pro_images))  # Int
            # item_loader.add_value("floor_plan_images", floor_plan_images) # Array

            # # Monetary Status
            item_loader.add_value("rent", rent)  # Int
            # item_loader.add_value("deposit", deposit) # Int
            # item_loader.add_value("prepaid_rent", prepaid_rent) # Int
            # item_loader.add_value("utilities", utilities) # Int
            item_loader.add_value("currency", "CAD")  # String

            # item_loader.add_value("water_cost", water_cost) # Int
            # item_loader.add_value("heating_cost", heating_cost) # Int

            # item_loader.add_value("energy_label", energy_label) # String

            # # LandLord Details
            item_loader.add_value("landlord_name", "Royal Property Management")  # String
            item_loader.add_value("landlord_phone", '(613)969-1144') # String
            item_loader.add_value("landlord_email", landlord_email)  # String

            self.position += 1
            yield item_loader.load_item()
