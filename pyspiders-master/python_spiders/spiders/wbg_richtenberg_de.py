# -*- coding: utf-8 -*-
# Author: Mohamed Zakaria

import re
import json
import math

from scrapy import Spider, Request, FormRequest
from python_spiders.loaders import ListingLoader
from python_spiders.user_agents import random_user_agent

from python_spiders.helper import extract_location_from_address, extract_location_from_coordinates, remove_white_spaces

class Wbg_richtenberg_deSpider(Spider):
    name = 'wbg_richtenberg_de'
    country='germany'
    locale='de' 
    external_source = "{}_PySpider_{}_{}".format(name.capitalize(), country, locale)
    execution_type='testing'
    allowed_domains = ["www.wbg-richtenberg.de"]
    position = 1
    custom_settings = {
        "User-Agent": random_user_agent()
    }
    

    def start_requests(self):
        yield FormRequest(url=f"https://wbg-richtenberg.ivm-professional.de/modules/json/rest_search.php",
                    callback=self.parse,
                    method='POST')

    def parse(self, response):
        parsed_response_properties = json.loads(response.body)
        for flat_data in parsed_response_properties["flats"]:
            flat_id = flat_data["flat_id"]

            yield FormRequest( url = f"https://wbg-richtenberg.ivm-professional.de/modules/json/rest_details.php", 
                callback=self.get_property_details,
                dont_filter = True, 
                formdata = {
                    "flat_id": flat_id
                }
                # meta = {"property_data": property_data}
            )
    def get_property_details(self, response):
        parsed_response_property = json.loads(response.body)
        url = f"https://wbg-richtenberg.de/detailansicht/?flat_id={parsed_response_property['flat_id']}"
        yield Request(
            url = url,
            callback = self.populate_item,
            dont_filter = True,
            meta = {"property_data": parsed_response_property}
        )


    def populate_item(self, response):
        
        property_type = "apartment"
        property_data = response.meta.get("property_data")
        rent = str(property_data["flat_rent"])
        if(not re.search(r"([0-9]{2,})", rent)):
            return

        currency = "EUR"
        external_id = property_data["flat_id"]
        title = property_data["flat_exposetitle"]
        lowered_title = title.lower()
        if(
            "gewerbe" in lowered_title
            or "gewerbefläche" in lowered_title
            or "büro" in lowered_title
            or "praxisflächen" in lowered_title
            or "ladenlokal" in lowered_title
            or "arbeiten" in lowered_title 
            or "gewerbeeinheit" in lowered_title
            or "vermietet" in lowered_title
            or "stellplatz" in lowered_title
            or "garage" in lowered_title
            or "restaurant" in lowered_title
            or "lager" in lowered_title
            or "einzelhandel" in lowered_title
            or "sonstige" in lowered_title
            or "grundstück" in lowered_title
        ):
            return

        square_meters = str(math.ceil(float(property_data["flat_space"])))
        room_count = math.ceil(float(property_data["flat_rooms"]))

        floor = property_data["flat_floor"]

        deposit = property_data["flat_deposit"]
        utilities = property_data["flat_charges"]
        heating_cost = property_data["flat_heating"]


        energy_label = property_data["flat_energyconsumption"]
        
        address = property_data["district_name"]
        city = property_data["flat_city"]
        zipcode = property_data["flat_zip"]

        latitude = str(property_data["district_lat"])
        longitude = str(property_data["district_lon"])

        images = [f'https://wbg-richtenberg.ivm-professional.de/_img/flats/{property_data["flat_image"]}']
        description = property_data["district_description"]

        available_date = property_data['date']
        landlord_name = property_data["arranger_name"]
        landlord_phone = property_data["arranger_phone"]
        landlord_email = property_data["arranger_email"]

        item_loader = ListingLoader(response=response)

        item_loader.add_value("external_link", response.url) 
        item_loader.add_value("external_source", self.external_source) 

        item_loader.add_value("external_id", external_id) 
        item_loader.add_value("position", self.position) 
        self.position += 1
        item_loader.add_value("title", title) 

        item_loader.add_value("city", city) 
        item_loader.add_value("zipcode", zipcode) 
        item_loader.add_value("address", address) 
        item_loader.add_value("latitude", latitude) 
        item_loader.add_value("longitude", longitude) 
        item_loader.add_value("floor", floor) 
        item_loader.add_value("property_type", property_type) 
        item_loader.add_value("square_meters", square_meters) 
        item_loader.add_value("room_count", room_count) 

        item_loader.add_value("available_date", available_date) 

        item_loader.add_value("images", images) 
        item_loader.add_value("external_images_count", len(images)) 

        item_loader.add_value("rent_string", rent) 
        item_loader.add_value("deposit", deposit) 

        item_loader.add_value("utilities", utilities) 
        item_loader.add_value("currency", currency) 

        item_loader.add_value("heating_cost", heating_cost) 

        item_loader.add_value("energy_label", energy_label) 

        item_loader.add_value("landlord_name", landlord_name) 
        item_loader.add_value("landlord_phone", landlord_phone) 
        item_loader.add_value("landlord_email", landlord_email) 

        yield item_loader.load_item()
