import scrapy
from scrapy import Request, FormRequest
from scrapy.utils.response import open_in_browser
from scrapy.loader import ItemLoader
from scrapy.selector import Selector

from ..loaders import ListingLoader
from ..helper import *
from ..user_agents import random_user_agent

import requests
import re
import time
from urllib.parse import urlparse, urlunparse, parse_qs

class HabitatSpider(scrapy.Spider):

    name = 'habitat'
    execution_type = 'testing'
    country = 'italy'
    locale = 'it'
    external_source = f"{name.capitalize()}_PySpider_{country}_{locale}"
    allowed_domains = ['www.habitat.roma.it']
    post_url = "https://www.habitat.roma.it/wp-admin/admin-ajax.php"

    position = 1

    def start_requests(self):
        start_urls = [
            {
                "formdata": {
                "action": "ajax_filter_listings",
                "action_values": "affitto",
                "category_values": "appartamento",
                "city": "All Cities",
                "area": "All Areas",
                "order": "0",
                "newpage": "1",
                },
                'property_type': 'apartment'
            },
            {
                "formdata": {
                "action": "ajax_filter_listings",
                "action_values": "affitto",
                "category_values": "attico",
                "city": "All Cities",
                "area": "All Areas",
                "order": "0",
                "newpage": "1",
                },
                'property_type': 'apartment'
            },
            {
                "formdata": {
                "action": "ajax_filter_listings",
                "action_values": "affitto",
                "category_values": "bilocale",
                "city": "All Cities",
                "area": "All Areas",
                "order": "0",
                "newpage": "1",
                },
                'property_type': 'apartment'
            },
            {
                "formdata": {
                "action": "ajax_filter_listings",
                "action_values": "affitto",
                "category_values": "casale-rustico",
                "city": "All Cities",
                "area": "All Areas",
                "order": "0",
                "newpage": "1",
                },
                'property_type': 'house'
            },
            {
                "formdata": {
                "action": "ajax_filter_listings",
                "action_values": "affitto",
                "category_values": "bifamiliare",
                "city": "All Cities",
                "area": "All Areas",
                "order": "0",
                "newpage": "1",
                },
                'property_type': 'house'
            },
            {
                "formdata": {
                "action": "ajax_filter_listings",
                "action_values": "affitto",
                "category_values": "unifamiliare",
                "city": "All Cities",
                "area": "All Areas",
                "order": "0",
                "newpage": "1",
                },
                'property_type': 'house'
            },

        ]

        for url in start_urls:

            yield FormRequest(url = self.post_url, 
                          formdata = url.get('formdata'), 
                          callback = self.parse, 
                          dont_filter = True, 
                          meta = {'property_type': url.get('property_type'),"formdata": url.get('formdata')})

    def parse(self, response):
        

        
        cards = response.css(".listing_wrapper")

        for index, card in enumerate(cards):

            position = self.position
            property_type = response.meta['property_type']
            card_url = card.css(".property_listing::attr(data-link)").get()
            external_id = card.css("::attr(data-listid)").get()

            dataUsage = {
                "position": position,
                "property_type": property_type,
                "card_url": card_url,
                "external_id": external_id,
            }


            HabitatSpider.position += 1
            yield Request(card_url, callback=self.parseApartment, dont_filter=True, meta=dataUsage)

        if len(cards) > 0 :
            nextPageUrlNumber = int(response.meta['formdata']['newpage'].encode("ascii")) + 1
            response.meta['formdata']['newpage'] = str(nextPageUrlNumber)
            if nextPageUrlNumber:
                yield FormRequest(  url = self.post_url, 
                                    callback = self.parse, 
                                    formdata=response.meta['formdata'], 
                                    dont_filter=True,
                                    meta = response.meta)

    def parseApartment(self, response):

        property_type = response.meta['property_type']

        external_id = response.meta['external_id']

        square_meters = response.css(".listing_detail:contains('Superficie Coperta')::text").get()
        if square_meters:
            square_meters = remove_white_spaces(square_meters)
            square_meters = extract_number_only(square_meters)

        room_count = response.css(".listing_detail:contains('Locali')::text").get()
        if room_count:
            room_count = remove_white_spaces(room_count)
            room_count = extract_number_only(room_count)
        else:
            room_count = 1

        bathroom_count = response.css(".listing_detail:contains('Bagni')::text").get()
        if bathroom_count:
            bathroom_count = remove_white_spaces(bathroom_count)
            bathroom_count = extract_number_only(bathroom_count)
        else:
            bathroom_count = 1

        rent = response.css(".listing_detail:contains('Prezzo')::text").get()
        if rent:
            rent = remove_white_spaces(rent)
            rent = extract_number_only(rent).replace(".", "")

        currency = response.css(".listing_detail:contains('Prezzo')::text").get()
        if currency:
            currency = remove_white_spaces(currency)
            currency = currency_parser(currency, self.external_source)
        else:
            currency = "EUR"

        title = response.css("h1.entry-title::text").get()
        if title:
            title = remove_white_spaces(title)


        city = response.css(".adres_area i::text").get()
        if city:
            city = remove_white_spaces(city)
            
        area = response.css(".adres_area i:nth-of-type(2)::text").get()
        if area:
            area = remove_white_spaces(area)

        address = f"{area}, {city}"
        
        description = response.css('.single-content.listing-content p::text, .single-content.listing-content p strong::text, .dsc::text').getall()
        description = " ".join(description)
        description = remove_white_spaces(description)

        energy_label = response.css(".listing_detail:contains('Energetica')::text").get()
        if energy_label:
            energy_label = remove_white_spaces(energy_label)


        latitude = response.css("#googleMapSlider::attr(data-cur_lat)").get()
        if latitude:
            latitude = latitude
        
        longitude = response.css("#googleMapSlider::attr(data-cur_long)").get()
        if longitude:
            longitude = longitude

        images = response.css('.item img::attr(src)').getall()
        external_images_count = len(images)

        floor_plan_images = response.css('.floor_image img::attr(src)').getall()

        
        utilities = response.css(".listing_detail:contains('Spese Condominiali')::text").get()
        if utilities:
            utilities = extract_number_only(utilities)

        elevator = response.css(".listing_detail:contains('Ascensore')::text").get()
        if elevator:
            elevator = remove_white_spaces(elevator.lower())
            if elevator == "si":
                elevator = True
            elif elevator == "no":
                elevator = False
            elif int(elevator) != 0:
                elevator = True
            else:
                elevator = False

        landlord_name = "habitat roma"
        landlord_email = "info@habitat.roma.it"
        landlord_phone = "063215245"
        
        responseGeocode = requests.get(f"https://geocode.arcgis.com/arcgis/rest/services/World/GeocodeServer/reverseGeocode?location={longitude},{latitude}&f=pjson&distance=50000&outSR=")
        responseGeocodeData = responseGeocode.json()
        zipcode = responseGeocodeData['address']['Postal']
        
        if rent:
            item_loader = ListingLoader(response=response)
            item_loader.add_value("external_link", response.url)
            item_loader.add_value("external_source", self.external_source)
            item_loader.add_value("external_id", external_id)
            item_loader.add_value("title", title)
            item_loader.add_value("description", description)
            item_loader.add_value("city", city)
            item_loader.add_value("zipcode", zipcode)
            item_loader.add_value("address", address)
            item_loader.add_value("latitude", latitude)
            item_loader.add_value("longitude", longitude)
            item_loader.add_value("property_type", property_type)
            item_loader.add_value("square_meters", square_meters)
            item_loader.add_value("room_count", room_count)
            item_loader.add_value("bathroom_count", bathroom_count)
            item_loader.add_value("images", images)
            item_loader.add_value("floor_plan_images",floor_plan_images)
            item_loader.add_value("external_images_count", external_images_count)
            item_loader.add_value("rent", rent)
            item_loader.add_value("currency", currency)
            item_loader.add_value("utilities", utilities)
            item_loader.add_value("energy_label", energy_label)
            item_loader.add_value("elevator", elevator)
            item_loader.add_value("landlord_name", landlord_name)
            item_loader.add_value("landlord_email", landlord_email)
            item_loader.add_value("landlord_phone", landlord_phone)
            item_loader.add_value("position", response.meta['position'])
            yield item_loader.load_item()



def get_p_type_string(p_type_string):

    if p_type_string and "studio" in p_type_string.lower():
        return "studio"
    elif p_type_string and "student" in p_type_string.lower():
        return "student_apartment"
    elif p_type_string and ("apartment" in p_type_string.lower() or "terrace" in p_type_string.lower() or "flat" in p_type_string.lower() or "maisonette" in p_type_string.lower()):
        return "apartment"
    elif p_type_string and ("house" in p_type_string.lower() or "detached" in p_type_string.lower() or "bungalow" in p_type_string.lower()):
        return "house"
    else:
        return None


def energy_label_calculate(energy_number):
    energy_number = int(energy_number)
    energy_label = ""
    if energy_number >= 92:
        energy_label = "A"
    elif energy_number >= 81 and energy_number <= 91:
        energy_label = "B"
    elif energy_number >= 69 and energy_number <= 80:
        energy_label = "C"
    elif energy_number >= 55 and energy_number <= 68:
        energy_label = "D"
    elif energy_number >= 39 and energy_number <= 54:
        energy_label = "E"
    elif energy_number >= 21 and energy_number <= 38:
        energy_label = "F"
    else:
        energy_label = "G"
    return energy_label
