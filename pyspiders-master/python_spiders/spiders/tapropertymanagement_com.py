# -*- coding: utf-8 -*-
# Author: Mohamed Zakaria

import math
import re
import requests

from scrapy import Spider, Request
from python_spiders.loaders import ListingLoader

class TapropertymanagementSpider(Spider):
    name = 'tapropertymanagement_com'
    country='canada'
    locale='en' 
    external_source = "{}_PySpider_{}_{}".format(name.capitalize(), country, locale)
    execution_type='testing'
    allowed_domains = ["www.tapropertymanagement.com"]
    start_urls = ["https://tapropertymanagement.com/properties/?availability=Available&building_type=&bedrooms="]

    def parse(self, response):
        for url in response.css("a:contains('View Property Details')::attr(href)").getall():
            yield Request(response.urljoin(url), callback=self.populate_item, dont_filter = True)

    def populate_item(self, response):

        property_type = "Apartment"
        title = response.css("h1.property-title::text").get()
        rent = response.css("dl.property-details-summary dt:contains('Rental Price:') + dd::text").get()
        rent = re.findall("([0-9]+)", rent)[0]
        currency = "CAD"

        available_date = response.css("dl.property-details-summary dt:contains('Date Available:') + dd::text").get()
        room_count = response.css("dl.property-details-summary dt:contains('Bedrooms:') + dd::text").get()
        bathroom_count = response.css("dl.property-details-summary dt:contains('Bathrooms:') + dd::text").get()
        bathroom_count = math.ceil(float(bathroom_count))

        images = response.css("a.property-thumb::attr(href)").getall()
        description = response.css("dl.property-details dd p::text").get()
        
        amenities = response.css("div.cell").getall()
        amenities = "    ".join(amenities)
        amenities = re.findall('<span class=\"dashicons dashicons-yes\"></span> ([A-Z][a-z]+)</div>', amenities)
        amenities = " ".join(amenities).lower()
        parking = "parking" in amenities
        furnished = "furnished" in amenities
        washing_machine = "laundry" in amenities
        dishwasher = "dishwasher" in amenities

        latitude = response.css("div.marker::attr(data-lat)").get()
        longitude = response.css("div.marker::attr(data-lng)").get()

        landlord_name = "tapropertymanagement"
        landlord_phone = "519 432 4325"
        landlord_email = "info@tapropertymanagement.com"

        responseGeocode = requests.get(f"https://geocode.arcgis.com/arcgis/rest/services/World/GeocodeServer/reverseGeocode?location={longitude},{latitude}&f=pjson&distance=50000&outSR=")
        responseGeocodeData = responseGeocode.json()

        address = responseGeocodeData['address']['Match_addr']
        city = responseGeocodeData['address']['City']
        zipcode = responseGeocodeData['address']['Postal']

        item_loader = ListingLoader(response=response)

        item_loader.add_value("external_link", response.url)
        item_loader.add_value("external_source", self.external_source)
        item_loader.add_value("property_type", property_type)
        item_loader.add_value("title", title)
        item_loader.add_value("rent_string", rent)
        item_loader.add_value("currency", currency)
        item_loader.add_value("available_date", available_date)
        item_loader.add_value("room_count", room_count)
        item_loader.add_value("bathroom_count", bathroom_count)
        item_loader.add_value("images", images)
        item_loader.add_value("description", description)
        item_loader.add_value("parking", parking)
        item_loader.add_value("furnished", furnished)
        item_loader.add_value("washing_machine", washing_machine)
        item_loader.add_value("dishwasher", dishwasher)
        item_loader.add_value("latitude", latitude)
        item_loader.add_value("longitude", longitude)
        item_loader.add_value("landlord_name", landlord_name)
        item_loader.add_value("landlord_phone", landlord_phone)
        item_loader.add_value("landlord_email", landlord_email)
        item_loader.add_value("address", address)
        item_loader.add_value("city", city)
        item_loader.add_value("zipcode", zipcode)
       
        yield item_loader.load_item()
