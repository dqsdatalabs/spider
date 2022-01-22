# -*- coding: utf-8 -*-
# Author: Mohamed Zakaria

import re
import urllib
import requests

from scrapy import Spider, Request
from python_spiders.loaders import ListingLoader

class ColumbiapropertySpider(Spider):
    name = 'columbiaproperty_ca'
    country='canada'
    locale='en' 
    external_source = "{}_PySpider_{}_{}".format(name.capitalize(), country, locale)
    execution_type='testing'
    allowed_domains = ["www.columbiaproperty.ca"]
    start_urls = ["https://columbiaproperty.ca/listings/public/view/residential"]

    def parse(self, response):
        property_pages = {}
        for url in response.css("div.listing a::attr(href)").getall():
            property_pages[url] = url

        for url in property_pages:
            yield Request(response.urljoin(property_pages[url]), callback=self.populate_item, dont_filter = True)

    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        property_type = response.css("span:contains('Building Type:') + span::text").get().strip()
        if( property_type != "Apartment"):
            property_type = "House"

        title = response.css("div.ten h1::text").get()

        rent = response.css("div.listingRent::text").get().strip().split("/")[0]
        rent = re.findall("([0-9]+)", rent)[0]
        currency = "CAD"

        address = response.css("div.description div.propertyInfo span.address::text").getall()
        address = " ".join(address)
        room_count = response.css("span:contains('Bedrooms:') + span::text").get()
        bathroom_count = response.css("span:contains('Bathrooms:') + span::text").get()
        available_date = response.css("span:contains('Available:') + span::text").get()

        parking = response.css("span:contains('Parking:') + span::text").get()
        if( parking ):
            parking = True
        else:
            parking = False

        description = response.css("div.propertyInfo p::text").getall()
        description = " ".join(description).strip()

        property_features = response.css("ul.listingFeatures li::text").getall()

        balcony = "Balcony/Deck" in property_features
        elevator = "Elevator" in property_features
        dishwasher = "Dishwasher" in property_features
        washing_machine = "Washing machine" in property_features

        images = response.css("a.imageLink img::attr(src)").getall()

        images = [ response.urljoin(urllib.parse.quote(re.sub("thumbs/", "", image_src))) for image_src in images]

        landlord_name = "columbiaproperty"
        landlord_phone = "250-851-9310"
        landlord_email = "info@columbiaproperty.ca"
        address_api = address
        if("-" in address_api):
            address_api = address.split("-")[1]
        arcgis_api = f"https://geocode.arcgis.com/arcgis/rest/services/World/GeocodeServer/find?f=json&text={address_api}"
        
        response = requests.get(arcgis_api)
        latitude = re.findall('"y":(-?[0-9]+\.[0-9]+)', response.text)[0]
        longitude = re.findall('"x":(-?[0-9]+\.[0-9]+)', response.text)[0]

        item_loader.add_value("external_link", response.url)
        item_loader.add_value("external_source", self.external_source)
        item_loader.add_value("property_type", property_type)
        item_loader.add_value("title", title)
        item_loader.add_value("rent_string", rent)
        item_loader.add_value("currency", currency)
        item_loader.add_value("address", address)
        item_loader.add_value("room_count", room_count)
        item_loader.add_value("bathroom_count", bathroom_count)
        item_loader.add_value("available_date", available_date)
        item_loader.add_value("parking", parking)
        item_loader.add_value("description", description)
        item_loader.add_value("balcony", balcony)
        item_loader.add_value("elevator", elevator)
        item_loader.add_value("dishwasher", dishwasher)
        item_loader.add_value("washing_machine", washing_machine)
        item_loader.add_value("images", images)
        item_loader.add_value("landlord_name", landlord_name)
        item_loader.add_value("landlord_phone", landlord_phone)
        item_loader.add_value("landlord_email", landlord_email)
        item_loader.add_value("latitude", latitude)
        item_loader.add_value("longitude", longitude)
       
        yield item_loader.load_item()
