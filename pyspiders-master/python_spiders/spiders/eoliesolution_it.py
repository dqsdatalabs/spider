# -*- coding: utf-8 -*-
# Author: Mohamed Zakaria

import re
import requests

from scrapy import Spider, Request
from python_spiders.loaders import ListingLoader

class EoliesolutionSpider(Spider):
    name = 'eoliesolution_it'
    country='italy'
    locale='it' 
    external_source = "{}_PySpider_{}_{}".format(name.capitalize(), country, locale)
    execution_type='testing'
    allowed_domains = ["www.eoliesolution.it"]
    start_urls = ["https://eoliesolution.it/affitti_residenziali.asp"]
    position = 1

    def parse(self, response):
        for url in response.css("div.info h3 a::attr(href)").getall():
            yield Request(response.urljoin(url), callback=self.populate_item, dont_filter = True)
        

    def populate_item(self, response):
        property_type = "apartment"
        title = response.css("h1.property-title::text").get()
        lowered_title = title.lower()

        if (
            ("commerciale" in lowered_title) 
            or ("ufficio" in lowered_title) 
            or ("magazzino" in lowered_title) 
            or ("box" in lowered_title) 
            or ("auto" in lowered_title) 
            or ("negozio" in lowered_title) 
            or ("vendita" in lowered_title) ):
            return

        rent = response.css("div.price span::text").get()
        currency = "EUR"

        square_meters = response.css("li:contains('Superficie mq.:')::text").get()
        square_meters = re.findall("([0-9]+)", square_meters)
        if(len(square_meters) > 0):
            square_meters = square_meters[0]
        
        room_count = response.css("li:contains('Nr. Locali:')::text").get()
        room_count = re.findall("([0-9]+)", room_count)
        if(len(room_count) > 0):
            room_count = room_count[0]
        
        bathroom_count = response.css("li:contains('Nr. Bagni:')::text").get()
        bathroom_count = re.findall("([0-9]+)", bathroom_count)
        if(len(bathroom_count) > 0):
            bathroom_count = bathroom_count[0]

        external_id = response.css("div#property-id::text").get()
        
        images = response.css("div.item img::attr(src)").getall()
        images = [response.urljoin(image_src) for image_src in images]

        description = response.css("div.tab-pane p span strong::text").get()

        property_features = response.css("ul.property-features li::text").getall()
        property_features = " ".join(property_features)

        furnished = "Arredamento: Sì" in property_features
        parking = "posto auto" in property_features
        terrace = "Terrazzo: SI" in property_features
        floor = response.css("li:contains('piano')::text").get()

        page_address = response.css("h4:contains('Indirizzo')::text").get()
        page_address = page_address.split("Indirizzo:")[1]

        responseGeocode = requests.get(f"https://geocode.arcgis.com/arcgis/rest/services/World/GeocodeServer/find?f=json&text={page_address}&maxLocations=1")
        responseGeocodeData = responseGeocode.json()
        longitude = responseGeocodeData['locations'][0]['feature']['geometry']['x']
        longitude = str(longitude)
        latitude = responseGeocodeData['locations'][0]['feature']['geometry']['y']
        latitude = str(latitude)

        responseGeocode = requests.get(f"https://geocode.arcgis.com/arcgis/rest/services/World/GeocodeServer/reverseGeocode?location={longitude},{latitude}&f=pjson&distance=50000&outSR=")
        responseGeocodeData = responseGeocode.json()

        address = responseGeocodeData['address']['Match_addr']
        if( re.search("([0-9]+)", address)):
            address = page_address
        city = responseGeocodeData['address']['City']
        zipcode = responseGeocodeData['address']['Postal']

        landlord_name = "eoliesolution"
        landlord_phone = "+39 090 9813149 "
        landlord_email = "info@eoliesolution.it"

        if("Villa" in title):
            property_type = "house"

        if( not description ):
            description = response.css("div.tab-pane p::text").get()
        
        if(not description):
            description = response.css("div.tab-pane h4 span span strong::text").get()


        item_loader = ListingLoader(response=response)

        item_loader.add_value("external_link", response.url)
        item_loader.add_value("external_source", self.external_source)
        item_loader.add_value("position", self.position)
        self.position += 1

        item_loader.add_value("property_type", property_type)
        item_loader.add_value("title", title)
        item_loader.add_value("rent_string", rent)
        item_loader.add_value("currency", currency)
        item_loader.add_value("square_meters", square_meters)
        item_loader.add_value("room_count", room_count)
        item_loader.add_value("bathroom_count", bathroom_count)
        item_loader.add_value("external_id", external_id)
        item_loader.add_value("images", images)
        item_loader.add_value("description", description)
        item_loader.add_value("furnished", furnished)
        item_loader.add_value("parking", parking)
        item_loader.add_value("terrace", terrace)
        item_loader.add_value("floor", floor)
        item_loader.add_value("latitude", latitude)
        item_loader.add_value("longitude", longitude)
        item_loader.add_value("address", address)
        item_loader.add_value("city", city)
        item_loader.add_value("zipcode", zipcode)
        item_loader.add_value("landlord_name", landlord_name)
        item_loader.add_value("landlord_phone", landlord_phone)
        item_loader.add_value("landlord_email", landlord_email)
        
        yield item_loader.load_item()
