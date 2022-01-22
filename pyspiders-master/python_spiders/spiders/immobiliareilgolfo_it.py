# -*- coding: utf-8 -*-
# Author: Mohamed Zakaria

import re
import requests

from scrapy import Spider, Request
from python_spiders.loaders import ListingLoader

class ImmobiliareilgolfoSpider(Spider):
    name = 'immobiliareilgolfo_it'
    country='italy'
    locale='it' 
    external_source = "{}_PySpider_{}_{}".format(name.capitalize(), country, locale)
    execution_type='testing'
    allowed_domains = ["www.immobiliareilgolfo.it"]
    start_urls = ["https://immobiliareilgolfo.it/immobili.php?motivazione=Affitto,Affitto/Vendita", "https://immobiliareilgolfo.it/immobili.php"]
    position = 1


    def parse(self, response):
        for url in response.css("h1.title a::attr(href)").getall():
            yield Request(response.urljoin(url), callback=self.populate_item, dont_filter = True)

        

    def populate_item(self, response):

        property_type = "apartment"
        title = response.css("h2.uppercase::text").get()
        rent = response.css("h2.price span::text").get()
        if( not re.search("([0-9]+)", rent)):
            return
        currency = "EUR"
        external_id = response.css("span:contains('Riferimento:') strong::text").get()

        page_address = response.css("p.address span::text").get()
        responseGeocode = requests.get(f"https://geocode.arcgis.com/arcgis/rest/services/World/GeocodeServer/find?f=json&text={page_address}&maxLocations=1")
        responseGeocodeData = responseGeocode.json()

        latitude = responseGeocodeData['locations'][0]['feature']['geometry']['y']
        latitude = str(latitude)
        longitude = responseGeocodeData['locations'][0]['feature']['geometry']['x']
        longitude = str(longitude)
    
        responseGeocode = requests.get(f"https://geocode.arcgis.com/arcgis/rest/services/World/GeocodeServer/reverseGeocode?location={longitude},{latitude}&f=pjson&distance=50000&outSR=")
        responseGeocodeData = responseGeocode.json()

        address = responseGeocodeData['address']['Match_addr']
        if(re.search("([0-9]+)", address)):
            address = page_address
        city = responseGeocodeData['address']['City']
        zipcode = responseGeocodeData['address']['Postal']


        room_count = response.css("span:contains('Locali:') + span::text").get()
        bathroom_count = response.css("span:contains('Bagni:') + span::text").get()
        square_meters = response.css("span:contains('Superficie:') + span::text").get()
        description = response.css("div.mdc-card:contains('Descrizione')::text").getall()
        description = " ".join(description)

        images = response.css("div.main-carousel div.swiper-container div.swiper-wrapper div.swiper-slide img.slide-item::attr(data-src)").getall()
        images = [ response.urljoin(image_src) for image_src in images ]

        features = response.css("div.row span::text").getall()
        features = " ".join(features)

        balcony = "Balcon" in features
        parking = "Posti auto" in features


        landlord_name = "immobiliareilgolfo"
        landlord_phone = "0183403141"
        landlord_email = "info@immobiliareilgolfo.it"
        
        item_loader = ListingLoader(response=response)

        item_loader.add_value("external_link", response.url)
        item_loader.add_value("external_source", self.external_source)
        item_loader.add_value("position", self.position)
        self.position += 1
        item_loader.add_value("property_type", property_type)
        item_loader.add_value("title", title)
        item_loader.add_value("rent_string", rent)
        item_loader.add_value("currency", currency)
        item_loader.add_value("external_id", external_id)
        item_loader.add_value("latitude", latitude)
        item_loader.add_value("longitude", longitude)
        item_loader.add_value("address", address)
        item_loader.add_value("city", city)
        item_loader.add_value("zipcode", zipcode)
        item_loader.add_value("room_count", room_count)
        item_loader.add_value("bathroom_count", bathroom_count)
        item_loader.add_value("square_meters", square_meters)
        item_loader.add_value("description", description)
        item_loader.add_value("images", images)
        item_loader.add_value("balcony", balcony)
        item_loader.add_value("parking", parking)
        item_loader.add_value("landlord_name", landlord_name)
        item_loader.add_value("landlord_phone", landlord_phone)
        item_loader.add_value("landlord_email", landlord_email)
       
        yield item_loader.load_item()
