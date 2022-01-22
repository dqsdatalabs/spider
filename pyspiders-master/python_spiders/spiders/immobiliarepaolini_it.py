# -*- coding: utf-8 -*-
# Author: Mohamed Zakaria

import re
import requests

from scrapy import Spider, Request
from python_spiders.loaders import ListingLoader

class ImmobiliarepaoliniSpider(Spider):
    name = 'immobiliarepaolini_it'
    country='italy'
    locale='it' 
    external_source = "{}_PySpider_{}_{}".format(name.capitalize(), country, locale)
    execution_type='testing'
    allowed_domains = ["www.immobiliarepaolini.it"]
    start_urls = ["https://www.immobiliarepaolini.it/affitti/appartamento/?id_immobile=&posti_letto=&superficie="]

    def parse(self, response):
        for url in response.css("select#pagine option::attr(value)").getall():
            yield Request(response.urljoin(url), callback=self.get_pages, dont_filter = True)

    def get_pages(self, response):
        for url in response.css("div.ant-image a::attr(href)").getall():
            yield Request(response.urljoin(url), callback=self.populate_item, dont_filter = True)


    def populate_item(self, response):
        
        property_type = "apartment"

        title = response.css('h2.title-scheda2::text').get()
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

        characteristics = response.css("div.caratteristica::text").getall()
        characteristics = " ".join(characteristics)
        characteristics = re.sub("\s+", " ", characteristics)

        parking = "PostoAuto" in characteristics

        characteristics = characteristics.split(" ")

        external_id = characteristics[1]
        room_count = characteristics[2]
        bathroom_count = characteristics[3]
        square_meters = characteristics[4]

        energy_label = response.css("p:contains('Classe Energetica:')::text").get()
        energy_label = re.findall("Classe Energetica: ([A-Z])", energy_label)[0]

        description = response.css("div.desc-frontoffice p span span::text").get()

        rent = response.css("div:contains('Prezzo') + p::text").get()
        rent = re.findall("([0-9]+)", rent)[0]
        currency = "EUR"

        images = response.css("a.lightbox-image::attr(href)").getall()

        images = [response.urljoin(image_src) for image_src in images]

        location_link = response.css("a#mostra-pos::attr(href)").get()
        latitude = None
        longitude = None

        if(location_link):
            latitude = re.findall("lat=(-?[0-9]+\.[0-9]+)", location_link)[0]
            longitude = re.findall("lon=(-?[0-9]+\.[0-9]+)", location_link)[0]

        landlord_name = "immobiliarepaolini"
        landlord_phone = "+39 0585 807557"
        landlord_email = "info@immobiliarepaolini.it"

        balcony = "balcon" in description.lower()
        terrace = "terrazza" in description.lower()
        elevator = "ascensore" in description.lower()

        responseGeocode = requests.get(f"https://geocode.arcgis.com/arcgis/rest/services/World/GeocodeServer/reverseGeocode?location={longitude},{latitude}&f=pjson&distance=50000&outSR=")
        responseGeocodeData = responseGeocode.json()

        city = None
        zipcode = None
        try:
            address = responseGeocodeData['address']['Match_addr']
            city = responseGeocodeData['address']['City']
            zipcode = responseGeocodeData['address']['Postal']
        except:
            address = title.split("in Affitto a")[1]

        item_loader = ListingLoader(response=response)

        item_loader.add_value("external_link", response.url)
        item_loader.add_value("external_source", self.external_source)
        item_loader.add_value("property_type", property_type)
        item_loader.add_value("title", title)
        item_loader.add_value("parking", parking)
        item_loader.add_value("external_id", external_id)
        item_loader.add_value("room_count", room_count)
        item_loader.add_value("bathroom_count", bathroom_count)
        item_loader.add_value("square_meters", square_meters)
        item_loader.add_value("energy_label", energy_label)
        item_loader.add_value("description", description)
        item_loader.add_value("rent_string", rent)
        item_loader.add_value("currency", currency)
        item_loader.add_value("images", images)
        item_loader.add_value("latitude", latitude)
        item_loader.add_value("longitude", longitude)
        item_loader.add_value("landlord_name", landlord_name)
        item_loader.add_value("landlord_phone", landlord_phone)
        item_loader.add_value("landlord_email", landlord_email)
        item_loader.add_value("address", address)
        item_loader.add_value("city", city)
        item_loader.add_value("zipcode", zipcode)
        item_loader.add_value("balcony", balcony)
        item_loader.add_value("terrace", terrace)
        item_loader.add_value("elevator", elevator)
       
        yield item_loader.load_item()
