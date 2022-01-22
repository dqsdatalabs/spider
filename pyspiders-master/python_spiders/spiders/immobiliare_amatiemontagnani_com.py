# -*- coding: utf-8 -*-
# Author: Mohamed Zakaria

import re
import requests

from scrapy import Spider, Request
from python_spiders.loaders import ListingLoader

class Immobiliare_amatiemontagnaniSpider(Spider):
    name = 'immobiliare_amatiemontagnani_com'
    country='italy'
    locale='it' 
    external_source = "{}_PySpider_{}_{}".format(name.capitalize(), country, locale)
    execution_type='testing'
    allowed_domains = ["www.immobiliare.amatiemontagnani.com"]
    start_urls = ["https://immobiliare.amatiemontagnani.com/search-results-page/?status%5B%5D=affitto&type%5B%5D=residenziale&keyword="]

    def parse(self, response):
        for url in response.css("h2.item-title a::attr(href)").getall():
            yield Request(response.urljoin(url), callback=self.populate_item, dont_filter = True)
        

    def populate_item(self, response):

        property_type = "apartment"
        
        title = response.css("div.page-title h1::text").get()
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

        rent = response.css("li.item-price::text").get()
        if(rent == None):
            return
        rent = re.findall("([0-9]+)", rent)
        if(len(rent) == 0):
            return
        rent = "".join(rent)
        currency = "EUR"

        description = response.css("div.block-content-wrap p::text").get()
        external_id = response.css("div:contains('Rif')::text").getall()
        external_id = " ".join(external_id)
        external_id = re.sub("\s+", " ", external_id)
        external_id = re.findall("([0-9]+)", external_id)
        if(len(external_id) > 0):
            external_id = external_id[0]
        else: 
            external_id = None
        
        room_count = response.css("div:contains('Van')::text").getall()
        room_count = " ".join(room_count)
        room_count = re.sub("\s+", " ", room_count)
        room_count = re.findall("([0-9])", room_count)
        if(len(room_count) > 0):
            room_count = room_count[0]
        else: 
            room_count = None
        
        bathroom_count = response.css("div:contains('Serviz')::text").getall()
        bathroom_count = " ".join(bathroom_count)
        bathroom_count = re.sub("\s+", " ", bathroom_count)
        bathroom_count = re.findall("([0-9])", bathroom_count)
        if(len(bathroom_count) > 0):
            bathroom_count = bathroom_count[0]
        else: 
            bathroom_count = None

        square_meters = response.css("div:contains('Mq.')::text").getall()
        square_meters = " ".join(square_meters)
        square_meters = re.sub("\s+", " ", square_meters)
        square_meters = re.findall("([0-9]+)", square_meters)
        if(len(square_meters) > 0):
            square_meters = square_meters[0]
        else: 
            square_meters = None


        utilities = response.css("div:contains('Condo:')::text").getall()
        utilities = " ".join(utilities)
        utilities = re.sub("\s+", " ", utilities)
        utilities = re.findall("([0-9]+)", utilities)
        if(len(utilities) > 0):
            utilities = utilities[0]
        else: 
            utilities = None

        images = response.css("div.col-md-3 a.houzez-trigger-popup-slider-js img.img-fluid::attr(src)").getall()

        images = [ re.sub("-592x444", "", image_src) for image_src in images]
        energy_label = response.css("strong:contains('Classe Energetica:') + span::text").get()

        location_script = response.css("script#houzez-single-property-map-js-extra::text").get()
        latitude = re.findall('"lat":"(-?[0-9]+\.[0-9]+)"', location_script)[0]
        longitude = re.findall('"lng":"(-?[0-9]+\.[0-9]+)"', location_script)[0]

        landlord_name = "amatiemontagnani"
        landlord_phone = "0692949482"
        landlord_email = "sviluppo@amatiemontagnani.com"

        responseGeocode = requests.get(f"https://geocode.arcgis.com/arcgis/rest/services/World/GeocodeServer/reverseGeocode?location={longitude},{latitude}&f=pjson&distance=50000&outSR=")
        responseGeocodeData = responseGeocode.json()
        address = responseGeocodeData['address']['Match_addr']
        city = responseGeocodeData['address']['City']
        zipcode = responseGeocodeData['address']['Postal']

        terrace = "terrazza" in description.lower()
        balcony = "balcon" in description.lower()
        elevator = "ascensore" in description.lower() 
        washing_machine = "lavanderia" in description.lower()
        parking = "posto auto" in description.lower()

        floor_plan_images = response.css("div.floor-plan-left-wrap a::attr(href)").get()

        item_loader = ListingLoader(response=response)

        item_loader.add_value("external_link", response.url)
        item_loader.add_value("external_source", self.external_source)
        item_loader.add_value("property_type", property_type)
        item_loader.add_value("title", title)
        item_loader.add_value("rent_string", rent)
        item_loader.add_value("currency", currency)
        item_loader.add_value("description", description)
        item_loader.add_value("external_id", external_id)
        item_loader.add_value("room_count", room_count)
        item_loader.add_value("bathroom_count", bathroom_count)
        item_loader.add_value("square_meters", square_meters)
        item_loader.add_value("utilities", utilities)
        item_loader.add_value("images", images)
        item_loader.add_value("energy_label", energy_label)
        item_loader.add_value("latitude", latitude)
        item_loader.add_value("longitude", longitude)
        item_loader.add_value("landlord_name", landlord_name)
        item_loader.add_value("landlord_phone", landlord_phone)
        item_loader.add_value("landlord_email", landlord_email)
        item_loader.add_value("address", address)
        item_loader.add_value("city", city)
        item_loader.add_value("zipcode", zipcode)
        item_loader.add_value("terrace", terrace)
        item_loader.add_value("balcony", balcony)
        item_loader.add_value("elevator", elevator)
        item_loader.add_value("washing_machine", washing_machine)
        item_loader.add_value("parking", parking)
       
        yield item_loader.load_item()
