# -*- coding: utf-8 -*-
# Author: Mohamed Zakaria

import re
import requests 

from scrapy import Spider, Request
from python_spiders.loaders import ListingLoader

class OmnicasaSpider(Spider):
    name = 'omnicasa_it'
    country='italy'
    locale='it' 
    external_source = "{}_PySpider_{}_{}".format(name.capitalize(), country, locale)
    execution_type='testing'
    allowed_domains = ["www.omnicasa.it"]
    start_urls = ["https://www.omnicasa.it/it/cerca?sch_categoria%5B%5D=11&sch_contratto=affitto&sch_provincia=&mq_min=&price_max=&sch_camere=&sch_bagni=#result_anchor"]

    def parse(self, response):
        for url in response.css("div.property-main-box a::attr(href)").getall():
            yield Request(response.urljoin(url), callback=self.populate_item)
        
        next_page = response.css("a.pager_link_tail::attr(href)").get()
        if next_page:
            yield response.follow(response.urljoin(next_page), callback=self.parse)

    def populate_item(self, response):
        
        property_type = "apartment"
        title = response.css("h1.annuncio-title::text").get()
        lowerd_title = title.lower()
        if (
            ("commerciale" in lowerd_title) 
            or ("ufficio" in lowerd_title) 
            or ("magazzino" in lowerd_title) 
            or ("box" in lowerd_title) 
            or ("auto" in lowerd_title) 
            or ("negozio" in lowerd_title) 
            or ("vendita" in lowerd_title) ):
            return

        description = response.css("div.single-property-details::text").getall()
        description = " ".join(description)
        description = re.sub("\s+", " ", description)

        rent = response.css("strong:contains('Prezzo') + span::text").get()
        if(not re.search("[0-9]+", rent)):
            return

        square_meters = response.css("strong:contains('Mq') + span::text").get()
        room_count = response.css("strong:contains('Camere') + span::text").get()
        if(room_count):
            if(not re.search("([1-9])", room_count)):
                room_count = "1"
        else:
            room_count = "1"
        bathroom_count = response.css("strong:contains('Bagni') + span::text").get()
        external_id = response.css("strong:contains('Riferimento') + span::text").get()
        images = response.css("img.rsTmb::attr(src)").getall()
        latitude = response.css("span#latitude_hidden::text").get()
        longitude = response.css("span#longitude_hidden::text").get()
        
        amenities = response.css("div.amenities-checkbox label::text").getall()
        amenities = " ".join(amenities)


        elevator = "Ascensore" in amenities
        parking = "Garage" in amenities
        
        energy_label = response.css("strong:contains('Classe energetica') + span").get()
        if(energy_label):
            try:
                energy_label = re.findall('<span><img src="/ui/common_images/ipe_icons/([a-z]).png"></span>', energy_label)[0]
            except:
                energy_label = None

        landlord_name = response.css("div.agent-name h2::text").get()
        landlord_phone = response.css("i.fa-phone + a::text").get()

        responseGeocode = requests.get(f"https://geocode.arcgis.com/arcgis/rest/services/World/GeocodeServer/reverseGeocode?location={longitude},{latitude}&f=pjson&distance=50000&outSR=")
        responseGeocodeData = responseGeocode.json()

        address = responseGeocodeData['address']['Match_addr']
        city = responseGeocodeData['address']['City']
        zipcode = responseGeocodeData['address']['Postal']

        utilities = response.css("label:contains('Spese condomin') + span::text").get()
        balcony = "Poggioli" in amenities or "balcon" in amenities.lower() 
        
        item_loader = ListingLoader(response=response)

        item_loader.add_value("external_link", response.url)
        item_loader.add_value("external_source", self.external_source)
        item_loader.add_value("property_type", property_type)
        item_loader.add_value("title", title)
        item_loader.add_value("description", description)
        item_loader.add_value("rent_string", rent)
        item_loader.add_value("square_meters", square_meters)
        item_loader.add_value("room_count", room_count)
        item_loader.add_value("bathroom_count", bathroom_count)
        item_loader.add_value("external_id", external_id)
        item_loader.add_value("images", images)
        item_loader.add_value("latitude", latitude)
        item_loader.add_value("longitude", longitude)
        item_loader.add_value("elevator", elevator)
        item_loader.add_value("parking", parking)
        item_loader.add_value("energy_label", energy_label)
        item_loader.add_value("landlord_name", landlord_name)
        item_loader.add_value("landlord_phone", landlord_phone)
        item_loader.add_value("address", address)
        item_loader.add_value("city", city)
        item_loader.add_value("zipcode", zipcode)
        item_loader.add_value("utilities", utilities)
        item_loader.add_value("balcony", balcony)
       
        yield item_loader.load_item()
