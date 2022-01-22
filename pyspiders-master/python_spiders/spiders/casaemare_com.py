# -*- coding: utf-8 -*-
# Author: Mohamed Zakaria

import re
import urllib

from scrapy import Spider, Request
from python_spiders.loaders import ListingLoader
from python_spiders.helper import extract_location_from_coordinates

class AssociatedpmSpider(Spider):
    name = 'casaemare_com'
    country='italy'
    locale='it' 
    external_source = "{}_PySpider_{}_{}".format(name.capitalize(), country, locale)
    execution_type='testing'
    allowed_domains = ["www.casaemare.com"]
    start_urls = ["https://casaemare.com/it/affitto"]

    def parse(self, response):
        for url in response.css("a.a_img_box_immo::attr(href)").getall():
            yield Request(response.urljoin(url), callback=self.populate_item, dont_filter=True)
        

    def populate_item(self, response):
        property_type = "apartment"

        title = response.css("h1::text").getall()
        title = " ".join(title).strip()
        title = re.sub("\s+", " ", title)
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

        external_id = re.findall("RIF. (.+)", title)[0]

        rents = response.css("div.costo_prezzo::text").getall()
        joined_rent = " ".join(rents)

        if(joined_rent == "" or joined_rent == " "):
            return
        joined_rent = re.findall("([0-9]+)", joined_rent)
        if(len(joined_rent) == 0):
            return
        
        currency = "EUR"

        address = response.css("p.indirizzo::text").get()
        description = response.css("section.chisiamo_dovesiamo div.row div.col-12 p::text").getall()
        if(len(description) == 0):
            description = response.css("section.chisiamo_dovesiamo div.row div.col-12 p span::text").getall()

        description = " ".join(description)
        description = re.sub("\s+", " ", description)

        square_meters = response.css("div:contains('Metri Quadri') + div::text").get()
        if(square_meters):
            square_meters = re.findall("[0-9]+", square_meters)[0]

        room_count = response.css("div:contains('Numero Vani') + div::text").get()
        bathroom_count = response.css("div:contains('Numero Bagni') + div::text").get()
        energy_label = response.css("div:contains('Classe Energetica') + div::text").get()

        balcony = response.css("div:contains('Balcone') + div::text").get()
        if(balcony):
            balcony = True
        else:
            balcony = False
        
        parking = response.css("div:contains('Posti Auto') + div::text").get()
        if(parking):
            parking = True
        else:
            parking = False
        
        images = response.css("img.immobile-slick-for::attr(src)").getall()
        images = [response.urljoin(urllib.parse.quote(image_src)) for image_src in images]

        floor_plan_images = response.css("div:contains('Planimetria') img::attr(src)").getall()
        floor_plan_images = [response.urljoin(urllib.parse.quote(image_src)) for image_src in floor_plan_images]
        landlord_name = "casaemare"
        landlord_phone = "0565.745061"
        landlord_email = "casaemare@casaemare.com"

        amenities = response.css("div.row_servizi div.sing_servizio::text").getall()
        amenities = " ".join(amenities)
        amenities = amenities.lower()
        dishwasher = "lavastoviglie" in amenities
        washing_machine = "lavatrice" in amenities
        balcony = "baclon" in amenities
        
        terrace = response.css("div.col-8:contains('Terrazza') + div.col-4::text").get()
        if(terrace):
            terrace = True
        else:
            terrace = False

        parking = response.css("div.col-8:contains('auto') + div.col-4::text").get()
        if(parking):
            parking = True
        else: 
            parking = False

        location_script = response.css("script:contains('function initMap()')::text").get()

        latitude = re.findall("center: { lat:(-?[0-9]+\.[0-9]+), lng:-?[0-9]+\.[0-9]+},", location_script)[0]
        longitude = re.findall("center: { lat:-?[0-9]+\.[0-9]+, lng:(-?[0-9]+\.[0-9]+)},", location_script)[0]

        location_data = extract_location_from_coordinates(longitude, latitude)
        zipcode = location_data[0]
        city = location_data[1]
        address = location_data[2]

        property_order = 1


        for rent in joined_rent:
            
            item_loader = ListingLoader(response=response)

            item_loader.add_value("external_link", response.url + f"#{property_order}")
            item_loader.add_value("external_source", self.external_source)
            item_loader.add_value("property_type", property_type)
            item_loader.add_value("title", title)
            item_loader.add_value("external_id", external_id)
            item_loader.add_value("rent_string", rent)
            item_loader.add_value("currency", currency)
            item_loader.add_value("address", address)
            item_loader.add_value("city", city)
            item_loader.add_value("zipcode", zipcode)
            item_loader.add_value("description", description)
            item_loader.add_value("square_meters", square_meters)
            item_loader.add_value("room_count", room_count)
            item_loader.add_value("bathroom_count", bathroom_count)
            item_loader.add_value("energy_label", energy_label)
            item_loader.add_value("balcony", balcony)
            item_loader.add_value("parking", parking)
            item_loader.add_value("images", images)
            item_loader.add_value("floor_plan_images", floor_plan_images)
            item_loader.add_value("landlord_name", landlord_name)
            item_loader.add_value("landlord_phone", landlord_phone)
            item_loader.add_value("landlord_email", landlord_email)
            item_loader.add_value("dishwasher", dishwasher)
            item_loader.add_value("washing_machine", washing_machine)
            item_loader.add_value("balcony", balcony)
            item_loader.add_value("terrace", terrace)
            item_loader.add_value("latitude", latitude)
            item_loader.add_value("longitude", longitude)
            property_order += 1

            yield item_loader.load_item()
