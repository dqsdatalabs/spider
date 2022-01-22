# -*- coding: utf-8 -*-
# Author: Ahmed Hegab
import scrapy
from scrapy import Request
from ..loaders import ListingLoader
import json
import requests


class effecimmobiliare_com_PySpider_italySpider(scrapy.Spider):
    name = "effecimmobiliare_com"
    page_number = 2
    start_urls = ['https://www.effecimmobiliare.com/in-affitto/?pg=1']
    allowed_domains = ["effecimmobiliare.com"]
    country = 'Italy' 
    locale = 'it' 
    external_source = "{}_PySpider_{}_{}".format(
        name.capitalize(), country, locale)
    execution_type = 'testing' 

    position = 1



    def start_requests(self):
        for url in self.start_urls:
            yield scrapy.Request(url, callback=self.parse)



    def parse(self, response, **kwargs):
        urls = response.css("#immobili-elenco > div.immobili-elenco-row.spalla-sx > div > p > span.details.top > span.detail-go > a::attr(href)").extract()
        affito = response.css(".contract span::text").extract()
        price = response.css("#immobili-elenco > div.immobili-elenco-row.spalla-sx > div > a > h2.price > span *::text").extract()
        ids = response.css("#immobili-elenco > div.immobili-elenco-row.spalla-sx > div > p > span.details.top > span.rif::text").extract()
        location = response.css("#immobili-elenco > div.immobili-elenco-row.spalla-sx > div > p > span.luogo::text").extract()
        titles = response.css("#immobili-elenco > div.immobili-elenco-row.spalla-sx > div > p > span.titolo-immobile::text").extract()
        counter = 2
        for i in range(len(urls)):
            availability = affito[i]
            rent = price[i]
            external_id = ids[i].replace('Rif.','')
            title = titles[i]
            address = location[i]
            square_meters = None
            square_meters = response.css("#immobili-elenco > div.immobili-elenco-row.spalla-sx > div:nth-child("+str(counter)+") > p > span:nth-child(3) > span.size::text").get()
            room_count = None
            bathroom_count = None
            try:
                room_count = response.css("#immobili-elenco > div.immobili-elenco-row.spalla-sx > div:nth-child("+str(counter)+") > p > span:nth-child(3) > span.rooms::text").get()
            except:
                pass
            try:
                bathroom_count = response.css("#immobili-elenco > div.immobili-elenco-row.spalla-sx > div:nth-child("+str(counter)+") > p > span:nth-child(3) > span.wc::text").get()
            except:
                pass
            counter = counter + 1
            if availability == 'affitto':
                if "€" in rent:
                    if 'centro direzionale' not in title and 'Capannone' not in title and 'Ampia metrature' not in title and 'centro storico' not in title:
                        yield Request(url = urls[i],
                        callback = self.populate_item,
                        meta={
                           'rent':rent,
                           'room_count':room_count,
                           'bathroom_count':bathroom_count,
                           'square_meters':square_meters,
                           'address':address,
                           'title':title,
                           'external_id':external_id 
                        })
        counter = 2
        next_page = ("https://www.effecimmobiliare.com/in-affitto/?pg="+ str(effecimmobiliare_com_PySpider_italySpider.page_number))
        if effecimmobiliare_com_PySpider_italySpider.page_number <= 5:
            effecimmobiliare_com_PySpider_italySpider.page_number += 1
            yield response.follow(next_page, callback=self.parse)


    def populate_item(self, response):
        item_loader = ListingLoader(response=response)
        
        title = response.meta.get("title")
        external_id = response.meta.get("external_id")
        address = response.meta.get("address")
        room_count = int(response.meta.get("room_count").split(' ')[1])
        bathroom_count = int(response.meta.get("bathroom_count").split('bagno')[0])
        square_meters = int(response.meta.get("square_meters").split('mq.')[0])
        rent = response.meta.get("rent").split('€')[1]
        if '.' in rent:
            rent = int(rent.replace('.',''))
        else:
            rent = int(rent)

        description = response.css("#content1 > p::text").get()

        latlng = response.css("#tab-map > script:nth-child(2)").get()

        images = response.css(".sl::attr(src)").extract()
        latlng = latlng.split("LatLng(")[1].split(')')[0]
        latitude = latlng.split(',')[0]
        longitude = latlng.split(',')[1]
        responseGeocode = requests.get(f"https://geocode.arcgis.com/arcgis/rest/services/World/GeocodeServer/reverseGeocode?location={longitude},{latitude}&f=pjson&distance=50000&outSR=")
        responseGeocodeData = responseGeocode.json()
        zipcode = responseGeocodeData['address']['Postal']
        city = responseGeocodeData['address']['City']
        extra_info = response.css("#content2 > div:nth-child(5)").get()
        balcony = None
        terrace = None
        elevator = None
        floor = response.css("#content2 > div:nth-child(1) > ul > li:nth-child(3) > b::text").get()
        if 'Balcone/i' in extra_info:
            balcony = True
        if 'Ascensore' in extra_info:
            elevator = True
        if 'Terrazzo/i' in extra_info:
            terrace = True


        item_loader.add_value("external_link", response.url) # String
        item_loader.add_value("external_source", self.external_source) # String

        item_loader.add_value("external_id", external_id) # String
        item_loader.add_value("position", self.position) # Int
        item_loader.add_value("title", title) # String
        item_loader.add_value("description", description) # String

        # # Property Details
        item_loader.add_value("city", city) # String
        item_loader.add_value("zipcode", zipcode) # String
        item_loader.add_value("address", address) # String
        item_loader.add_value("latitude", latitude) # String
        item_loader.add_value("longitude", longitude) # String
        item_loader.add_value("floor", floor) # String
        item_loader.add_value("property_type", "apartment") # String => ["apartment", "house", "room", "student_apartment", "studio"]
        item_loader.add_value("square_meters", square_meters) # Int
        item_loader.add_value("room_count", room_count) # Int
        item_loader.add_value("bathroom_count", bathroom_count) # Int

        #item_loader.add_value("available_date", available_date) # String => date_format

        #item_loader.add_value("pets_allowed", pets_allowed) # Boolean
        #item_loader.add_value("furnished", furnished) # Boolean
        #item_loader.add_value("parking", parking) # Boolean
        item_loader.add_value("elevator", elevator) # Boolean
        item_loader.add_value("balcony", balcony) # Boolean
        item_loader.add_value("terrace", terrace) # Boolean
        #item_loader.add_value("swimming_pool", swimming_pool) # Boolean
        #item_loader.add_value("washing_machine", washing_machine) # Boolean
        #item_loader.add_value("dishwasher", dishwasher) # Boolean

        # # Images
        item_loader.add_value("images", images) # Array
        item_loader.add_value("external_images_count", len(images)) # Int
        #item_loader.add_value("floor_plan_images", floor_plan_images) # Array

        # # Monetary Status
        item_loader.add_value("rent", rent) # Int
        #item_loader.add_value("deposit", deposit) # Int
        #item_loader.add_value("prepaid_rent", prepaid_rent) # Int
        #item_loader.add_value("utilities", utilities) # Int
        item_loader.add_value("currency", "EUR") # String

        #item_loader.add_value("water_cost", water_cost) # Int
        #item_loader.add_value("heating_cost", heating_cost) # Int

        #item_loader.add_value("energy_label", energy_label) # String

        # # LandLord Details
        item_loader.add_value("landlord_name", "effecimmobiliare") # String
        item_loader.add_value("landlord_phone", "0185.459214") # String
        item_loader.add_value("landlord_email", "info@effecimmobiliare.com") # String

        self.position += 1
        yield item_loader.load_item()
