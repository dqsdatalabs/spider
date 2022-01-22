# -*- coding: utf-8 -*-
# Author: Ahmed Hegab
import scrapy
from scrapy import Request
from ..loaders import ListingLoader
import json


class immobiliaremenaggio_com_PySpider_italySpider(scrapy.Spider):
    name = "immobiliaremenaggio_com"
    start_urls = ['https://www.immobiliaremenaggio.com/it/status/affitto/']
    allowed_domains = ["immobiliaremenaggio.com"]
    country = 'Italy' # Fill in the Country's name
    locale = 'it' # Fill in the Country's locale, look up the docs if unsure
    external_source = "{}_PySpider_{}_{}".format(
        name.capitalize(), country, locale)
    execution_type = 'testing' 

    position = 1


    def start_requests(self):
        for url in self.start_urls:
            yield scrapy.Request(url, callback=self.parse)


    def parse(self, response, **kwargs):
        urls = response.css(".block_title a::attr(href)").extract()
        area = response.css(".area~ .superficie::text").extract()
        room = response.css(".bedroom~ .superficie::text").extract()
        baths = response.css(".bathroom~ .superficie::text").extract()
        price_all = response.css(".currency::text").extract()
        titles = response.css(".block_title a::text").extract()
        for i in range(len(urls)):
            square_meters = area[i]
            room_count = room[i]
            bathroom_count = baths[i]
            title = titles[i]
            price = price_all[i]

            if 'COMMERCIALE' in title or 'NEGOZIO' in title or 'CAPANNONE' in title:
                pass
            else:
                if 'mensili' in price:
                    yield Request(url = urls[i],
                    callback = self.populate_item,
                    meta={
                        'square_meters':square_meters,
                        'room_count':room_count,
                        'bathroom_count':bathroom_count,
                        'title':title,
                        'rent':price
                    }
                    )

    def populate_item(self, response):
        item_loader = ListingLoader(response=response)
        title = response.meta.get("title")
        room_count = int(response.meta.get("room_count"))
        bathroom_count = int(response.meta.get("bathroom_count"))
        square_meters = response.meta.get("square_meters")
        if 'mq' in square_meters:
            square_meters = int(square_meters.replace('mq','').replace('circa',''))
        else:
            square_meters = int(square_meters)
        if square_meters == 0:
            square_meters = None
        rent = int(response.meta.get("rent").split('Euro')[1].split(',')[0].replace('.',''))

        description = ''
        descriptions = response.css(".descri p *::text").extract()
        for i in range(len(descriptions)):
            description = description + " " + descriptions[i]
        images = response.css(".ns-img::attr(href)").extract()
        external_id = response.css("body > section:nth-child(12) > div > div > div > div.span12 > div.span4 > div > ul > li:nth-child(1) > span::text").get()
        address = response.css("body > section:nth-child(12) > div > div > div > div.span12 > div.span4 > div > ul > li:nth-child(2) > span::text").get()
        energy_label = response.css("body > section:nth-child(12) > div > div > div > div.span12 > div.span4 > div > ul > li:nth-child(8) > span::text").get()
        if 'Classe' in energy_label:
            energy_label = energy_label.split('Classe ')[1]
        else:
            energy_label = None
        info_ = response.css("body > section:nth-child(12) > div > div > div > div.span12 > div.span4 > div > ul > li:nth-child(11) *::text").extract()
        info = ''
        for i in range(len(info_)):
            info = info + " " + info_[i]
        info = info.lower()
        furnished = None
        if 'Arredato' in description or 'arredato' in info:
            furnished = True
        terrace = None
        if 'terrazzino' in description or 'terrazza' in info:
            terrace = True
        parking = None
        if 'garage' in info or 'posto auto' in info or 'posto auto' in description:
            parking = True
        city = response.css("section > div > div > div:nth-child(1) > h5 > a::text").get()

        item_loader.add_value("external_link", response.url) # String
        item_loader.add_value("external_source", self.external_source) # String

        item_loader.add_value("external_id", external_id) # String
        item_loader.add_value("position", self.position) # Int
        item_loader.add_value("title", title) # String
        item_loader.add_value("description", description) # String

        # # Property Details
        item_loader.add_value("city", city) # String
        #item_loader.add_value("zipcode", zipcode) # String
        item_loader.add_value("address", address) # String
        #item_loader.add_value("latitude", latitude) # String
        #item_loader.add_value("longitude", longitude) # String
        #item_loader.add_value("floor", floor) # String
        item_loader.add_value("property_type", "apartment") # String => ["apartment", "house", "room", "student_apartment", "studio"]
        item_loader.add_value("square_meters", square_meters) # Int
        item_loader.add_value("room_count", room_count) # Int
        item_loader.add_value("bathroom_count", bathroom_count) # Int

        #item_loader.add_value("available_date", available_date) # String => date_format

        #item_loader.add_value("pets_allowed", pets_allowed) # Boolean
        item_loader.add_value("furnished", furnished) # Boolean
        item_loader.add_value("parking", parking) # Boolean
        #item_loader.add_value("elevator", elevator) # Boolean
        #item_loader.add_value("balcony", balcony) # Boolean
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

        item_loader.add_value("energy_label", energy_label) # String

        # # LandLord Details
        item_loader.add_value("landlord_name", "immobiliaremenaggio") # String
        item_loader.add_value("landlord_phone", "+39 0344 30181") # String
        item_loader.add_value("landlord_email", "info@immobiliaremenaggio.com") # String

        self.position += 1
        yield item_loader.load_item()
