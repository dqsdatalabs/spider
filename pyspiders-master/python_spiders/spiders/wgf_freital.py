# -*- coding: utf-8 -*-
# Author: Abdelrahman-Moharram
import scrapy
import scrapy
import dateutil.parser
from python_spiders.helper import get_amenities, remove_white_spaces
from python_spiders.loaders import ListingLoader

class WgfFreitalSpider(scrapy.Spider):
    name = 'wgf_freital'
    start_urls = ['https://www.wgf-freital.de/wohnung-mieten-aktuelle-angebote-fuer-wohnungen-zur-miete-in-freital/']
    allowed_domains = ['wgf-freital.de']
    country = 'germany' # Fill in the Country's name
    locale = 'de' # Fill in the Country's locale, look up the docs if unsure
    external_source = "{}_PySpider_{}_{}".format(
        name.capitalize(), country, locale)
    execution_type = 'testing' 

    position = 1

    # 1. SCRAPING level 1
    def start_requests(self):
        for url in self.start_urls:
            yield scrapy.Request(url, callback=self.parse)

    # 2. SCRAPING level 2
    def parse(self, response, **kwargs):
        for url in set(response.css(".tile__container article a::attr(href)").getall()):
            yield scrapy.Request(url=response.urljoin(url), callback=self.populate_item)

    # 3. SCRAPING level 3
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)
        title           = response.css("h1::text").get().strip()
        landlord_name   = remove_white_spaces(" ".join(response.css(".person .details strong ::text").getall()))
        landlord_email  = response.css(".person .details .email a::text").get()
        landlord_phone  = response.css(".person .details .phone a::text").get()
        images          = response.css(".swiper-slide figure a::attr(href)").getall()
        description     = remove_white_spaces(" ".join(response.css(".description .tab__container p::text").getall()))
        external_id     = response.xpath('//strong[contains(text(), "Objekt-ID")]/following-sibling::span/text()').get()
        address         = response.xpath('//strong[contains(text(), "Adresse")]/following-sibling::span/text()').getall()
        address, city   = address[0], address[1]        
        zipcode, city   = city.strip().split()
        available_date  = response.xpath('//strong[contains(text(), "Frei ab")]/following-sibling::span/text()').get()
        square_meters   = round(float(response.xpath('//strong[contains(text(), "Wohnfläche")]/following-sibling::span/text()').get().replace("m²","").replace(",",".").strip()))
        room_count      = int(response.xpath('//strong[contains(text(), "Zimmer")]/following-sibling::span/text()').get())
        floor           = response.xpath('//strong[contains(text(), "Etage")]/following-sibling::span/text()').re("[0-9]+")
        utilities       = int(float(response.xpath('//strong[contains(text(), "Nebenkosten")]/following-sibling::span/text()').get().replace("€","").strip().replace(",",".")))
        rent            = int(float(response.xpath('//strong[contains(text(), "Gesamtmiete")]/following-sibling::span/text()').get().replace("€","").strip().replace(",",".")))
        deposit         = int(float(response.xpath('//strong[contains(text(), "Kaution")]/following-sibling::span/text()').get().replace("€","").strip().replace(",",".")))
        energy_label    = response.xpath('//strong[contains(text(), "Energieeffizienzklasse")]/following-sibling::span/text()').get()
        longitude       = response.css("div#map-canvas::attr(data-longitude)").get()
        latitude        = response.css("div#map-canvas::attr(data-latitude)").get()
        if floor:
            floor = floor[0]

        if available_date:
            if available_date == 'sofort':
                available_date = ""
            else:
                available_date = dateutil.parser.parse(available_date).strftime("%Y-%m-%d")
        
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
        #item_loader.add_value("latitude", latitude) # String
        #item_loader.add_value("longitude", longitude) # String
        item_loader.add_value("floor", floor) # String
        item_loader.add_value("property_type", 'apartment') # String => ["apartment", "house", "room", "student_apartment", "studio"]
        item_loader.add_value("square_meters", square_meters) # Int
        item_loader.add_value("room_count", room_count) # Int
        #item_loader.add_value("bathroom_count", bathroom_count) # Int

        item_loader.add_value("available_date", available_date) # String => date_format

        #item_loader.add_value("pets_allowed", pets_allowed) # Boolean
        #item_loader.add_value("furnished", furnished) # Boolean
        #item_loader.add_value("parking", parking) # Boolean
        #item_loader.add_value("elevator", elevator) # Boolean
        #item_loader.add_value("balcony", balcony) # Boolean
        #item_loader.add_value("terrace", terrace) # Boolean
        #item_loader.add_value("swimming_pool", swimming_pool) # Boolean
        #item_loader.add_value("washing_machine", washing_machine) # Boolean
        #item_loader.add_value("dishwasher", dishwasher) # Boolean

        # # Images
        item_loader.add_value("images", images) # Array
        item_loader.add_value("external_images_count", len(images)) # Int
        #item_loader.add_value("floor_plan_images", floor_plan_images) # Array

        # # Monetary Status
        item_loader.add_value("rent", rent) # Int
        item_loader.add_value("deposit", deposit) # Int
        #item_loader.add_value("prepaid_rent", prepaid_rent) # Int
        item_loader.add_value("utilities", utilities) # Int
        item_loader.add_value("currency", "EUR") # String

        #item_loader.add_value("water_cost", water_cost) # Int
        #item_loader.add_value("heating_cost", heating_cost) # Int

        item_loader.add_value("energy_label", energy_label) # String

        # # LandLord Details
        item_loader.add_value("landlord_name", landlord_name) # String
        item_loader.add_value("landlord_phone", landlord_phone) # String
        item_loader.add_value("landlord_email", landlord_email) # String

        self.position += 1
        yield item_loader.load_item()
