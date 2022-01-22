# -*- coding: utf-8 -*-
# Author: Abdelrahman-Moharram
import scrapy
import scrapy
import dateutil.parser
from python_spiders.helper import get_amenities, remove_white_spaces
from python_spiders.loaders import ListingLoader

class WgLipsiaSpider(scrapy.Spider):
    name = 'wg_lipsia'
    allowed_domains = ['wg-lipsia.de']
    start_urls = ['https://wg-lipsia.de/vermietung/suche/']
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
        for url in response.css(".house-grid article a::attr(href)").getall():
            yield scrapy.Request(url=url, callback=self.populate_item)
        next_page = response.css(".krd-pagination a.next::attr(href)").get()
        if next_page:
            yield scrapy.Request(url=next_page, callback=self.parse)

    # 3. SCRAPING level 3
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)
        title           = " ".join(response.css(".title ::text").getall()).strip()
        room_count      = int(response.xpath('//div[contains(text(), "Zimmeranzahl")]/following-sibling::div/text()').get())
        square_meters   = int(float(response.xpath('//div[contains(text(), "Wohnfläche")]/following-sibling::div/text()').get().replace("m","").strip().replace(",",".")))
        available_date  = response.xpath('//div[contains(text(), "Verfügbar ab")]/following-sibling::div/text()').get()
        floor           = response.xpath('//div[contains(text(), "Etage")]/following-sibling::div/text()').re("[0-9]+")
        rent            = int(float(response.xpath('//div[contains(text(), "Gesamtmiete")]/following-sibling::div/text()').get().replace("€","").strip().replace(",",".")))
        utilities       = int(float(response.xpath('//div[contains(text(), "Nebenkosten")]/following-sibling::div/text()').get().replace("€","").strip().replace(",",".")))
        landlord_name   = response.css(".krd-teams-name::text").get()
        landlord_phone  = response.css(".krd-teams-phone::text").get()
        landlord_email  = response.css(".team-person-email a::text").get().replace("(at)","@")
        images          = response.css(".krd-gallery-item-image a::attr(href)").getall()
        description     = remove_white_spaces(response.css("#page_description::text").get())
        external_id     = response.css(".moreinfo_item_content::text").get().strip().split(":")[1].strip()
        if floor:
            floor = floor[0]
        if available_date:
            if available_date == 'sofort':
                available_date = ""
            else:
                available_date = dateutil.parser.parse(available_date).strftime("%Y-%m-%d")

        address, city, val = " ".join(response.css(".house-address ::text").getall()).strip().split(",")
        zipcode, city      = city.strip().split()
        pets_allowed, furnished, parking, elevator, balcony, terrace, swimming_pool, washing_machine, dishwasher = get_amenities(description, " ".join(response.css(".equipment_item_content div::text").getall()), item_loader)

        
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

        item_loader.add_value("pets_allowed", pets_allowed) # Boolean
        item_loader.add_value("furnished", furnished) # Boolean
        item_loader.add_value("parking", parking) # Boolean
        item_loader.add_value("elevator", elevator) # Boolean
        item_loader.add_value("balcony", balcony) # Boolean
        item_loader.add_value("terrace", terrace) # Boolean
        item_loader.add_value("swimming_pool", swimming_pool) # Boolean
        item_loader.add_value("washing_machine", washing_machine) # Boolean
        item_loader.add_value("dishwasher", dishwasher) # Boolean

        # # Images
        item_loader.add_value("images", images) # Array
        item_loader.add_value("external_images_count", len(images)) # Int
        #item_loader.add_value("floor_plan_images", floor_plan_images) # Array

        # # Monetary Status
        item_loader.add_value("rent", rent) # Int
        # item_loader.add_value("deposit", deposit) # Int
        #item_loader.add_value("prepaid_rent", prepaid_rent) # Int
        item_loader.add_value("utilities", utilities) # Int
        item_loader.add_value("currency", "EUR") # String

        #item_loader.add_value("water_cost", water_cost) # Int
        #item_loader.add_value("heating_cost", heating_cost) # Int

        # item_loader.add_value("energy_label", energy_label) # String

        # # LandLord Details
        item_loader.add_value("landlord_name", landlord_name) # String
        item_loader.add_value("landlord_phone", landlord_phone) # String
        item_loader.add_value("landlord_email", landlord_email) # String

        self.position += 1
        yield item_loader.load_item()
