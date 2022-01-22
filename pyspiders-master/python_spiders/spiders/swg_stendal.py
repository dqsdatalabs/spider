# -*- coding: utf-8 -*-
# Author: Abdelrahman-Moharram
import scrapy
import scrapy
import dateutil.parser
from python_spiders.helper import get_amenities, remove_white_spaces
from python_spiders.loaders import ListingLoader

class SwgStendalSpider(scrapy.Spider):
    name = 'swg_stendal'
    allowed_domains = ['swg-stendal.de']
    start_urls = ['https://www.swg-stendal.de/wohnungsangebote/']
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
        for url in response.css(".show a::attr(href)").getall():
            yield scrapy.Request(url=response.urljoin(url), callback=self.populate_item)

    # 3. SCRAPING level 3
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)
        external_id     = response.css(".btn-request::attr(data-object)").get()
        address         = response.xpath('//div[contains(@class, "title-location")]/following-sibling::div/text()').get()
        title           = response.css(".cboxElement::attr(title)").get()
        room_count      = int(response.xpath('//div[contains(@class, "title-rooms")]/following-sibling::div/text()').re("[0-9]+")[0])
        square_meters   = round(float(response.xpath('//div[contains(@class, "title-space")]/following-sibling::div/text()').get().replace("m²","").replace(",",".").strip()))
        available_date  = response.xpath('//div[contains(@class, "title-rentdate")]/following-sibling::div/text()').get()
        rent            = int(float(response.xpath('//div[contains(@class, "title-rent-all")]/following-sibling::div/text()').get().replace("€","").replace(",",".").strip()))
        deposit         = int(float(response.xpath('//div[contains(@class, "title-deposit")]/following-sibling::div/text()').get().replace("€","").replace(",",".").strip()))
        utilities       = int(float(response.xpath('//div[contains(@class, "title-charges")]/following-sibling::div/text()').get().replace("€","").replace(",",".").strip()))
        energy_label    = response.xpath('//div[contains(@class, "title-energyconsumption")]/following-sibling::div/text()').get()
        images          = response.css(".col-md-4 a::attr(href)").re(".*\.jpg|png|svg")
        if available_date:
            available_date = dateutil.parser.parse(available_date).strftime("%Y-%m-%d")
        pets_allowed, furnished, parking, elevator, balcony, terrace, swimming_pool, washing_machine, dishwasher = get_amenities(" ", " ".join(response.xpath('//div[contains(@class, "environmet")]/span/text()').getall()), item_loader)
        
        
        item_loader.add_value("external_link", response.url) # String
        item_loader.add_value("external_source", self.external_source) # String

        item_loader.add_value("external_id", external_id) # String
        item_loader.add_value("position", self.position) # Int
        item_loader.add_value("title", title) # String
        # item_loader.add_value("description", description) # String

        # # Property Details
        item_loader.add_value("city", "roma") # String
        # item_loader.add_value("zipcode", zipcode) # String
        item_loader.add_value("address", address) # String
        #item_loader.add_value("latitude", latitude) # String
        #item_loader.add_value("longitude", longitude) # String
        # item_loader.add_value("floor", floor) # String
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
        item_loader.add_value("deposit", deposit) # Int
        #item_loader.add_value("prepaid_rent", prepaid_rent) # Int
        item_loader.add_value("utilities", utilities) # Int
        item_loader.add_value("currency", "EUR") # String

        #item_loader.add_value("water_cost", water_cost) # Int
        #item_loader.add_value("heating_cost", heating_cost) # Int

        item_loader.add_value("energy_label", energy_label) # String

        # # LandLord Details
        item_loader.add_value("landlord_name", '03931-634500') # String
        item_loader.add_value("landlord_phone", 'info@swg-stendal.de') # String
        item_loader.add_value("landlord_email", 'Stendaler Wohnungsbaugesellschaft mbH') # String

        self.position += 1
        yield item_loader.load_item()
