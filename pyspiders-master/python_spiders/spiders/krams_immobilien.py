# -*- coding: utf-8 -*-
# Author: Abdelrahman-Moharram
import scrapy
import dateutil.parser
from python_spiders.helper import remove_white_spaces, get_amenities, extract_location_from_address, extract_location_from_coordinates
from python_spiders.loaders import ListingLoader

class KramsImmobilienSpider(scrapy.Spider):
    name = "krams_immobilien"
    start_urls = ['https://www.krams-immobilien.de/immobilienangebote/mietobjekte/haeuser-wohnungen2']
    allowed_domains = ["krams-immobilien.de"]
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
        for url in response.css(".object_button a::attr(href)").getall():
            yield scrapy.Request(url=response.urljoin(url), callback=self.populate_item)


    # 3. SCRAPING level 3
    def populate_item(self, response):
        item_loader     = ListingLoader(response=response)
        title           = response.css("h2 span.title::text").get()
        images          = [response.urljoin(i) for i in  response.css(".object_images div a::attr(href)").getall()]
        zipcode         = response.css(".fieldValue .postal::text").get()
        address         = response.css(".fieldValue .region::text").get()
        property_type   = make_property_type(response.xpath('//div[contains(text(), "Objektart")]/following-sibling::div/text()').get())
        available_date  = response.xpath('//div[contains(text(), "Frei ab")]/following-sibling::div/text()').get()
        square_meters   = int(float(response.xpath('//div[contains(text(), "Wohnfläche")]/following-sibling::div/text()').get().replace("m²","").replace("ca.","").replace(",",".").strip()))
        room_count      = int(float(response.xpath('//div[contains(text(), "Zimmer")]/following-sibling::div/text()').get().replace("½","").strip()))
        rent            = int(response.xpath('//div[contains(text(), "Kaltmiete")]/following-sibling::div/text()').re("[0-9]+\.*[0-9]*")[0].replace(".",""))
        utilities       = int(response.xpath('//div[contains(text(), "Nebenkosten")]/following-sibling::div/text()').re("[0-9]+\.*[0-9]*")[0].replace(".",""))
        deposit         = response.xpath('//div[contains(text(), "Kaution")]/following-sibling::div/text()').re("[0-9]+\.*[0-9]*")
        description     = remove_white_spaces(" ".join(response.xpath('//div[contains(text(), "Beschreibung")]/following-sibling::div').css(" ::text").getall()))
        landlord_name   = remove_white_spaces(" ".join(response.xpath('//div[contains(text(), "Ansprechpartner")]/following-sibling::div').css(" ::text").getall()))
        landlord_phone  = response.xpath('//div[contains(text(), "Telefonnummer")]/following-sibling::div/a').css("::text").get().strip()
        landlord_email  = response.xpath('//div[contains(text(), "E-Mail-Adresse")]/following-sibling::div/a').css("::text").get().strip()
        energy_label    = response.xpath('//div[contains(text(), "Energieeffizenzklasse")]/following-sibling::div').css("::text").get()
        if available_date:
            if 'sofort' in available_date:
                available_date = ''
            else:
                available_date = dateutil.parser.parse(available_date).strftime("%Y-%m-%d")
        pets_allowed, furnished, parking, elevator, balcony, terrace, swimming_pool, washing_machine, dishwasher = get_amenities(description, " ".join(response.xpath('//div[contains(text(), "Ausstattung")]/following-sibling::div').css(" ::text").getall()), item_loader)

        if deposit:
            deposit = int(deposit[0].replace(".",""))
        if not property_type:
            property_type   = make_property_type(title)

        try:
            longitude, latitude     = extract_location_from_address(address)
            zipcode, city, address  = extract_location_from_coordinates(longitude, latitude)
            
            item_loader.add_value("city", city) # String
            item_loader.add_value("zipcode", zipcode) # String
            item_loader.add_value("latitude", latitude) # String
            item_loader.add_value("longitude", longitude) # String
        except:
            pass

        item_loader.add_value("external_link", response.url) # String
        item_loader.add_value("external_source", self.external_source) # String

        #item_loader.add_value("external_id", external_id) # String
        item_loader.add_value("position", self.position) # Int
        item_loader.add_value("title", title) # String
        item_loader.add_value("description", description) # String

        # # Property Details
        item_loader.add_value("address", address) # String
        #item_loader.add_value("floor", floor) # String
        item_loader.add_value("property_type", property_type) # String => ["apartment", "house", "room", "student_apartment", "studio"]
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
        item_loader.add_value("landlord_name", landlord_name) # String
        item_loader.add_value("landlord_phone", landlord_phone) # String
        item_loader.add_value("landlord_email", landlord_email) # String

        self.position += 1
        yield item_loader.load_item()
def make_property_type(prop):
    apartments = ['wohnung', 'loft', 'attic', 'dachgeschoss']
    houses     = ['huis','woning','terratetto', 'einfamilienhaus']
    if not prop:
        return ""
    prop = prop.lower()
    for apartment in apartments:
        if apartment in prop:
            return 'apartment'
    for house in houses:
        if house in prop:
            return 'house'
    return prop
