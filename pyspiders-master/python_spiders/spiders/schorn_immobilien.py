# -*- coding: utf-8 -*-
# Author: Abdelrahman-Moharram
import scrapy
from ..loaders import ListingLoader
import re
import dateutil.parser
from python_spiders.helper import remove_white_spaces, get_amenities, extract_location_from_address, extract_location_from_coordinates

class SchornImmobilienSpider(scrapy.Spider):
    name = "schorn_immobilien"
    allowed_domains = ["schorn-immobilien.de"]
    country = 'germany' # Fill in the Country's name
    locale = 'de' # Fill in the Country's locale, look up the docs if unsure
    external_source = "{}_PySpider_{}_{}".format(
        name.capitalize(), country, locale)
    execution_type = 'testing' 

    position = 1

    # 1. SCRAPING level 1
    def start_requests(self):
        start_urls = [            
            {'url': 'https://www.schorn-immobilien.de/vermietung/haeuser.html',
                'property_type': 'house'},
            {'url': 'https://www.schorn-immobilien.de/vermietung/wohnungen.html',
                'property_type': 'apartment'}
            ]
        for url in start_urls:
            yield scrapy.Request(url=url.get('url'), callback=self.parse,  meta={'property_type': url.get('property_type')})

    # 2. SCRAPING level 2
    def parse(self, response, **kwargs):
        for url in response.css(".caption h3 a.hover-effect::attr(href)").getall():
            yield scrapy.Request(url=response.urljoin(url), callback=self.populate_item, meta={'property_type':response.meta.get('property_type')})

    # 3. SCRAPING level 3
    def populate_item(self, response):
        property_type = response.meta['property_type']
        item_loader = ListingLoader(response=response)

        title           = response.css("h1::text").get()
        images          = [response.urljoin(i) for i in response.css("div.item img::attr(src)").getall()]
        description     = remove_white_spaces(" ".join(response.xpath('//h3[contains(text(), "Objektbeschreibung")]/following-sibling::p/text()').getall()))
        utilities       = int(re.findall("[0-9]+\.*[0-9]*", response.xpath('//dt[contains(text(), "Nebenkosten")]/following-sibling::dd/text()').get())[0])
        deposit         = response.xpath('//dt[contains(text(), "Kaution")]/following-sibling::dd/text()').get()
        rent            = response.xpath('//dt[contains(text(), "Kaltmiete")]/following-sibling::dd/text()').get()
        external_id     = response.xpath('//dt[contains(text(), "Objektnr extern:")]/following-sibling::dd/text()').get()
        property_type   = make_property_type(response.xpath('//dt[contains(text(), "Objektart")]/following-sibling::dd/text()').get())
        address         = response.xpath('//dt[contains(text(), "Ort")]/following-sibling::dd/text()').get()
        available_date  = response.xpath('//dt[contains(text(), "Verfügbar ab")]/following-sibling::dd/text()').get()
        square_meters   = int(float(response.xpath('//dt[contains(text(), "Wohnfläche")]/following-sibling::dd/text()').get().replace(",",".").replace("m²","")))
        floor           = response.xpath('//dt[contains(text(), "Etage")]/following-sibling::dd/text()').get()
        room_count      = int(float(response.xpath('//dt[contains(text(), "Zimmer")]/following-sibling::dd/text()').get()))
        bathroom_count  = response.xpath('//dt[contains(text(), "Badezimmer")]/following-sibling::dd/text()').get()



        pets_allowed, furnished, parking, elevator, balcony, terrace, swimming_pool, washing_machine, dishwasher = get_amenities(description, "  *".join(response.css(".default .label.label-default::text").getall()), item_loader)

        if deposit:
            deposit = re.findall("[0-9]+\.*[0-9]*", deposit)
            if deposit:
                deposit = int(deposit[0].replace(".",""))


        longitude, latitude     = extract_location_from_address(address)
        zipcode, city, address  = extract_location_from_coordinates(longitude, latitude)

        if not rent:
            rent        = response.xpath('//dt[contains(text(), "Nettokaltmiete")]/following-sibling::dd/text()').get()
        rent            = int(re.findall("[0-9]+\.*[0-9]*", rent)[0].replace(".",""))

        if 'haus' in description.lower() and not property_type:
            property_type = 'house'

        if bathroom_count:
            bathroom_count = int(float(response.xpath('//dt[contains(text(), "Badezimmer")]/following-sibling::dd/text()').get()))

        if available_date:
            available_date = available_date.lower()
            if 'sofort' in available_date or 'kurzfristig' in available_date or 'nach absprache' in available_date or 'nach vereinbarung' in available_date or 'sofortbezug möglich' in available_date:
                available_date = ''

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
        item_loader.add_value("latitude", latitude) # String
        item_loader.add_value("longitude", longitude) # String
        item_loader.add_value("floor", floor) # String
        item_loader.add_value("property_type", property_type) # String => ["apartment", "house", "room", "student_apartment", "studio"]
        item_loader.add_value("square_meters", square_meters) # Int
        item_loader.add_value("room_count", room_count) # Int
        item_loader.add_value("bathroom_count", bathroom_count) # Int

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
        # item_loader.add_value("prepaid_rent", prepaid_rent) # Int
        item_loader.add_value("utilities", utilities) # Int
        item_loader.add_value("currency", "EUR") # String

        #item_loader.add_value("water_cost", water_cost) # Int
        #item_loader.add_value("heating_cost", heating_cost) # Int

        #item_loader.add_value("energy_label", energy_label) # String

        # # LandLord Details
        item_loader.add_value("landlord_name", 'Schorn & Schorn Immobilien Hennef') # String
        item_loader.add_value("landlord_phone", '02242 90 10 50') # String
        item_loader.add_value("landlord_email", 'info@schorn-immobilien.de') # String

        self.position += 1
        yield item_loader.load_item()


def make_property_type(prop):
    apartments = ['wohnung', 'loft', 'attic', 'dachgeschoss']
    houses     = ['huis','woning','terratetto', 'einfamilienhaus', 'haus']
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
