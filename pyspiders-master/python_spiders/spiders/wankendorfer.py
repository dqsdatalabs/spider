# -*- coding: utf-8 -*-
# Author: Abdelrahman-Moharram
import scrapy
from ..loaders import ListingLoader
import dateutil.parser
from python_spiders.helper import remove_white_spaces, get_amenities, extract_location_from_coordinates

class WankendorferSpider(scrapy.Spider):
    name = "wankendorfer"
    start_urls = ['https://www.wankendorfer.de/immobiliensuche/']
    allowed_domains = ["wankendorfer.de"]
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
        for property in response.css(".GridCol3Even-col"):
            title = property.css("h4::text").get()
            if title:
                if 'stellplatz' not in title.lower():
                    yield scrapy.Request(url=response.urljoin(property.css(".Base-figure a::attr(href)").get()), callback=self.populate_item)
            

    # 3. SCRAPING level 3
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        external_id     = response.xpath('//strong[contains(text(), "Objekt-Nr.")]/following-sibling::text()').get().strip()
        title           = response.css("h1::text").get()
        address         = response.css(".ImmoObject-address p::text").get()
        description     = remove_white_spaces(response.xpath('//h3[contains(text(), "Objektbeschreibung")]/following-sibling::div/div/p/text()').get())
        latitude        = response.css("div.LocationMap::attr(data-locationmap)").re('latitude":"[0-9]+\.[0-9]+')[0].replace('latitude":"',"")
        longitude       = response.css("div.LocationMap::attr(data-locationmap)").re('longitude":"[0-9]+\.[0-9]+')[0].replace('longitude":"',"")
        property_type   = make_property_type(response.xpath('//strong[contains(text(), "Typ")]/following-sibling::span/text()').get())
        floor           = response.xpath('//strong[contains(text(), "Etage")]/following-sibling::span/text()').get()
        room_count      = response.xpath('//strong[contains(text(), "Zimmer")]/following-sibling::span/text()').get()
        square_meters   = response.xpath('//strong[contains(text(), "Wohnfläche")]/following-sibling::span/text()').get()
        available_date  = response.xpath('//strong[contains(text(), "Verfügbar ab")]/following-sibling::span/text()').get().strip()
        rent            = int(float(response.xpath('//strong[contains(text(), "Gesamtmiete ")]/following-sibling::span/text()').get().strip().replace("+","").replace(",", ".").replace("€", "").strip()))
        utilities       = int(float(response.xpath('//strong[contains(text(), "Nebenkosten")]/following-sibling::span/text()').get().strip().replace("+","").replace(",", ".").replace("€", "").strip()))
        deposit         = int(response.xpath('//strong[contains(text(), "Kaution")]/following-sibling::span/text()').re("[0-9]+\.*[0-9]*")[0].replace(".",""))
        energy_label    = response.xpath('//strong[contains(text(), "Wertklasse")]/following-sibling::span/text()').get()
        images          = [ response.urljoin(i) for i in response.css(".js-lightbox::attr(href)").getall()]
        floor_plan_images  = [ response.urljoin(i) for i in  response.xpath('//h3[contains(text(), "Grundriss")]/following-sibling::div/a/@href').getall()]
        
        pets_allowed, furnished, parking, elevator, balcony, terrace, swimming_pool, washing_machine, dishwasher = get_amenities(description, "  *".join(response.xpath('//h3[contains(text(), "Ausstattung")]/following-sibling::ul/li/strong/text()').getall()), item_loader)
        zipcode, city, address = "","",""

        city   = response.css(".ImmoObject-address p::text").getall()[1].split(" ")
        if len(city) > 2:
            zipcode, city = city[0], city[-1]
        if energy_label:
            energy_label = energy_label.replace("Klasse","").strip()
        if square_meters:
            square_meters   = int(float(square_meters.replace(",",".").replace("m²","")))

        if room_count:
            room_count = int(float(room_count.replace(",",".").strip()))
        if longitude:
            zipcode, city, address = extract_location_from_coordinates(longitude, latitude)
        if available_date:
            if 'sofort' in available_date.lower():
                available_date = ''

            else:
                available_date = dateutil.parser.parse(available_date).strftime("%Y-%m-%d")

        if not property_type:
            property_type = 'apartment'

        # # MetaData
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
        item_loader.add_value("floor_plan_images", floor_plan_images) # Array

        # # Monetary Status
        item_loader.add_value("rent", rent) # Int
        item_loader.add_value("deposit", deposit) # Int
        # item_loader.add_value("prepaid_rent", prepaid_rent) # Int
        item_loader.add_value("utilities", utilities) # Int
        item_loader.add_value("currency", "EUR") # String

        #item_loader.add_value("water_cost", water_cost) # Int
        #item_loader.add_value("heating_cost", heating_cost) # Int

        item_loader.add_value("energy_label", energy_label) # String

        # # LandLord Details
        item_loader.add_value("landlord_name", 'wankendorfer') # String
        item_loader.add_value("landlord_phone", '+43120050') # String
        item_loader.add_value("landlord_email", 'info@wankendorfer.de') # String

        self.position += 1
        yield item_loader.load_item()
def make_property_type(prop):
    apartments = ['wohnung', 'loft', 'attic', 'dachgeschoss', 'erdgeschoss', 'terrassen']
    houses     = ['huis','woning','terratetto', 'einfamilienhaus']
    if not prop:
        return ""
    prop = prop.lower()
    
    if prop == "keine angabe":
        return ""

    for apartment in apartments:
        if apartment in prop:
            return 'apartment'
    for house in houses:
        if house in prop:
            return 'house'
    return prop
