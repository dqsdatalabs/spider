# -*- coding: utf-8 -*-
# Author: Abdelrahman-Moharram
import scrapy
import dateutil.parser
from python_spiders.helper import remove_white_spaces, get_amenities, extract_location_from_address, extract_location_from_coordinates
from python_spiders.loaders import ListingLoader

class SchneiderImmobilienSpider(scrapy.Spider):
    name = "schneider_immobilien"
    start_urls = ['https://www.schneider-immobilien.com/angebote']
    allowed_domains = ["schneider-immobilien.com"]
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
        for property in response.css(".object.object__container"):
            price = property.css(".object__infotext-item .key::text").getall()
            if "Kaufpreis" not in price:
                yield scrapy.Request(url=response.urljoin(property.css(".object__btn::attr(href)").get()), callback=self.populate_item)
        next_page = response.xpath('//a[contains(@title, "Weiter")]/@href').get()
        if next_page:
            yield scrapy.Request(url=response.urljoin(next_page), callback=self.parse)



    # 3. SCRAPING level 3
    def populate_item(self, response):
        landlord_phone  = "+49 2102 709400"
        item_loader     = ListingLoader(response=response)
        property_type   = 'apartment'
        property_type   = make_property_type(response.css("h1 small::text").get())

        rent            = response.xpath('//dt[contains(text(), "Miete inkl. NK")]/following-sibling::dd/text()').get()
        if not rent:
            rent        = response.xpath('//dt[contains(text(), "Warmmiete")]/following-sibling::dd/text()').get()
        if not rent:
            rent        = response.xpath('//dt[contains(text(), "Miete zzgl. NK")]/following-sibling::dd/text()').get()
        
        if property_type != 'dont_scrape' and rent != 'Preis auf Anfrage':
            rent            = int(float(rent.replace("€","").replace(".","").replace("\xa0\xa0zzgl\xa0Heizkosten","").replace("\xa0\xa0zzgl\xa0Nebenkosten\xa0&\xa0Heizkosten","").replace(",",".").strip()))
            city            = response.css("h1 small::text").re("in.*")[0].replace("in","").strip()
            title           =  "".join(response.css("h1::text").getall()).strip()
            landlord_name   = response.xpath('//ul[contains(@class, "list--contact")]/li/i[contains(@class, "fa-user")]/following-sibling::text()').get().strip()
            landlord_phone  = response.xpath('//ul[contains(@class, "list--contact")]/li/i[contains(@class, "fa-phone")]/following-sibling::text()').get().strip()
            external_id     = response.xpath('//h2[contains(text(), "Objekt-Nr")]/text()').get().strip()
            description     = remove_white_spaces(" ".join(response.css(".readmore__wrap p::text").getall()))
            square_meters   = response.xpath('//dt[contains(text(), "Wohnfläche")]/following-sibling::dd/text()').get()
            room_count      = int(float(response.xpath('//dt[contains(text(), "Zimmer")]/following-sibling::dd/text()').get().replace(",",".").strip()))
            
            utilities       = response.xpath('//dt[contains(text(), "Nebenkosten")]/following-sibling::dd/text()').get()
            deposit         = response.xpath('//dt[contains(text(), "Kaution")]/following-sibling::dd/text()').get()
            bathroom_count  = response.xpath('//dt[contains(text(), "Anzahl Badezimmer")]/following-sibling::dd/text()').get()
            floor           = response.xpath('//dt[contains(text(), "Anzahl Etagen")]/following-sibling::dd/text()').get()
            address         = response.xpath('//h2[contains(text(), "Adresse")]/following-sibling::p/text()').get().strip()
            energy_label    = response.xpath('//dt[contains(text(), "Energieeffizienzklasse")]/following-sibling::dd/text()').get()
            images          = response.css(".m-imageratio::attr(style)").re("https.*(?=\))")
            pets_allowed, furnished, parking, elevator, balcony, terrace, swimming_pool, washing_machine, dishwasher = get_amenities(description, " ".join(response.css("dl.dl-horizontal dt::text").getall()), item_loader)

            if deposit:
                deposit  = int(float(deposit.replace("€","").replace(".","").replace(",",".").strip()))

            if not square_meters:
                square_meters   = response.xpath('//dt[contains(text(), "Nutzfläche")]/following-sibling::dd/text()').get()

            square_meters   = int(float(square_meters.replace("ca.","").replace("m²","").replace(",",".").strip()))

            

            if utilities :
                utilities       = int(float(utilities.replace("€","").replace(".","").replace(",",".").strip()))



            if bathroom_count:
                bathroom_count  = int(bathroom_count.strip())
            
            
            longitude, latitude     = extract_location_from_address(address)
            zipcode, city, address  = extract_location_from_coordinates(longitude, latitude)

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

            #item_loader.add_value("available_date", available_date) # String => date_format

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
            #item_loader.add_value("external_images_count", len(images)) # Int
            #item_loader.add_value("floor_plan_images", floor_plan_images) # Array

            # # Monetary Status
            item_loader.add_value("rent", rent) # Int
            item_loader.add_value("deposit", deposit) # Int
            # item_loader.add_value("prepaid_rent", prepaid_rent) # Int
            item_loader.add_value("utilities", utilities) # Int
            item_loader.add_value("currency", "EUR") # String

            #item_loader.add_value("water_cost", water_cost) # Int
            #item_loader.add_value("heating_cost", heating_cost) # Int

            item_loader.add_value("energy_label", energy_label) # String

            # LandLord Details
            item_loader.add_value("landlord_name", landlord_name) # String
            item_loader.add_value("landlord_phone", landlord_phone) # String
            # item_loader.add_value("landlord_email", landlord_email) # String

            self.position += 1
            yield item_loader.load_item()



def make_property_type(val):
    apartments = ['wohnung', 'loft', 'attic', 'dachgeschoss']
    houses     = ['huis','woning','terratetto', 'einfamilienhaus', 'haus']
    studios     = ['studio', 'bachelor','student', 'bedrooms']
    commericals = ['einzelhandel', 'büro', 'sonstige', 'halle', 'Lager', 'produktion', 'garage', 'stellplatz', 'gastgewerbe', 'hotel', 'grundstück zur miete']
    if not val:
        return ''
    val =  val.lower()

    for commerical in commericals:
        if commerical in val:
            return 'dont_scrape'

    if 'zimmer' in val:
        return 'room'
    for house in houses:
        if house in val:
            return 'house'
    for aprt in apartments:
        if aprt in val:
            return 'apartment'
    for studio in studios:
        if studio in val:
            return 'studio'
