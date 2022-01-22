# -*- coding: utf-8 -*-
# Author: A. Abbas
import scrapy
from ..loaders import ListingLoader
from ..helper import extract_location_from_address,convert_to_numeric, extract_location_from_coordinates, remove_unicode_char, string_found, extract_number_only, remove_white_spaces, format_date, extract_utilities



class HelbleRichterDeSpider(scrapy.Spider):
    name = "helble_richter_de"
    start_urls = ['https://www.helble-richter.de/wohnen-alt?vermarktung=miete&objektart=&ort=&_layout=list&page=1',
                  'https://www.helble-richter.de/wohnen-alt?vermarktung=miete&objektart=&ort=&_layout=list&page=2'
                  ]
    country = 'germany' # Fill in the Country's name
    locale = 'de' # Fill in the Country's locale, look up the docs if unsure
    external_source = "{}_PySpider_{}_{}".format(
        name.capitalize(), country, locale)
    execution_type = 'testing'

    position = 1

    # 1. SCRAPING level 1
    # def start_requests(self):
    #     for url in self.start_urls:
    #         yield scrapy.Request(url, callback=self.parse)

    # 2. SCRAPING level 2
    def parse(self, response, **kwargs):
        for item in response.xpath('//figure[@class="col-sm-5 front-cover"]'):
            url = item.xpath('.//a/@href').get()
            if item.xpath('.//figcaption/text()').get() != "vermietet":
                yield scrapy.Request(url, callback=self.populate_item)



    # 3. SCRAPING level 3
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        # START
        images = response.xpath('//div[@class="item"]/@style').getall()
        for i in range(len(images)):
            images[i] = images[i].replace("background-image: url('","").replace("');", "")
        title = response.xpath('//div[@class="col-12 estate-title"]/h1//text()').get()
        description = response.xpath('//div[@class="col-md-8"]//text()').getall()

        details = response.xpath('//div[@class="col-md-4 separated"]/dl//text()').getall()
        all_details = response.xpath('//div[@class="col-md-6 estate-details"]//table//text()').getall()
        all_details2 = response.xpath('//div[@class="col-md-6 estate-free-text"]/p//text()').getall()
        amenities = " ".join(description) + " ".join(all_details) + " ".join(all_details2)

        des = remove_white_spaces("".join(all_details)).split(" ")
        if des[-1].replace(".", "").isalpha():
            available_date = None
        else:
            available_date = des[-1].replace(".", "/")



        if "Objektart" in details:
            position = details.index("Objektart")
            type = details[position + 2]
            if type == "Etagenwohnung":
                property_type = "flat"
            else:
                property_type = "apartment"

        external_id = None
        if "Objektnummer" in details:
            position = details.index("Objektnummer")
            external_id = details[position + 2]

        location = None
        if "Ort" in details:
            position = details.index("Ort")
            location = details[position + 2]

        floor = None
        if "Etage" in details:
            position = details.index("Etage")
            floor = details[position + 2]

        rent = None
        if "Kaltmiete" in details:
            position = details.index("Kaltmiete")
            rent = convert_to_numeric(extract_number_only(details[position + 2]))

        heating_cost = None
        if "Warmmiete" in details:
            position = details.index("Warmmiete")
            cost = convert_to_numeric(extract_number_only(details[position + 2]))
            heating_cost = cost - rent

        deposit = None
        if "Kaution" in details:
            position = details.index("Kaution")
            deposit = details[position + 2]

        utilities = None
        if "Nebenkosten" in details:
            position = details.index("Nebenkosten")
            utilities = details[position + 2]

        room_count = None
        if "Anzahl Zimmer" in details:
            position = details.index("Anzahl Zimmer")
            room_count = details[position + 2]

        square_meters = None
        if "Wohnfläche" in details:
            position = details.index("Wohnfläche")
            square_meters = details[position + 2]

        parking = False
        if string_found(['Garage', 'Außenstellplatz', 'Tiefgarage', 'Tiefgaragenstellplatz', 'Stellplatz', 'Stellplätze','Einzelgarage'], amenities):
            parking = True

        elevator = False
        if string_found(['Aufzug', 'Personenfahrstuhl', 'Personenaufzug'], amenities):
            elevator = True

        balcony = False
        if string_found(['Balkon', 'Balkone', 'Südbalkon'], amenities):
            balcony = True

        terrace = False
        if string_found(['Terrassenwohnung', 'Terrasse', 'Terrasse (ca.)', 'Dachterrasse'], amenities):
            terrace = True

        longitude, latitude = extract_location_from_address(location)
        zipcode, city, address = extract_location_from_coordinates(longitude, latitude)

        # END


        # # MetaData
        item_loader.add_value("external_link", response.url) # String
        item_loader.add_value("external_source", self.external_source) # String

        item_loader.add_value("external_id", external_id) # String
        item_loader.add_value("position", self.position) # Int
        item_loader.add_value("title", remove_unicode_char(title)) # String
        item_loader.add_value("description", remove_unicode_char(" ".join(description))) # String

        # # Property Details
        item_loader.add_value("city", city) # String
        item_loader.add_value("zipcode", zipcode) # String
        item_loader.add_value("address", location) # String
        item_loader.add_value("latitude", str(latitude)) # String
        item_loader.add_value("longitude", str(longitude)) # String
        item_loader.add_value("floor", floor) # String
        item_loader.add_value("property_type", property_type) # String
        item_loader.add_value("square_meters", square_meters) # Int
        item_loader.add_value("room_count", room_count) # Int
        #item_loader.add_value("bathroom_count", bathroom_count) # Int

        item_loader.add_value("available_date", format_date(available_date))  # String => date_format

        #item_loader.add_value("pets_allowed", pets_allowed) # Boolean
        #item_loader.add_value("furnished", furnished) # Boolean
        item_loader.add_value("parking", parking) # Boolean
        item_loader.add_value("elevator", elevator) # Boolean
        item_loader.add_value("balcony", balcony) # Boolean
        item_loader.add_value("terrace", terrace) # Boolean
        #item_loader.add_value("swimming_pool", swimming_pool) # Boolean
        # item_loader.add_value("washing_machine", washing_machine) # Boolean
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
        item_loader.add_value("heating_cost", heating_cost) # Int

        #item_loader.add_value("energy_label", energy_label) # String

        # # LandLord Details
        #item_loader.add_value("landlord_name", landlord_name) # String
        #item_loader.add_value("landlord_phone", landlord_number) # String
        #item_loader.add_value("landlord_email", landlord_email) # String

        self.position += 1
        yield item_loader.load_item()
