# -*- coding: utf-8 -*-
# Author: Fill with the developer's Name
import scrapy
from ..loaders import ListingLoader
from ..helper import extract_location_from_coordinates


class WohnbarPyspiderGermanySpider(scrapy.Spider):
    name = "Wohnbar"
    start_urls = [
        'https://www.wohnbar24.de/wohnungen-und-haeuser-zur-miete.xhtml?f[516-43]=wohnung%2Chaus&f[516-7]=miete&f[516-9]=wohnen&f[516-1]=0&p[obj0]=1',
        'https://www.wohnbar24.de/wohnungen-und-haeuser-zur-miete.xhtml?f[516-43]=wohnung%2Chaus&f[516-7]=miete&f[516-9]=wohnen&f[516-1]=0&p[obj0]=2'
    ]
    allowed_domains = ["wohnbar24.de"]
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
        apartments_divs = response.xpath('.//div[contains(@class, "wrap-listitems")]//div[contains(@class, "grid-33")]')
        for apartment_div in apartments_divs:
            available = apartment_div.xpath('.//div[contains(@class, "obj-listobject-img")]//a//span').extract()
            if len(available) == 0:
                available = ""
            else:
                available = available[0]
            if "rented" not in available:
                if "reserved" not in available:
                    apartment_data = apartment_div.xpath('.//div[contains(@class, "obj-listobject-text")]//div[contains(@class, "obj-listobject-data")]//table')
                    room_count = apartment_data.xpath('.//tr[3]//td//span/text()')[0].extract()
                    square_meters = apartment_data.xpath('.//tr[4]//td//span/text()')[0].extract()
                    apartment_url = apartment_div.xpath('.//div[contains(@class, "obj-listobject-img")]//a/@href')[0].extract()
                    apartment_url = (apartment_url.split("id"))[1]
                    url = "https://www.wohnbar24.de/detailansicht-mietwohnungen.xhtml?id" + apartment_url
                    yield scrapy.Request(url, callback=self.populate_item, meta={
                        "room_count": room_count,
                        "square_meters": square_meters,
                    })

    # 3. SCRAPING level 3
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        room_count = response.meta.get("room_count")
        room_count = int(room_count)

        square_meters = response.meta.get("square_meters")
        square_meters = (square_meters.split())[0]
        square_meters = int(square_meters)

        title = response.xpath('.//p[contains(@class, "obj-subtitle")]/text()')[0].extract()
        title = title.replace("[]", "|")
        images = response.xpath('.//div[contains(@class, "fotorama")]//div/@data-img').extract()
        address = response.xpath('.//div[contains(@class, "obj-data-basics")]//p/text()').extract()

        apartment_info_keys = response.xpath('.//div[contains(@class, "obj-data-basics")]//strong/text()').extract()
        apartment_info_values = response.xpath('.//div[contains(@class, "obj-data-basics")]//span/text()').extract()
        apartment_info = dict(zip(apartment_info_keys, apartment_info_values))

        external_id = apartment_info["Expose Nr"]

        floor = apartment_info["Etage"]

        available_date = apartment_info["verfügbar ab"]
        if "sofort" in available_date:
            available_date = None
        else:
            available_date = available_date.split(".")
            day = available_date[0]
            month = available_date[1]
            year = available_date[2]
            available_date = year.strip() + "-" + month.strip() + "-" + day.strip()

        rent = apartment_info["Kaltmiete"]
        rent = (rent.split())[0]
        rent = rent.replace(".", "")
        rent = rent.replace(",", ".")
        rent = float(rent)
        rent = round(rent)
        rent = int(rent)

        utilities = apartment_info["Nebenkosten"]
        utilities = (utilities.split())[0]
        utilities = utilities.replace(".", "")
        utilities = utilities.replace(",", ".")
        utilities = float(utilities)
        utilities = round(utilities)
        utilities = int(utilities)

        deposit = apartment_info["Kaution"]
        deposit = (deposit.split())[0]
        deposit = deposit.replace(".", "")
        deposit = deposit.replace(",", ".")
        deposit = float(deposit)
        deposit = round(deposit)
        deposit = int(deposit)

        balcony = None
        elevator = None
        parking = None
        balcony_exist = apartment_info["Balkon"]
        if "nein" not in balcony_exist:
            balcony = True
        elevator_exist = apartment_info["Fahrstuhl"]
        if "nein" not in elevator_exist:
            elevator = True
        parking_exist = apartment_info["Stellplatz"]
        if "nein" not in parking_exist:
            parking = True

        description = response.xpath('.//div[contains(@class, "obj-decription-text")][1]//p[1]/text()').extract()

        property_type = "apartment"

        landlord_data = response.xpath('.//div[contains(@class, "obj-contact")]//span[contains(@class, "wrap-data")]//span/text()').extract()
        landlord_name = landlord_data[0]
        landlord_number = landlord_data[1]
        landlord_email = landlord_data[2]

        lat_lng = response.xpath('.//script/text()').extract()
        lat_lng_final = None
        for item in lat_lng:
            if "google.maps" in item:
                item = (item.split("google.maps"))[1]
                lat_lng_final = (item.split("disableDefaultUI"))[0]
        lat_lng_final = lat_lng_final.split("(")[1]
        lat_lng_final = lat_lng_final.split(")")[0]
        lat_lng_final = lat_lng_final.split(",")
        latitude = lat_lng_final[0].strip()
        longitude = lat_lng_final[1].strip()
        zipcode, city, no_address = extract_location_from_coordinates(longitude, latitude)

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

        #item_loader.add_value("pets_allowed", pets_allowed) # Boolean
        #item_loader.add_value("furnished", furnished) # Boolean
        item_loader.add_value("parking", parking) # Boolean
        item_loader.add_value("elevator", elevator) # Boolean
        item_loader.add_value("balcony", balcony) # Boolean
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

        #item_loader.add_value("energy_label", energy_label) # String

        # # LandLord Details
        item_loader.add_value("landlord_name", landlord_name) # String
        item_loader.add_value("landlord_phone", landlord_number) # String
        item_loader.add_value("landlord_email", landlord_email) # String

        self.position += 1
        yield item_loader.load_item()
