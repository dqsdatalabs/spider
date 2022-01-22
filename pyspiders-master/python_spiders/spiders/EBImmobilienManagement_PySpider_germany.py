# -*- coding: utf-8 -*-
# Author: Fill with the developer's Name
import scrapy
from ..loaders import ListingLoader
from ..helper import extract_location_from_coordinates, extract_location_from_address


class EbimmobilienmanagementPyspiderGermanySpider(scrapy.Spider):
    name = "EBImmobilienManagement"
    start_urls = [
        'https://www.eb-immo.de/component/prime/objects/search/0-0-2-1?Itemid=104',
    ]
    allowed_domains = ["eb-immo.de"]
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
        total_items = response.css('h2.pstate::text')[0].extract()
        total_items = total_items.split()[0]
        total_items = int(total_items)
        total_pages = int(total_items / 20) + (total_items % 20 > 0)
        url = ['https://www.eb-immo.de/component/prime/objects/search/0-0-2-1?Itemid=104']
        for i in range(total_pages-1):
            next_page = 20 * (i + 1)
            url_new = 'https://www.eb-immo.de/component/prime/objects/search/0-0-2-1?start=' + str(next_page)
            url.append(url_new)
        for item in url:
            yield scrapy.Request(url=item, callback=self.parse_pages, dont_filter=True)

    # 3. SCRAPING level 3
    def parse_pages(self, response):
        apartments_divs = response.css('div.object-flat')
        for apartment_div in apartments_divs:
            address = apartment_div.xpath('.//div[contains(@class, "b01-margin-right")]//p/text()').extract()
            external_id = apartment_div.xpath('.//span[contains(@class, "externid")]/text()').extract()
            property_type = apartment_div.xpath('.//span[contains(@class, "type")]/text()').extract()
            apartment_url = apartment_div.xpath('.//div[contains(@class, "b01-margin-right")]//a/@href')[0].extract()
            url = "https://www.eb-immo.de" + apartment_url
            title = apartment_div.xpath('.//div[contains(@class, "b01-margin-right")]//h3/text()').extract()
            rent = apartment_div.xpath('.//div[contains(@class, "b01-margin-right")]//ul//li[1]//span//strong/text()').extract()
            room_count = apartment_div.xpath('.//div[contains(@class, "b01-margin-right")]//ul//li[2]//span//strong/text()').extract()
            yield scrapy.Request(url, callback=self.populate_item, meta={
                "external_id": external_id,
                "property_type": property_type,
                "title": title,
                "address": address,
                "rent": rent,
                "room_count": room_count,
            })

    # 4. SCRAPING level 4
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        room_count = response.meta.get("room_count")
        room_count = int(room_count[0])

        property_type_name = response.meta.get("property_type")
        terrace = None
        if "Terrassen" in property_type_name[0]:
            terrace = True
            property_type = "apartment"
        elif "Maisonette" in property_type_name:
            property_type = "house"
        else:
            property_type = "apartment"

        title = response.meta.get("title")

        address = response.meta.get("address")

        rent = response.meta.get("rent")
        rent = rent[0]
        rent = (rent.split())[0]
        rent = rent.replace("-", "00")
        rent = rent.replace(".", "")
        rent = rent.replace(",", ".")
        rent = float(rent)
        rent = round(rent)
        rent = int(rent)

        external_id = response.meta.get("external_id")

        apartment_data = response.xpath('.//div[contains(@class, "b01-panel")]//table[contains(@class, "b01-table")]//tr')
        apartment_data_keys = []
        apartment_data_values = []
        for row in apartment_data:

            key = row.xpath('.//td[1]/text()').extract()
            if len(key) < 1:
                key = row.xpath('.//td[1]//strong/text()').extract()
            apartment_data_keys.append(key[0])

            value = row.xpath('.//td[2]/text()').extract()
            if len(value) < 1:
                value = row.xpath('.//td[2]//strong/text()').extract()
                if len(value) < 1:
                    value = row.xpath('.//td[2]//span/@class').extract()
            apartment_data_values.append(value[0])

        apartment_dict = dict(zip(apartment_data_keys, apartment_data_values))
        utilities = None
        heating_cost = None
        floor = None
        available_date = None
        bathroom_count = 1
        square_meters = None
        elevator = None
        balcony = None
        if "Nebenkosten" in apartment_dict.keys():
            utilities = apartment_dict["Nebenkosten"]
            utilities = (utilities.split())[0]
            utilities = utilities.replace("-", "00")
            utilities = utilities.replace(".", "")
            utilities = utilities.replace(",", ".")
            utilities = float(utilities)
            utilities = round(utilities)
            utilities = int(utilities)

        if "Heizkosten" in apartment_dict.keys():
            heating_cost = apartment_dict["Heizkosten"]
            heating_cost = (heating_cost.split())[0]
            heating_cost = heating_cost.replace("-", "00")
            heating_cost = heating_cost.replace(".", "")
            heating_cost = heating_cost.replace(",", ".")
            heating_cost = float(heating_cost)
            heating_cost = round(heating_cost)
            heating_cost = int(heating_cost)

        if "Lage im Objekt (Etage)" in apartment_dict.keys():
            floor = apartment_dict["Lage im Objekt (Etage)"]

        if "Verfügbar ab" in apartment_dict.keys():
            available_date = apartment_dict["Verfügbar ab"]
            if "sofort" in available_date.lower():
                available_date = None
            else:
                available_date = available_date.split(".")
                day = available_date[0]
                month = available_date[1]
                year = available_date[2]
                available_date = year.strip() + "-" + month.strip() + "-" + day.strip()

        if "Anzahl Badezimmer" in apartment_dict.keys():
            bathroom_count = apartment_dict["Anzahl Badezimmer"]
            bathroom_count = int(bathroom_count[0])

        if "Wohnfläche" in apartment_dict.keys():
            square_meters = apartment_dict["Wohnfläche"]
            square_meters = (square_meters.split())[0]
            square_meters = square_meters.replace(".", "")
            square_meters = square_meters.replace(",", ".")
            square_meters = float(square_meters)
            square_meters = round(square_meters)
            square_meters = int(square_meters)

        if "Aufzug" in apartment_dict.keys():
            elevator = True

        if "Anzahl Balkone" in apartment_dict.keys():
            balcony = True

        # description_div = response.xpath('.//div[contains(@class, "panel")]//div[contains(@class, "b01-panel")]')
        description_div = response.css('div.panel div.b01-panel')
        description_list = []
        description = ""
        for div in description_div:
            item = div.css('p::text').extract()
            description_list.append(item)
        for item in description_list:
            description = description + " ".join(item)
        description = description.replace("* Pflichtfelder", "")
        description = description.replace("\n", "")
        description = description.replace("  ", " ")

        if "Terrassen" in description:
            terrace = True
        if "Balkon" in description:
            balcony = True

        contact_info_div = response.xpath('.//div[contains(@class, "b01-panel")]')
        landlord_info = []
        for div in contact_info_div:
            found_div = div.xpath('.//h3/text()').extract()
            if "Kontakt" in found_div:
                landlord_info = div.xpath('.//p/text()').extract()
        landlord_name = landlord_info[0].strip()
        landlord_name = landlord_name.split(':')[1]
        landlord_name = landlord_name.strip()
        landlord_number = landlord_info[1].strip()
        landlord_number = landlord_number.replace("Tel.:", "").strip()
        landlord_number = landlord_number.replace(" ", "")
        landlord_email = landlord_info[2].strip()
        landlord_email = landlord_email.replace("E-Mail:", "").strip()

        images_list = response.xpath('.//ul[contains(@class, "b01-slideshow")]//li//a/@href').extract()
        images = []
        for image in images_list:
            images.append("https://www.eb-immo.de" + image)

        if len(address) < 1:
            address = response.css('ul#pswitch h3.b01-panel-title::text')[0].extract()
        else:
            address = address[0]
        longitude = response.xpath('.//div/@data-geo_long')[0].extract()
        latitude = response.xpath('.//div/@data-geo_lat')[0].extract()
        if longitude == "0":
            longitude, latitude = extract_location_from_address(address)
        longitude = str(longitude)
        latitude = str(latitude)
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
        item_loader.add_value("bathroom_count", bathroom_count) # Int

        item_loader.add_value("available_date", available_date) # String => date_format

        #item_loader.add_value("pets_allowed", pets_allowed) # Boolean
        #item_loader.add_value("furnished", furnished) # Boolean
        #item_loader.add_value("parking", parking) # Boolean
        item_loader.add_value("elevator", elevator) # Boolean
        item_loader.add_value("balcony", balcony) # Boolean
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
        item_loader.add_value("utilities", utilities) # Int
        item_loader.add_value("currency", "EUR") # String

        #item_loader.add_value("water_cost", water_cost) # Int
        item_loader.add_value("heating_cost", heating_cost) # Int

        #item_loader.add_value("energy_label", energy_label) # String

        # # LandLord Details
        item_loader.add_value("landlord_name", landlord_name) # String
        item_loader.add_value("landlord_phone", landlord_number) # String
        item_loader.add_value("landlord_email", landlord_email) # String

        self.position += 1
        yield item_loader.load_item()
