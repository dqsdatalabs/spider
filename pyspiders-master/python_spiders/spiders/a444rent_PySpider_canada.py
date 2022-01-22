# -*- coding: utf-8 -*-
# Author: Fill with the developer's Name
import scrapy
from ..loaders import ListingLoader
from ..helper import extract_location_from_address, sq_feet_to_meters


class A444rentPyspiderCanadaSpider(scrapy.Spider):
    name = "444rent"
    start_urls = ['https://www.444rent.com/']
    allowed_domains = ["444rent.com"]
    country = 'canada' # Fill in the Country's name
    locale = 'en' # Fill in the Country's locale, look up the docs if unsure
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
        apartments_table = response.xpath('//body//center//table[1]//tr[2]//td[2]//table[3]//center')
        for apartment_row in apartments_table:
            apartment_url = apartment_row.xpath('.//a/@href')[0].extract()
            apartment_url = "https://www.444rent.com/" + apartment_url
            apartment_address = apartment_row.xpath('.//table//tr[2]//td[2]//table//tr//td[1]//div/text()')[:3].extract()
            yield scrapy.Request(url=apartment_url, callback=self.populate_item, meta={"address": apartment_address})

    # 3. SCRAPING level 3
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)
        title = response.xpath('.//div[contains(@id, "addressmain")]/text()')[0].extract()

        apartment_id = title.split(", ")
        apartment_id = apartment_id[1]
        apartment_id = apartment_id.split()
        if "Apt" in apartment_id:
            apartment_id.remove("Apt")
        if "Unit" in apartment_id:
            apartment_id.remove("Unit")
        external_id = " ".join(apartment_id)

        address = response.meta.get("address")
        zipcode = address[2]
        city = address[1]
        address = (address[0].split(", "))[0] + " " + (address[0].split(", "))[1] + " " + address[2] + " NS"
        lon, lat = extract_location_from_address(address)
        latitude = str(lat)
        longitude = str(lon)

        rooms = response.xpath('normalize-space(.//div[contains(@id, "suitemain")]/text())')[0].extract()
        rooms = rooms.split()
        if "+" in rooms:
            rooms.remove("+")
        if "DEN" in rooms:
            rooms.remove("DEN")
        room_count = rooms[0]
        bathroom_count = rooms[2]

        square_meters = response.xpath('.//div[contains(@id, "floorplanmain")]/text()')[0].extract()
        square_meters = square_meters.replace(" ft", "")
        square_meters = square_meters.replace(",", "")
        square_meters = int(square_meters)
        square_meters = sq_feet_to_meters(square_meters)

        rent = response.xpath('.//div[contains(@id, "pricemain")]//b/text()')[0].extract()
        rent = rent.replace("$", "")
        rent = rent.replace(",", "")
        rent = int(rent)

        available_date_all = response.xpath('normalize-space(.//body//center//table[1]//tr[2]//td[2]//table//table[1]//td[2]//table//td[2]//div[3]/text())')[0].extract()
        if available_date_all == "Available Now":
            available_date = available_date_all
        else:
            available_date_all = available_date_all.split(",")
            year = available_date_all[1][-4:]
            day = available_date_all[0][-2:].strip()
            month = available_date_all[0][:3]
            month = self.convertMonthToNum(month)
            available_date = year + "-" + month + "-" + "0" + day

        contact_location = response.xpath('.//body//center//table[1]//tr[2]//td[2]//table//tr//td//table[1]')
        landlord_name = contact_location.xpath('.//tr//td[2]//div//div//b/text()')[0].extract()
        landlord_number = contact_location.xpath('.//tr//td[2]//div//table//tr//td[2]//div/text()')[0].extract()
        landlord_number = landlord_number[-12:]
        landlord_email = contact_location.xpath('.//tr//td[2]//div//u/text()')[0].extract()

        # tab1
        leasing_all = response.xpath('.//div[contains(@id, "tab1")]//table[1]//tr')
        leasing_key = []
        leasing_value = []
        for leasing in leasing_all:
            leasing_keys = leasing.xpath('.//td[1]//b/text()').extract()
            leasing_values = leasing.xpath('.//td[2]//table//td/text()').extract()
            if leasing_values:
                leasing_key.append(leasing_keys[0])
                leasing_value.append(leasing_values)
        leasing_dict = dict(zip(leasing_key, leasing_value))
        parking = None
        if leasing_dict.get('Parking:'):
            parking = True

        storage_utility = ""
        utilities = None
        if leasing_dict.get('Storage:'):
            storage_utility = leasing_dict['Storage:'][0]
        total_utilities = 0
        if storage_utility:
            utilities = storage_utility
            utilities = utilities.replace("/ month", " ")
            utilities = utilities.replace("$", "")
            if "-" in utilities:
                utility = utilities.split("-")
                avg_utility = (int(utility[0]) + int(utility[1])) / 2
                total_utilities = total_utilities + avg_utility
            else:
                utility = int(utilities)
                total_utilities = total_utilities + utility
            utilities = int(total_utilities)

        deposit = None
        if leasing_dict.get('Deposit:'):
            deposit = leasing_dict['Deposit:']
            deposit = deposit[0].replace("$", "")
            deposit = deposit.replace(",", "")
            deposit = float(deposit)
            deposit = round(deposit)
            deposit = int(deposit)

        pets_allowed = None
        if leasing_dict.get('Policy:'):
            if "Cats Allowed" in leasing_dict['Policy:']:
                pets_allowed = True
            elif "No Pets" in leasing_dict['Policy:']:
                pets_allowed = False

        # tab2
        description = response.xpath('normalize-space(.//div[contains(@id, "tab2")]/text())').extract()

        # tab3
        building = response.xpath('.//div[contains(@id, "tab3")]//table//tr//td//table//tr//td/text()').extract()
        elevator = None
        if "Elevator" in building:
            elevator = True
        terrace = None
        if "Garden Terrace" in building:
            terrace = True

        # tab4
        suite = response.xpath('.//div[contains(@id, "tab4")]//table')
        suite_first = suite.xpath('normalize-space(.//tr//td/text())').extract()
        floor_existance = [i for i in suite_first if i.startswith('Unit is on floor')]
        floor = (floor_existance[0])[17]
        dishwasher = None
        dishwasher_existance = [i for i in suite_first if "Dishwasher" in i ]
        if dishwasher_existance:
            dishwasher = True
        washing_machine = None
        washing_machine_existance = [i for i in suite_first if "Washer" in i]
        if washing_machine_existance:
            washing_machine = True
        balcony_find = suite.xpath('normalize-space(.//tr//td//b/text())').extract()
        balcony = None
        balcony_existance = [i for i in balcony_find if "Balcony" in i]
        if balcony_existance:
            balcony = True

        # tab6
        images_urls = response.xpath('.//div[contains(@id, "tab6")]//a/@href').extract()
        images = []
        for image_url in images_urls:
            images.append("https://www.444rent.com/" + image_url)

        # tab7
        floor_plan_urls = response.xpath('.//div[contains(@id, "tab7")]//img/@src').extract()
        floor_plan_images = []
        for floor_plan_url in floor_plan_urls:
            floor_plan_images.append("https://www.444rent.com/" + floor_plan_url)

        property_type = "apartment"

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

        item_loader.add_value("pets_allowed", pets_allowed) # Boolean
        #item_loader.add_value("furnished", furnished) # Boolean
        item_loader.add_value("parking", parking) # Boolean
        item_loader.add_value("elevator", elevator) # Boolean
        item_loader.add_value("balcony", balcony) # Boolean
        item_loader.add_value("terrace", terrace) # Boolean
        #item_loader.add_value("swimming_pool", swimming_pool) # Boolean
        item_loader.add_value("washing_machine", washing_machine) # Boolean
        item_loader.add_value("dishwasher", dishwasher) # Boolean

        # # Images
        item_loader.add_value("images", images) # Array
        item_loader.add_value("external_images_count", len(images)) # Int
        item_loader.add_value("floor_plan_images", floor_plan_images) # Array

        # # Monetary Status
        item_loader.add_value("rent", rent) # Int
        item_loader.add_value("deposit", deposit) # Int
        #item_loader.add_value("prepaid_rent", prepaid_rent) # Int
        item_loader.add_value("utilities", utilities) # Int
        item_loader.add_value("currency", "CAD") # String

        #item_loader.add_value("water_cost", water_cost) # Int
        #item_loader.add_value("heating_cost", heating_cost) # Int

        #item_loader.add_value("energy_label", energy_label) # String

        # # LandLord Details
        item_loader.add_value("landlord_name", landlord_name) # String
        item_loader.add_value("landlord_phone", landlord_number) # String
        item_loader.add_value("landlord_email", landlord_email) # String

        self.position += 1
        yield item_loader.load_item()

    def convertMonthToNum(self, monthName):
        months = {
            "jan": "01",
            "feb": "02",
            "mar": "03",
            "apr": "04",
            "may": "05",
            "jun": "06",
            "jul": "07",
            "aug": "08",
            "sep": "09",
            "oct": "10",
            "nov": "11",
            "dec": "12",
        }
        return months.get(str(monthName).lower())
