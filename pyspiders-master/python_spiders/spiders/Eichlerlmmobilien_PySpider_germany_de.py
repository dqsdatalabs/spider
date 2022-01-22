# -*- coding: utf-8 -*-
# Author: Marwan Eid
import scrapy
from scrapy import Request
from ..loaders import ListingLoader
import re

from ..helper import extract_location_from_address, extract_location_from_coordinates, format_date

def custom_date_format(date):
    return date.replace(".", "").replace("Januar", "January").replace("Februar", "February").replace("März", "March").replace("Mai", "May").replace("Juni", "June").replace("Juli", "July").replace("Oktober", "October").replace("Dezember", "December")

def remove_white_spaces(str):
    new_str = ''.join(c for c in str if c not in '\r\t\n\xa0').strip()
    return re.sub(' {2,}', ' ', new_str)

def find_nth(haystack, needle, n):
    start = haystack.find(needle)
    while start >= 0 and n > 1:
        start = haystack.find(needle, start+len(needle))
        n -= 1
    return start

def parse_numeric(num):
    num = num[: num.find(",")].replace(".", "")
    return get_float(num)

def get_float(str):
    num = re.findall(r"[-+]?\d*\.\d+|\d+", str)
    if len(num) == 1:
        num = int(num[0])
    elif len(num) == 2:
        if (len(num[1]) == 1):
            num = float(num[0]) + float(num[1][0]) * 0.1
        elif (len(num[1]) == 1):
            num = float(num[0]) + float(num[1][0]) * 0.1 + float(num[1][1]) * 0.01
    return num

class EichlerlmmobilienPyspiderGermanyDeSpider(scrapy.Spider):
    name = "eichlerimmobilien"
    start_urls = ['http://www.regioklick.eu/miete-eichler.asp']
    allowed_domains = ["de", "eu"]
    country = 'germany'
    locale = 'de'
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
        for item in response.xpath("//p[contains(@align,'right')]//a/@href").extract():
            follow_url = response.urljoin(item)
            yield Request(follow_url, callback=self.populate_item)

    # 3. SCRAPING level 3
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        external_id = response.url[str(response.url).find("=") + 1:]
        description = remove_white_spaces(response.xpath("//tr[contains(@bgcolor,'#F0F0F0')][10]//font[contains(@face,'Arial')]/text()[1]").getall()[1])
        address = remove_white_spaces(response.xpath("//tr[contains(@bgcolor,'#F0F0F0')][1]//font[contains(@face,'Arial')]/text()[1]").getall()[1])
        longitude, latitude = extract_location_from_address(address)
        zipcode, city, address = extract_location_from_coordinates(longitude, latitude)
        p_type = remove_white_spaces(response.xpath("//tr[contains(@bgcolor,'#F0F0F0')][2]//font[contains(@face,'Arial')]/text()[1]").getall()[0])
        if ("Zimmerwohnung" in p_type):
            property_type = "apartment"
        square_meters = remove_white_spaces(response.xpath("//tr[contains(@bgcolor,'#F0F0F0')][3]//font[contains(@face,'Arial')]/text()[1]").getall()[0])
        square_meters = get_float(square_meters)
        room_count = int(get_float(p_type[: p_type.find('-')]))
        available_date = remove_white_spaces(response.xpath("//tr[contains(@bgcolor,'#F0F0F0')][5]//font[contains(@face,'Arial')]/text()[1]").getall()[1])
        available_date = custom_date_format(available_date)
        available_date = format_date(available_date, date_format="%d %B %Y")
        if "erlaubt" in description:
            pets_allowed = True
        elif "wünschen kein" in description:
            pets_allowed = False
        parking_space = remove_white_spaces(response.xpath("//tr[contains(@bgcolor,'#F0F0F0')][6]//font[contains(@face,'Arial')]/text()[1]").getall())
        underground_parking = remove_white_spaces(response.xpath("//tr[contains(@bgcolor,'#F0F0F0')][8]//font[contains(@face,'Arial')]/text()[1]").getall())
        parking_space = parking_space[parking_space.find("Stell"): ]
        underground_parking = underground_parking[underground_parking.find("Tief"):]
        if ("ja" in parking_space or "ja" in underground_parking):
            parking = True
        else:
            parking = False
        balc = remove_white_spaces(response.xpath("//tr[contains(@bgcolor,'#F0F0F0')][7]//font[contains(@face,'Arial')]/text()[1]").getall())
        balc = balc[: balc.find("Einz")]
        if "ja" in balc:
            balcony = True
            terrace = True
        else:
            balcony = False
            terrace = False
        images = response.xpath("//img[contains(@border,'0')]//@src").getall()
        images = ["http://www.regioklick.eu/" + im for im in images]
        rent = parse_numeric(remove_white_spaces(response.xpath("//tr[contains(@bgcolor,'#F0F0F0')][12]//font[contains(@face,'Arial')]/text()[1]").getall()[1]))
        extra1 = remove_white_spaces(response.xpath("//tr[contains(@bgcolor,'#F0F0F0')][13]//font[contains(@face,'Arial')]/text()[1]").getall()[0])
        if (len(extra1) > 0):
            extra1 = int(extra1[extra1.find(" ") + 1: extra1.find(",")])
        else:
            extra1 = 0
        extra2 = parse_numeric(remove_white_spaces(response.xpath("//tr[contains(@bgcolor,'#F0F0F0')][14]//font[contains(@face,'Arial')]/text()[1]").getall()[1]))
        utilities = extra1 + extra2
        deposit = parse_numeric(remove_white_spaces(response.xpath("//tr[contains(@bgcolor,'#F0F0F0')][15]//font[contains(@face,'Arial')]/text()[1]").getall()[1]))
        if "Euro" in remove_white_spaces(response.xpath("//tr[contains(@bgcolor,'#F0F0F0')][15]//font[contains(@face,'Arial')]/text()[1]").getall()[1]):
            currency = "EUR"
        landlord_name = remove_white_spaces(response.xpath("//tr[contains(@bgcolor,'#F0F0F0')][18]//font[contains(@face,'Arial')]/text()[1]").getall()[1])
        landlord_name = landlord_name[: landlord_name.find(",")]
        landlord_phone = remove_white_spaces(response.xpath("//tr[contains(@bgcolor,'#F0F0F0')][19]//font[contains(@face,'Arial')]/text()[1]").getall()[1])
        index = find_nth(landlord_phone, " ", 3)
        landlord_number = landlord_phone[index + 1:]
        landlord_email = remove_white_spaces(response.xpath("//tr[contains(@bgcolor,'#F0F0F0')][21]//font[contains(@face,'Arial')]/text()[1]").getall()[1])

        # # MetaData
        item_loader.add_value("external_link", response.url) # String
        item_loader.add_value("external_source", self.external_source) # String
        item_loader.add_value("external_id", external_id) # String
        item_loader.add_value("position", self.position) # Int
        #item_loader.add_value("title", title) # String
        item_loader.add_value("description", description) # String
        item_loader.add_value("latitude", str(latitude)) # String
        item_loader.add_value("longitude", str(longitude)) # String
        # # Property Details
        item_loader.add_value("zipcode", zipcode) # String
        item_loader.add_value("city", city) # String
        item_loader.add_value("address", address) # String
        #item_loader.add_value("floor", floor) # String
        item_loader.add_value("property_type", property_type) # String => ["apartment", "house", "room", "student_apartment", "studio"]
        item_loader.add_value("square_meters", square_meters) # Int
        item_loader.add_value("room_count", room_count) # Int
        #item_loader.add_value("bathroom_count", bathroom_count) # Int
        item_loader.add_value("available_date", available_date) # String => date_format
        item_loader.add_value("pets_allowed", pets_allowed) # Boolean
        #item_loader.add_value("furnished", furnished) # Boolean
        item_loader.add_value("parking", parking) # Boolean
        #item_loader.add_value("elevator", elevator) # Boolean
        item_loader.add_value("balcony", balcony) # Boolean
        item_loader.add_value("terrace", terrace) # Boolean

        # # Images
        item_loader.add_value("images", images) # Array
        item_loader.add_value("external_images_count", len(images)) # Int

        # # Monetary Status
        item_loader.add_value("rent", rent) # Int
        item_loader.add_value("deposit", deposit) # Int
        #item_loader.add_value("prepaid_rent", prepaid_rent) # Int
        item_loader.add_value("utilities", utilities) # Int
        item_loader.add_value("currency", currency) # String

        # # LandLord Details
        item_loader.add_value("landlord_name", landlord_name) # String
        item_loader.add_value("landlord_phone", landlord_number) # String
        item_loader.add_value("landlord_email", landlord_email) # String

        self.position += 1
        yield item_loader.load_item()
