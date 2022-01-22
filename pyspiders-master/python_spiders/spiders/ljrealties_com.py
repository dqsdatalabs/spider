import re

import requests
import scrapy
from scrapy import Request

from ..helper import remove_unicode_char, remove_white_spaces, extract_number_only, sq_feet_to_meters, format_date
from ..loaders import ListingLoader


class WebsiteDomainSpider(scrapy.Spider):
    name = 'ljrealties_com'
    allowed_domains = ['www.ljrealties.com']
    start_urls = ['https://www.ljrealties.com/louer_maison.php?Codelang=En']
    country = 'canada'
    locale = 'en'
    external_source = "{}_PySpider_{}_{}".format(
        name.capitalize(), country, locale)
    execution_type = 'development'

    def parse(self, response):
        for start_url in self.start_urls:
            yield Request(url=start_url,
                          callback=self.parse_area)

    def parse_area(self, response):
        rentals = response.css('.secteur_maison a::attr(href)').extract()
        for rental in rentals:
            yield Request(url='https://www.ljrealties.com/'+rental,
                          callback=self.parse_area_pages)

    def parse_area_pages(self, response):
        item_loader = ListingLoader(response=response)
        external_link = response.url
        external_id = (response.css('.adresse span::text').extract_first()).replace("Centris No. ",'')
        external_source = self.external_source
        title = response.css('h1::text').extract_first()
        description = " ".join(response.css('.addendap::text').extract())
        address = ((response.css('.adresse::text').extract_first()).replace('\n','')).replace('\t','')
        responseGeocode = requests.get(
            f"https://geocode.arcgis.com/arcgis/rest/services/World/GeocodeServer/find?f=json&text={address}&maxLocations=1")
        responseGeocodeData = responseGeocode.json()

        longitude = responseGeocodeData['locations'][0]['feature']['geometry']['x']
        latitude = responseGeocodeData['locations'][0]['feature']['geometry']['y']

        responseGeocode = requests.get(
            f"https://geocode.arcgis.com/arcgis/rest/services/World/GeocodeServer/reverseGeocode?location={longitude},{latitude}&f=pjson&distance=50000&outSR=")
        responseGeocodeData = responseGeocode.json()
        zipcode = responseGeocodeData['address']['Postal']
        city = responseGeocodeData['address']['City']
        address = responseGeocodeData['address']['Match_addr']

        longitude = str(longitude)
        latitude = str(latitude)
        # city = None
        # zipcode = None
        # latitude = None
        # longitude = None

        char_dict = {}
        char_boxes = response.css('.tr')
        for box in char_boxes:
            if box.css('.td:nth-child(1)::text').extract_first() is not None:
                char_dict[box.css('.td:nth-child(1)::text').extract_first()] = box.css(
                    '.td:nth-child(2)::text').extract_first()


        property_type = (char_dict['Property Type']).lower()

        if property_type == "condo":
            property_type = 'apartment'
        square_meters = (char_dict['Living Area'])
        if square_meters:
            square_meters = sq_feet_to_meters(float((extract_number_only(extract_number_only(square_meters)))[:-2]))
        else:
            square_meters = 0

        if square_meters == 0:
            square_meters = 1

        room_count = int(extract_number_only((response.css('h3+p::text').extract())[0]))
        bathroom_count = int(extract_number_only((response.css('h3+p::text').extract())[2]))
        available_date = response.css('.charact').extract()
        date_regex = re.findall('date<\/td>\\n\s+<td valign="top" class="td">\\n\s+(?=(?P<month>\d{2})-(?P<day>\d{2})-(?P<year>\d{4}))',available_date[0])

        if date_regex:
            date_regex = date_regex[0]
            date_formatted = date_regex[-1]+'-'+date_regex[0]+'-'+date_regex[1]
            available_date = format_date(date_formatted)
        else:
            available_date = None
        images = response.css('.thumbitm img::attr(src)').extract()
        floor_plan_images = None
        external_images_count = len(images)
        rent = extract_number_only(extract_number_only((response.css('h1::text').extract())[-1]))
        currency = "CAD"
        deposit = None
        prepaid_rent = None
        utilities = None
        water_cost = None
        heating_cost = None
        energy_label = None
        pets_allowed = None
        furnished = None

        floor = response.css('p+ .simultable .tr:nth-child(2) .td:nth-child(2)::text').extract_first()
        if floor is None or property_type == 'house' or floor == 'Other':
            floor = ''
        elif floor == 'Basement':
            floor = 'basement floor'



        if 'parking' in description.lower():
            parking = True
        else:
            parking = False

        if 'elevator' in description.lower():
            elevator = True
        else:
            elevator = False

        if 'balcony' in description.lower():
            balcony = True
        else:
            balcony = False

        if 'terrace' in description.lower():
            terrace = True
        else:
            terrace = False

        swimming_pool = None

        if ' washer' in description.lower():
            washing_machine = True
        else:
            washing_machine = False

        if ' dishwasher' in description.lower():
            dishwasher = True
        else:
            dishwasher = False

        landlord_name = response.css('.courtaside:nth-child(1) p::text').extract_first()
        if landlord_name is None:
            landlord_name = 'LJ Realties'
        landlord_email = None
        landlord_phone = '514-500-4040'

        # --------------------------------#
        # item loaders
        item_loader.add_value('external_link', external_link)
        item_loader.add_value('external_id', external_id)
        item_loader.add_value('external_source', external_source)
        item_loader.add_value('title', title)
        item_loader.add_value('description', description)
        item_loader.add_value('city', city)
        item_loader.add_value('zipcode', zipcode)
        item_loader.add_value('address', address)
        item_loader.add_value("latitude", latitude)
        item_loader.add_value("longitude", longitude)
        item_loader.add_value('property_type', property_type)
        item_loader.add_value('square_meters', int(int(square_meters)*10.764))
        item_loader.add_value('room_count', room_count)
        item_loader.add_value('bathroom_count', bathroom_count)
        item_loader.add_value('available_date', available_date)
        item_loader.add_value("images", images)
        item_loader.add_value("floor_plan_images", floor_plan_images)
        item_loader.add_value("external_images_count", external_images_count)
        item_loader.add_value("rent", rent)
        item_loader.add_value("currency", currency)
        item_loader.add_value("deposit", deposit)
        item_loader.add_value("prepaid_rent", prepaid_rent)
        item_loader.add_value("utilities", utilities)
        item_loader.add_value("water_cost", water_cost)
        item_loader.add_value("heating_cost", heating_cost)
        item_loader.add_value("energy_label", energy_label)
        item_loader.add_value("pets_allowed", pets_allowed)
        item_loader.add_value("furnished", furnished)
        item_loader.add_value("parking", parking)
        item_loader.add_value("elevator", elevator)
        item_loader.add_value("floor", floor)
        item_loader.add_value("balcony", balcony)
        item_loader.add_value("terrace", terrace)
        item_loader.add_value("swimming_pool", swimming_pool)
        item_loader.add_value("washing_machine", washing_machine)
        item_loader.add_value("dishwasher", dishwasher)
        item_loader.add_value("landlord_name", landlord_name)
        item_loader.add_value("landlord_email", landlord_email)
        item_loader.add_value("landlord_phone", landlord_phone)

        yield item_loader.load_item()
