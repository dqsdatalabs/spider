# -*- coding: utf-8 -*-
# Author: Madhumitha S
# Team: Sabertooth

from time import strptime
import scrapy
from ..loaders import ListingLoader
from ..helper import extract_number_only
from ..user_agents import random_user_agent
from geopy.geocoders import Nominatim
import re
from scrapy import Selector
from datetime import datetime
from datetime import date


class MightyhouseCoUkSpider(scrapy.Spider):
    name = 'mightyhouse_co_uk'
    allowed_domains = ['mightyhouse.co.uk']
    start_urls = ['http://mightyhouse.co.uk/']
    execution_type = 'testing'
    country = 'united_kingdom'
    locale = 'en'
    thousand_separator = ','
    scale_separator = '.'
    position = 0
    custom_settings = {
        "PROXY_ON": True,
    }

    def start_requests(self):
        start_url = ["https://www.mightyhouse.co.uk/renting"]
        for url in start_url:
            yield scrapy.Request(url=url,
                                 callback=self.parse,
                                 meta={'request_url': url})

    def parse(self, response, **kwargs):
        listings = response.xpath("//div[@class='property clearfix']")
        for property_item in listings:
            url = "http://mightyhouse.co.uk" + property_item.xpath('.//a/@href').extract_first()
            latitude = property_item.xpath(".//@data-lat").extract_first()
            longitude = property_item.xpath(".//@data-lng").extract_first()
            external_id = property_item.xpath(".//@data-id").extract_first()
            features = "".join(property_item.xpath(".//div[@class='property-content']/ul/li/text()").extract())
            address = property_item.xpath(".//div[@class='property-content']/h3/text()").extract_first()
            yield scrapy.Request(
                url = url,
                callback=self.get_property_details,
                meta={'request_url': url,
                      'address': address,
                      'latitude': latitude,
                      'longitude': longitude,
                      'external_id': external_id,
                      'features': features})

    def get_property_details(self, response):
        self.position += 1
        item_loader = ListingLoader(response=response)

        address = response.meta.get('address')
        features = response.meta.get('features')

        pictures = response.xpath("//div[@class='property-image vinnybox']/@style").extract()
        for image in pictures:
            item_loader.add_value('images', image.split("(")[1].split(")")[0])

        # room_count = response.xpath(".//h1/text()").extract_first().split(" Bedroom ")[0]
        # rent_original = response.xpath(".//div[@class='mobile-price']/text()").extract_first().split("Per ")[1].rstrip("\n")
        # rent_type = rent_original.split(" - ")[0]
        # rent = rent_original.split(" - ")[1]
        # currency = rent[0]
        # rent = rent[1:]
        # rent = "".join(rent.split(","))        
        # if rent_type == "week":
        #     rent = int(rent)*4

        rent_original = response.xpath(".//div[@class='mobile-price']/text()").extract_first()
        currency = rent_original.split(' - ')[1][0]
        if 'Per week' in rent_original:
            rent = str(int(extract_number_only(rent_original))*4)
            item_loader.add_value('rent_string', currency + str(rent))
        else:
            item_loader.add_value('rent_string', rent_original)

        # description = response.xpath().extract()
        item_loader.add_value('external_link', response.meta.get('request_url'))
        item_loader.add_value('external_id', response.meta.get('external_id'))

        title = response.xpath('.//h1/text()').extract_first()
        item_loader.add_value("title", title)
        if len(re.findall(r"\d+[^\w]*bedroom", title.lower())) > 0:
            item_loader.add_value('room_count', extract_number_only(re.findall(r"\d+[^\w]*bedroom", title.lower())[0]))

        item_loader.add_xpath('description', ".//article[@class='cms-content cms-property']/p/text()")
        item_loader.add_value('address', response.meta.get('address'))

        apartment_types = ["appartement", "apartment", "flat",
                           "penthouse", "duplex", "triplex"]
        house_types = ['chalet', 'bungalow', 'maison', 'house', 'home', 'villa', 'holiday complex']
        studio_types = ["studio"]
        if any(i in response.xpath(".//h1/text()").extract_first().lower() for i in studio_types):
            item_loader.add_value('property_type', 'studio')
        elif any(i in response.xpath(".//h1/text()").extract_first().lower() for i in apartment_types):
            item_loader.add_value('property_type', 'apartment')
        elif any (i in response.xpath(".//h1/text()").extract_first().lower() for i in house_types):
            item_loader.add_value('property_type', 'house')
        else:
            return

        item_loader.add_value('latitude', response.meta.get('latitude'))
        item_loader.add_value('longitude', response.meta.get('longitude'))

        # if any(char.isdigit() for char in address):
        #     city = address.split(", ")[1].split(" ")[0]
        #     item_loader.add_value('city',city)
        #     zipcode = address.split(", ")[1].split(" ")[1] + " " + address.split(", ")[1].split(" ")[2]
        #     item_loader.add_value('zipcode',zipcode)

        city_zip = address.split(',')[-1]
        if any(char.isdigit() for char in city_zip):
            # satisfies https://www.mightyhouse.co.uk/properties/salford-road-galgate-lancaster/30127594
            if len(city_zip.split(' ')) == 4:
                item_loader.add_value('zipcode', ' '.join(city_zip.split(' ')[-2:]))
                item_loader.add_value('city', (city_zip.split(' ')[1]))
            # Futureproofing
            elif len(city_zip.split(' ')) == 3:
                item_loader.add_value('zipcode', city_zip)
                item_loader.add_value('zipcode', address.split(',')[-2])

        else:
            item_loader.add_value('city', city_zip)

        # http://mightyhouse.co.uk/properties/bay-view-court-station-road-lancaster/30085246"
        if "parking" in features.lower():
            item_loader.add_value('parking', True)
        
        if "terrace" in features.lower():
            item_loader.add_value('terrace', True)

        # "http://mightyhouse.co.uk/properties/bulgaria/24719259"
        if "pool" in features.lower():
            item_loader.add_value('swimming_pool', True)
        
        if "elevator" in features.lower():
            item_loader.add_value('elevator', True)
        
        if "balcony" in features.lower():
            item_loader.add_value('balcony', True)

        # "http://mightyhouse.co.uk/properties/salford-road-galgate-lancaster/30127594"
        if " furnished" in features.lower():
            item_loader.add_value('furnished', True)
        elif "unfurnished" in features.lower():
            item_loader.add_value('furnished', False)

        item_loader.add_value('landlord_name', 'Mighty House')
        item_loader.add_value('landlord_phone', "01524 548 888")
        item_loader.add_value('landlord_email', "enquiries@mightyhouse.co.uk")

        item_loader.add_value('position', self.position)
        item_loader.add_value("external_source", "Mightyhouse_PySpider_{}_{}".format(self.country, self.locale))
        yield item_loader.load_item()
