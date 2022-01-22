# -*- coding: utf-8 -*-
# Author: Praveen Chaudhary
# Team: Sabertooth
import json

import scrapy

from ..helper import extract_number_only
from ..loaders import ListingLoader


class ProfilesEstatesCoUkSpider(scrapy.Spider):
    name = 'profiles-estates_co_uk'
    allowed_domains = ['profiles-estates.co.uk']
    start_urls = ['https://www.profiles-estates.co.uk/']
    execution_type = 'testing'
    country = 'united_kingdom'
    locale = 'en'
    thousand_separator = ','
    scale_separator = '.'

    def start_requests(self):
        self.position = 0
        self.page = 1
        start_urls = ['https://www.profiles-estates.co.uk/search.ljson?channel=lettings&fragment=page-1']
        for url in start_urls:
            yield scrapy.Request(
                url=url,
                callback=self.parse,
                meta={
                    'request_url': url})

    def parse(self, response, **kwargs):
        ljson_temp_json = response.text
        temp_json = []
        for line in ljson_temp_json.split("\n"):
            temp_json.append(json.loads(line))
        if temp_json[0]["properties"] is not None:
            for propert in temp_json[0]["properties"]:
                if propert['status'] !="Let agreed":
                    url = f"https://www.profiles-estates.co.uk{propert['property_url']}"
                    yield scrapy.Request(
                        url=url,
                        callback=self.get_property_details,
                        meta={'request_url': url,
                              'data': propert
                              }
                    )
            if temp_json[0]["pagination"]["has_next_page"]:
                self.page += 1
                next_url = f"https://www.profiles-estates.co.uk/search.ljson?channel=lettings&fragment=page-{self.page}"
                yield scrapy.Request(
                    url=next_url,
                    callback=self.parse,
                    meta={'request_url': next_url, }
                )

    def get_property_details(self, response):
        item_loader = ListingLoader(response=response)
        item_loader.add_value('external_link', response.meta.get('request_url'))
        item_loader.add_value('external_id', str(response.meta.get('data')['property_id']))

        item_loader.add_value('address', response.meta.get('data')['display_address'])
        # zip not alvailable
        item_loader.add_value('city', response.meta.get('data')['display_address'].split(", ")[-1])
        item_loader.add_value('room_count', response.meta.get('data')['bedrooms'])
        item_loader.add_value('bathroom_count', response.meta.get('data')['bathrooms'])
        item_loader.add_value('longitude', str(response.meta.get('data')['lng']))
        item_loader.add_value('latitude', str(response.meta.get('data')['lat']))
        item_loader.add_xpath('title', './/title/text()')
        item_loader.add_xpath('description', './/div[@class="property--content"]/p/text()')
        item_loader.add_value('rent_string', f"£{response.meta.get('data')['price_value']}")
        item_loader.add_xpath('images', './/div[@class="rsContent"]//img/@src')

        # landlord details
        landlord_name=response.xpath('.//p[@class="branch--title"]/a/text()').extract_first()
        landlord_phone=response.xpath('.//a[@class="branch_phone"]/text()').extract_first()
        item_loader.add_value('landlord_name', landlord_name)
        item_loader.add_value('landlord_email', 'profilesea@aol.com')
        item_loader.add_value('landlord_phone', landlord_phone)
        item_loader.add_xpath('floor_plan_images', './/div[@id="floorplan"]//img/@src')

        if response.xpath('.//strong[contains(text(),"Garage")]') or response.xpath('.//strong[contains(text(),"garage")]') or 'parking' in item_loader.get_output_value('description').lower() or 'garage' in item_loader.get_output_value('description').lower():
            item_loader.add_value('parking', True)

        if 'washing machine' in item_loader.get_output_value('description').lower():
            item_loader.add_value('washing_machine', True)

        if 'elevator' in item_loader.get_output_value('description').lower():
            item_loader.add_value('elevator', True)

        if 'pool' in item_loader.get_output_value('description').lower():
            item_loader.add_value('swimming_pool', True)

        if " furnished" in item_loader.get_output_value('description').lower():
            item_loader.add_value('furnished', True)

        if "dishwasher" in item_loader.get_output_value('description').lower():
            item_loader.add_value('dishwasher', True)

        if "balcony" in item_loader.get_output_value('description').lower():
            item_loader.add_value('balcony', True)

        apartment_types = ["appartement", "apartment", "flat",
                           "penthouse", "duplex", "triplex"]
        house_types = ['chalet', 'bungalow', 'maison', 'house',
                       'home', ' villa ', 'cottage', 'semi-detached']
        studio_types = ["studio"]
        if any(i in response.meta.get('data')['property_type'].lower() for i in house_types):
            item_loader.add_value('property_type', 'house')
        elif any(i in response.meta.get('data')['property_type'].lower() for i in apartment_types):
            item_loader.add_value('property_type', 'apartment')
        elif any(i in response.meta.get('data')['property_type'].lower() for i in studio_types):
            item_loader.add_value('property_type', 'studio')
        else:
            return

        # note can't extract epc from image
        self.position += 1
        item_loader.add_value('position', self.position)

        item_loader.add_value("external_source",
                              "Profiles_Estates_Co_Uk_PySpider_{}_{}".format(self.country, self.locale))
        yield item_loader.load_item()
