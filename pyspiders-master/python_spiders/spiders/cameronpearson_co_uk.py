# -*- coding: utf-8 -*-
# Author: Mehmet Kurtipek


import scrapy
from ..loaders import ListingLoader
from ..helper import extract_number_only, remove_white_spaces
from geopy.geocoders import Nominatim
from ..user_agents import random_user_agent
import re
from math import ceil
import js2xml
import lxml.etree
from scrapy import Selector


class CameronpearsonSpider(scrapy.Spider):
    name = "cameronpearson_co_uk"
    allowed_domains = ["cameronpearson.co.uk"]
    execution_type = 'testing'
    country = 'united_kingdom'
    locale = 'en'
    thousand_separator = ','
    scale_separator = '.'
    position = 0

    def start_requests(self):
        start_urls = [            
            {'url': 'https://www.cameronpearson.co.uk/residential/lettings/search/results/list/desc/1?properties[property_types_category_id][]=2',
             'property_type': 'apartment'},
            {'url': 'https://www.cameronpearson.co.uk/residential/lettings/search/results/list/desc/1?properties[property_types_category_id][]=1',
             'property_type': 'house'}
            ]
        for url in start_urls:
            yield scrapy.Request(url=url.get('url'),
                                 callback=self.parse,
                                 meta={'request_url': url.get('url'),
                                       'property_type': url.get('property_type')})
            
    def parse(self, response, **kwargs):
        for property_url in response.xpath('.//*[@class="search-result-list"]/li/a/@href').extract():
            yield scrapy.Request(
                url='https://www.cameronpearson.co.uk'+property_url,
                callback=self.get_property_details,
                meta={'request_url': 'https://www.cameronpearson.co.uk' + property_url,
                      "property_type": response.meta["property_type"]})
            # break
        
        if len(response.xpath('.//*[@class="search-result-list"]/li/a')) > 0:
            current_page = re.findall(r"(?<=desc/)\d+", response.meta["request_url"])[0]
            next_page_url = re.sub(r"(?<=desc/)\d+", str(int(current_page) + 1), response.meta["request_url"])
            yield scrapy.Request(
                url=response.urljoin(next_page_url),
                callback=self.parse,
                meta={'request_url': next_page_url,
                      "property_type": response.meta["property_type"]}
            )

    def get_property_details(self, response):
        item_loader = ListingLoader(response=response)

        external_link = response.meta["request_url"]
        item_loader.add_value('external_link', external_link)
        item_loader.add_value('external_id', external_link.split("=")[-1])
        title = response.xpath('.//*[@class="header"]/text()').extract_first()
        item_loader.add_value('property_type', response.meta.get('property_type'))
        rent_string = response.xpath('.//*[@class="price"]/text()[1]').extract_first()
        rent_int = response.xpath('.//*[@class="price"]/abbr/text()').extract_first()
        if 'pw' in rent_int or 'pw' in rent_string:
            rent = str(ceil(float(extract_number_only(rent_string,thousand_separator=',', scale_separator='.'))*4))
            item_loader.add_value('rent_string', '£'+rent)
        else:
            item_loader.add_value('rent_string', rent_string)

        item_loader.add_xpath('title', './/*[@class="header"]/text()')
        item_loader.add_xpath('description', './/*[@id="property-content"]//p/text()')

        room_count = response.xpath('.//*[@id="propbeds"]/text()').extract_first()
        if room_count and extract_number_only(room_count) != '0':
            item_loader.add_value('room_count', extract_number_only(room_count))
        else:
            room_count = response.xpath('.//*[@class="extra"]/text()').extract_first()
            if room_count and extract_number_only(room_count) != '0':
                item_loader.add_value('room_count', extract_number_only(room_count))

        bathroom_count = response.xpath('.//*[@id="propbaths"]/text()').extract_first()
        if bathroom_count:
            item_loader.add_value('bathroom_count', extract_number_only(bathroom_count))

        if title:
            item_loader.add_value('address', remove_white_spaces(title))
            zipcode = item_loader.get_output_value('address').split(',')[-1].strip().split(' ')[-1].strip()
            if zipcode and not remove_white_spaces(zipcode).isalpha():
                item_loader.add_value('zipcode', remove_white_spaces(zipcode))

        city = re.search(r'(?<=lettings\/).+(?=\/)', external_link)
        if city:
            item_loader.add_value('city', city.group().capitalize().replace('-', ' '))

        javascript = response.xpath('.//script[contains(text(),"propertyLat")]/text()').extract_first()
        if javascript:
            xml = lxml.etree.tostring(js2xml.parse(javascript), encoding='unicode')
            selector = Selector(text=xml)
            lat = selector.xpath('.//var[@name="propertyLat"]/number/@value').extract_first()
            lng = selector.xpath('.//var[@name="propertyLong"]/number/@value').extract_first()
            if lat and lng:
                item_loader.add_value('latitude', lat)
                item_loader.add_value('longitude', lng)
        else:
            geolocator = Nominatim(user_agent=random_user_agent())
            location = geolocator.geocode(item_loader.get_output_value('address'))
            if location:
                item_loader.add_value('latitude', str(location.latitude))
                item_loader.add_value('longitude', str(location.longitude))

        item_loader.add_xpath('images', './/*[@id="tab-photos"]//img/@src')
        floor_plan_images = response.xpath('.//*[@id="tab-floorplans"]//img/@src').extract()
        if floor_plan_images:
            floor_plan_images = ['https://www.cameronpearson.co.uk'+floor_plan_image for floor_plan_image in floor_plan_images]
            item_loader.add_value('floor_plan_images', floor_plan_images)

        details = ' '.join(response.xpath('.//*[@class="right"]//li/text()').extract())
        if details:
            if 'dishwasher' in details.lower():
                item_loader.add_value('dishwasher', True)
            if 'balcony' in details.lower():
                item_loader.add_value('balcony', True)
            if 'terrace' in details.lower():
                item_loader.add_value('terrace', True)
            if 'lift' in details.lower() or 'elevator' in details.lower():
                item_loader.add_value('elevator', True)
            if 'parking' in details.lower():
                item_loader.add_value('parking', True)
            if 'swimming pool' in details.lower():
                item_loader.add_value('swimming_pool', True)
            if 'washing machine' in details.lower():
                item_loader.add_value('washing_machine', True)
            if 'unfurnished' in details.lower():
                item_loader.add_value('furnished', False)
            elif 'furnished' in details.lower():
                item_loader.add_value('furnished', True)

        item_loader.add_value('landlord_name', 'Cameron Pearson')
        item_loader.add_value('landlord_phone', '020 7373 3933')
        item_loader.add_value('landlord_email', 'enquiries@cameronpearson.co.uk')

        item_loader.add_value("external_source", "Cameronpearson_PySpider_{}_{}".format(self.country, self.locale))
        self.position += 1
        item_loader.add_value('position', self.position)
        yield item_loader.load_item()
