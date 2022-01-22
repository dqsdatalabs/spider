# -*- coding: utf-8 -*-
# Author: Karan Katle
# Team: Sabertooth

import scrapy
from ..loaders import ListingLoader
from ..helper import extract_number_only, remove_white_spaces, convert_string_to_numeric
import re
from geopy.geocoders import Nominatim
import js2xml
import lxml.etree
from parsel import Selector
from ..user_agents import random_user_agent
import math


class Homes4uSpider(scrapy.Spider):
    name = "homes4u_co_uk"
    allowed_domains = ["homes4u.co.uk"]
    execution_type = 'testing'
    country = 'united_kingdom'
    locale = 'en'
    thousand_separator = ','
    scale_separator = '.'
    position = 0

    def start_requests(self):
        start_urls = [
            {'url': 'https://www.homes4u.co.uk/property-search/page/1/?address_keyword&radius=40&minimum_price&maximum_price&minimum_rent&maximum_rent&minimum_bedrooms&property_type=22&officeID&minimum_floor_area&maximum_floor_area&commercial_property_type&department=residential-lettings',
             'property_type': 'apartment'},
            {'url': 'https://www.homes4u.co.uk/property-search/page/1/?address_keyword=&radius=40&minimum_price=&maximum_price=&minimum_rent=&maximum_rent=&minimum_bedrooms=&property_type=18&officeID=&minimum_floor_area=&maximum_floor_area=&commercial_property_type=&department=residential-lettings',
             'property_type': 'house'},
            {'url': 'https://www.homes4u.co.uk/property-search/page/1/?address_keyword&radius=40&minimum_price&maximum_price&minimum_rent&maximum_rent&minimum_bedrooms&property_type=9&officeID&minimum_floor_area&maximum_floor_area&commercial_property_type&department=residential-lettings',
             'property_type': 'house'}
                    ]
        for url in start_urls:
            yield scrapy.Request(url=url.get('url'),
                                 callback=self.parse,
                                 meta={'request_url': url.get('url'),
                                       'property_type': url.get('property_type')}
                                 )

    def parse(self, response, **kwargs):
        listing = response.xpath('.//a[@class="img_tag"]/@href').extract()
        for property_url in listing:
            yield scrapy.Request(
                url=response.urljoin(property_url),
                callback=self.get_property_details,
                meta={'request_url': response.urljoin(property_url),
                      "property_type": response.meta["property_type"]}
            )

        if len(response.xpath('.//a[@class="img_tag"]')) > 0:
            current_page = re.findall(r"(?<=page/)\d+", response.meta["request_url"])[0]
            next_page_url = re.sub(r"(?<=page/)\d+", str(int(current_page) + 1), response.meta["request_url"])
            yield scrapy.Request(
                url=response.urljoin(next_page_url),
                callback=self.parse,
                meta={'request_url': next_page_url,
                      'property_type': response.meta["property_type"]}
                )
            
    def get_property_details(self, response):
        item_loader = ListingLoader(response=response)
        item_loader.add_value('external_link', response.meta.get('request_url'))
        item_loader.add_value('property_type', response.meta.get('property_type'))
        item_loader.add_xpath('description', './/*[@class="property-details"]//p//text()')
        item_loader.add_xpath('address', './/div[contains(@class, "sub_title")]/h2/text()')

        room_count = response.xpath('.//i[contains(@class,"bed")]/preceding-sibling::text()').extract_first()
        item_loader.add_value('room_count', str(extract_number_only(room_count)))

        bathroom_count = response.xpath('.//i[contains(@class,"bath")]/preceding-sibling::text()').extract_first()
        item_loader.add_value('bathroom_count', str(extract_number_only(bathroom_count)))

        rent_string = response.xpath('.//*[@class="sale"]/text()').extract_first()
        if 'pw' in rent_string:
            rent = convert_string_to_numeric(rent_string, Homes4uSpider)*4
            item_loader.add_value('rent_string', '£'+str(math.ceil(rent)))
        elif 'pcm' in rent_string:
            item_loader.add_value('rent_string', rent_string)

        item_loader.add_xpath('images', './/*[@data-fancybox="gallery"]//@src')

        # https://www.homes4u.co.uk/property/princess-street-manchester-city-centre-manchester/
        item_loader.add_xpath('floor_plan_images', './/a[contains(text(), "FLOORPLAN")]/@href')

        title = response.xpath('.//*[contains(@class,"sub_title")]//text()').extract()
        if title:
            item_loader.add_value('title', remove_white_spaces(''.join(title)))

        titles = item_loader.get_output_value('title').lower()
        if 'terrace' in titles:
            item_loader.add_value('terrace', True)
        if 'unfurnished' in titles:
            item_loader.add_value('furnished', False)
        elif 'furnished' in titles:
            item_loader.add_value('furnished', True)

        javascript = response.xpath('.//script[contains(text(),"LatLng")]/text()').extract_first()
        if javascript:
            xml = lxml.etree.tostring(js2xml.parse(javascript), encoding='unicode')
            selector = Selector(text=xml)
            lat_lng = selector.xpath('.//identifier[@name="LatLng"]/../../..//arguments/number/@value').extract()
            if len(lat_lng) > 0:
                lat_lng = lat_lng[0:2]
                item_loader.add_value('latitude', lat_lng[0])
                item_loader.add_value('longitude', lat_lng[1])

        item_loader.add_value("external_source", "Homes4u_PySpider_{}_{}".format(self.country, self.locale))
        item_loader.add_value('landlord_name', 'homes4u')
        item_loader.add_xpath('landlord_phone', './/*[@title="Call Us"]/text()')
        item_loader.add_xpath('landlord_email', './/*[@title="Mail Us"]/text()')
        
        self.position += 1
        item_loader.add_value('position', self.position)
        yield item_loader.load_item()
