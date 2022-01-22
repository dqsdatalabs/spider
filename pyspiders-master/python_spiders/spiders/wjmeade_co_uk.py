# -*- coding: utf-8 -*-
# Author: Mehmet Kurtipek

import scrapy
from ..loaders import ListingLoader
from ..helper import remove_unicode_char, extract_rent_currency, format_date, extract_number_only
from geopy.geocoders import Nominatim
from ..user_agents import random_user_agent
import re
import lxml 
import js2xml
from scrapy import Selector
from math import ceil


class WjmeadeSpider(scrapy.Spider):
    name = "wjmeade_co_uk"
    allowed_domains = ['wjmeade.co.uk']
    start_urls = ['https://wjmeade.co.uk/']
    execution_type = 'testing'
    country = 'united_kingdom'
    locale = 'en'
    position = 0
    thousand_separator = ','
    scale_separator = '.'

    def start_requests(self):
        start_urls = [
            {'url': 'https://wjmeade.co.uk/listing?ok=1&page=1&property_type=rent&google_postcode=&postcode=&min_price=&max_price=&type=2,4,3&min_beds=&miles=&branch=&cityLat=&cityLng=&serach_area=&status=To%20let&orderby=',
             'property_type': 'house'},
            {'url': 'https://wjmeade.co.uk/listing?ok=1&page=1&property_type=rent&google_postcode=&postcode=&min_price=&max_price=&type=5&min_beds=&miles=&branch=&cityLat=&cityLng=&serach_area=&status=To%20let&orderby=',
             'property_type': 'apartment'},
            ]

        for url in start_urls:
            yield scrapy.Request(url=url.get('url'),
                                 callback=self.parse,
                                 meta={'request_url': url.get('url'),
                                       'property_type': url.get('property_type')})

    def parse(self, response, **kwargs):
        listings = []
        listings = response.xpath('//div[@class="property-text"]')
        for property_item in listings:
            url = property_item.xpath('.//a[contains(text(), "More Details")]/@href').extract_first()
            rent_string = property_item.xpath('.//span[@class="price_val"]/text()').extract_first()
            # room_count = property_item.xpath('.//i[@class="fa fa-bed"]/../text()').extract_first()
            # bathroom_count = property_item.xpath('.//i[@class="fa fa-bath"]/../text()').extract_first()
            
            yield scrapy.Request(
                url=url,
                callback=self.get_property_details,
                meta={'request_url': url,
                      'rent_string': rent_string,
                      # 'room_count':room_count,
                      # 'bathroom_count':bathroom_count,
                      'property_type': response.meta.get('property_type')
                      })
        
        if len(listings) == 15:
            current_page = re.findall(r"(?<=page=)\d+", response.meta["request_url"])[0]
            next_page_url = re.sub(r"(?<=page=)\d+", str(int(current_page) + 1), response.meta["request_url"])
            yield scrapy.Request(
                    url=next_page_url,
                    callback=self.parse,
                    meta={'request_url': next_page_url,'property_type': response.meta.get('property_type')})

    def get_property_details(self, response):

        square_meters = response.xpath('.//td[contains(text(), "Property Size")]/../td[2]/text()').extract_first()
        if square_meters:
            square_meters = str(ceil(int(square_meters)*0.092903))

        item_loader = ListingLoader(response=response)
        item_loader.add_value("external_source", "Wjmeade_PySpider_{}_{}".format(self.country, self.locale))
        item_loader.add_value('external_link', response.meta.get('request_url'))
        item_loader.add_value('external_id', response.meta.get('request_url').split('/')[-1])
        item_loader.add_value('property_type', response.meta.get('property_type'))
        item_loader.add_value('rent_string', '£' + response.meta.get('rent_string'))

        room_count = extract_number_only(response.xpath('.//i[@class="fa fa-bed"]/following-sibling::span/text()').extract_first())
        item_loader.add_value('room_count', room_count)

        bathroom_count = extract_number_only(response.xpath('.//i[@class="fa fa-bath"]/following-sibling::span/text()').extract_first())
        item_loader.add_value('bathroom_count', bathroom_count)

        available_date = response.xpath('//td[contains(text(), "Available From")]/..//td[2]/text()').extract_first()
        item_loader.add_value('available_date', format_date(available_date))

        item_loader.add_value('square_meters', square_meters)
        item_loader.add_xpath('description', './/div[@class="pro_desc"]//text()') 
        item_loader.add_xpath('images', './/div[@id="myCarousel"]//img/@src')
        item_loader.add_xpath('floor_plan_images', './/div[@id="profloor"]//img/@src')
        # item_loader.add_xpath('address', './/div[@class="col-md-6"]//h4/text()')
        item_loader.add_xpath('address', './/input[@id="property_address"]/@value')
        item_loader.add_xpath('title', './/div[@class="col-md-6"]//h4/text()')

        address = response.xpath('.//input[@id="property_address"]/@value').extract_first()
        if address:
            city_zip = address.split(",")
            zipcode = address.split(",")[-1].strip()
            
            item_loader.add_value('zipcode', zipcode)
        city=response.xpath('.//input[@id="property_address"]/@value').extract_first()
        if city:
            city=city.split(",")[-3].strip()
            item_loader.add_value('city', city)

            # zipcode = [text_i.strip() for text_i in city_zip if re.search(r"\d", text_i.lower()) and re.search(r"[a-z]", text_i.lower())]
            # if len(zipcode) > 0:
            #     item_loader.add_value('zipcode', zipcode[0])
            """
            city = [text_i.strip() for text_i in city_zip if not re.search(r"\d", text_i.lower())]
            if len(city) > 0:
                item_loader.add_value('city', city[-1])
            """

        javascript = response.xpath('.//script[contains(text(), "LatLng")]/text()').extract_first()
        if javascript:
            xml = lxml.etree.tostring(js2xml.parse(javascript), encoding='unicode')
            xml_selector = Selector(text=xml)
            item_loader.add_value('latitude', xml_selector.xpath('.//var[@name="latitude"]/number/@value').extract_first())
            item_loader.add_value('longitude', xml_selector.xpath('.//var[@name="longitude"]/number/@value').extract_first())

        features = ' '.join(response.xpath('.//div[@class="summary-table"]//li/a/text()').extract())

        # https://wjmeade.co.uk/property/16413
        if 'parking' in features.lower():
            item_loader.add_value('parking', True)

        # https://wjmeade.co.uk/property/10146
        if ' furnished' in features.lower() and 'partly furnished' not in features.lower() and ' unfurnished' not in features.lower():
            item_loader.add_value('furnished', True)
        elif ' furnished' not in features.lower() and 'partly furnished' not in features.lower() and ' unfurnished' in features.lower():
            item_loader.add_value('furnished', False)

        item_loader.add_xpath('landlord_name', './/div[@id="probranch"]//td[contains(text(),"Branch Manager")]/following-sibling::td/text()')
        item_loader.add_xpath('landlord_phone', './/div[@id="probranch"]//td[contains(text(),"Branch Number")]/following-sibling::td//text()')
        item_loader.add_xpath('landlord_email', './/div[@id="probranch"]//td[contains(text(),"Branch Email")]/following-sibling::td/text()')

        self.position += 1
        item_loader.add_value('position', self.position)
        yield item_loader.load_item()
