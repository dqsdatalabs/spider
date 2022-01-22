# -*- coding: utf-8 -*-
# Author: Mehmet Kurtipek

import scrapy,urllib
# from scrapy.http import FormRequest
# import js2xml
import re
import copy
from ..loaders import ListingLoader
from ..helper import remove_unicode_char, extract_rent_currency, format_date, extract_number_only
from datetime import date
from ..user_agents import random_user_agent


class LivInternationalSpider(scrapy.Spider):
    name = "liv_international_com"
    allowed_domains = ['liv-international.com']
    start_urls = ['http://www.liv-international.com/results.asp?displayperpage=231&pricetype=2&propbedt=+&view=grid']
    execution_type = 'testing'
    country = 'united_kingdom'
    locale = 'en'
    thousand_separator = ','
    scale_separator = '.'
    position = 0

    def start_requests(self):
        start_urls = ["http://www.liv-international.com/results.asp?pricetype=2"]
        for url in start_urls:
            yield scrapy.Request(url=url,
                                 callback=self.parse,
                                 meta={'page': 1,
                                       'request_url': url})

    def parse(self, response, **kwargs):
        listings = response.xpath('.//a[@class="prop-details"]/@href').extract()
        for url in listings:
            url = response.urljoin(url)
            yield scrapy.Request(
                url=url,
                callback=self.get_property_details,
                meta={'request_url': url}
            )

        next_page_url = response.xpath('.//a[contains(text(),"Next")]/@href').extract_first()
        if next_page_url:
            next_page_url = response.urljoin(next_page_url)
            yield scrapy.Request(
                url=next_page_url,
                callback=self.parse,
                meta={'request_url': next_page_url}
            )
    
    def get_property_details(self, response):

        external_link = response.meta.get('request_url')
        item_loader = ListingLoader(response=response)
        item_loader.add_value('external_link', external_link)
        item_loader.add_xpath('external_id', '//p[@class="col-xs-12 prop-ref"]/span/text()')

        address = response.xpath('//div[@class="detail-header col-xs-12 hidden-sm hidden-md hidden-lg"]/h1/a/text()').extract()
        address = [a_i.strip() for a_i in address]
        city = address[1]
        zipcode = address[2]
        address = ", ".join(address)
        item_loader.add_value('address', address)
        item_loader.add_value('title', address)
        item_loader.add_value('city', city)
        item_loader.add_value('zipcode', zipcode)

        rent_string = response.xpath('.//span[@class="priceask"]/text()').extract_first()
        if rent_string:
            item_loader.add_value('rent_string', rent_string.split('|')[-1].split(".")[0])

        item_loader.add_xpath('images', '//img[@class="sp-image img-responsive"]/@src')
        item_loader.add_xpath('floor_plan_images', './/div[contains(@class,"floorplan")]/a/img/@src')

        room_bathroom_count = response.xpath('//h3[contains(text(), "Bedrooms")]/text()').extract()
        for room in room_bathroom_count:
            if 'bedroom' in room.lower():
                item_loader.add_value('room_count', extract_number_only(room))
            elif 'bathroom' in room.lower():
                item_loader.add_value('bathroom_count', extract_number_only(room))

        item_loader.add_xpath('description', './/div[@class="col-md-6 detail-description"]/p/text()')

        if any(item in item_loader.get_output_value('description').lower() for item in ['studio', 'bedsit']):
            item_loader.add_value('property_type', 'studio')
        elif any(item in item_loader.get_output_value('description').lower() for item in ['apartment']):
            item_loader.add_value('property_type', 'apartment')
        elif any(item in item_loader.get_output_value('description').lower() for item in ['flat']):
            item_loader.add_value('property_type', 'apartment')
        elif any(item in item_loader.get_output_value('description').lower() for item in ['house']):
            item_loader.add_value('property_type', 'house')
        else:
            f_text = " ".join(response.xpath("//title//text()").getall())
            if get_p_type_string(f_text):
                prop_type = get_p_type_string(f_text)
                item_loader.add_value("property_type", prop_type)
            else:
                return

        latitude_longitude = response.xpath("//script[contains(.,'LatLng')]//text()").get()
        if latitude_longitude:
            latitude = latitude_longitude.split('LatLng(')[1].split(',')[0]
            longitude = latitude_longitude.split('LatLng(')[1].split(',')[1].split(')')[0].strip()      
            item_loader.add_value("longitude", longitude)
            item_loader.add_value("latitude", latitude)
        
        item_loader.add_value("landlord_name", "LIV INTERNATIONAL")
        item_loader.add_xpath('landlord_phone', './/div[contains(@class,"call-now")]/h3/text()')
        item_loader.add_value('landlord_email', "service@liv-international.com")
        
        # http://www.liv-international.com/property/herbert-crescent-knightsbridge-sw1x/livh-011159/1
        if "balcon" in item_loader.get_output_value('description').lower():
            item_loader.add_value('balcony', True)
        # http://www.liv-international.com/property/herbert-crescent-knightsbridge-sw1x/livh-011159/1
        if "pool" in item_loader.get_output_value('description').lower():
            item_loader.add_value('swimming_pool', True)

        # http://www.liv-international.com/property/golden-square-soho-w1f/livh-016290/1
        if "terrace" in item_loader.get_output_value('description').lower():
            item_loader.add_value('terrace', True)

        # http://www.liv-international.com/property/st-johns-wood-park-st-johns-wood-nw8/livh-004240/1'
        if "parking" in item_loader.get_output_value('description').lower():
            item_loader.add_value('parking', True)

        available_date = response.xpath('.//p[contains(@class,"available-date")]/text()').extract_first()
        if available_date:
            available_date=available_date.replace("\r", '').replace('\n', '').replace('\t', '').split('\xa0')[-1]
            item_loader.add_value('available_date', format_date(available_date))

        self.position += 1
        item_loader.add_value('position', self.position)
        item_loader.add_value("external_source", "LivInternational_PySpider_{}_{}".format(self.country, self.locale))
        return item_loader.load_item()

def get_p_type_string(p_type_string):
    if p_type_string and "student" in p_type_string.lower():
        return "student_apartment"
    elif p_type_string and "studio" in p_type_string.lower():
        return "studio"
    elif p_type_string and ("apartment" in p_type_string.lower() or "flat" in p_type_string.lower() or "etage" in p_type_string.lower()):
        return "apartment"
    elif p_type_string and ("house" in p_type_string.lower() or "terraced" in p_type_string.lower() or "woning" in p_type_string.lower() or "maisonette" in p_type_string.lower()):
        return "house"
    else:
        return None
