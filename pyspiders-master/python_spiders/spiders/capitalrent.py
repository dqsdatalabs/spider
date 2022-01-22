# -*- coding: utf-8 -*-
# Author: Mehmet Kurtipek

import scrapy
import js2xml
import re
from ..loaders import ListingLoader
from ..helper import remove_white_spaces
# from geopy.geocoders import Nominatim
from ..user_agents import random_user_agent
import lxml.etree
from parsel import Selector


class CapitalrentSpider(scrapy.Spider):
    name = 'capitalrent_eu'
    allowed_domains = ['capitalrent.eu']
    start_urls = ['https://www.capitalrent.eu/']
    execution_type = 'testing'
    country = 'belgium'
    locale = 'en'
    thousand_separator = ','
    scale_separator = '.'
    position = 0

    def start_requests(self):
        start_urls = [
            {
                "url": 'https://www.capitalrent.eu/page/1/?search-listings=true&ct_property_type=duplex',
                "property_type": "apartment",
            },
            {
                "url": 'https://www.capitalrent.eu/page/1/?search-listings=true&ct_property_type=flat',
                "property_type": "apartment",
            },
            {
                "url": 'https://www.capitalrent.eu/page/1/?search-listings=true&ct_property_type=ground-floor',
                "property_type": "apartment",
            },
            {
                "url": 'https://www.capitalrent.eu/page/1/?search-listings=true&ct_property_type=house',
                "property_type": "house",
            },
            {
                "url": 'https://www.capitalrent.eu/page/1/?search-listings=true&ct_property_type=studio',
                "property_type": "studio",
            }
        ]
        for url in start_urls:
            yield scrapy.Request(url=url.get("url"),
                                 callback=self.parse,
                                 meta={'response_url': url.get("url"),
                                       "property_type": url.get("property_type")})
            
    def parse(self, response, **kwargs):
        listings = response.xpath('.//a[@class="listing-featured-image"]')
        for listing in listings:
            property_url = listing.xpath('./@href').extract_first()
            yield scrapy.Request(url=property_url,
                                 callback=self.get_property_details,
                                 meta={'response_url': property_url,
                                       "property_type": response.meta["property_type"]})
        
        next_page_url = response.xpath('.//li[@id="next-page-link"]//a/@href').extract_first()
        if next_page_url:
            yield scrapy.Request(
                                url=next_page_url,
                                callback=self.parse,
                                meta={'response_url': next_page_url,
                                      "property_type": response.meta["property_type"]}
                                )
            
    def get_property_details(self, response):
        item_loader = ListingLoader(response=response)
        item_loader.add_value('external_link', response.meta.get('response_url'))
        item_loader.add_xpath('external_id', './/*[contains(text(),"Property ID")]/following-sibling::*/text()')
        item_loader.add_xpath('rent_string', './/*[@class="listing-price"]/text()')
        item_loader.add_xpath('address', './/*[@class="location marB0"]/text()')
        item_loader.add_xpath('title', './/*[@id="listing-title"]/text()')
        item_loader.add_value('property_type', response.meta["property_type"])
        item_loader.add_xpath('images', './/*[contains(@class,"single-image")]/img/@src')
        item_loader.add_xpath('images', './/div[@class="owl-carousel"]//img/@src')
        item_loader.add_value('landlord_name', 'CAPITAL RENT')
        item_loader.add_value('landlord_phone', '+32 (0)2 345 00 00')
        item_loader.add_value('landlord_email', 'info@capitalrent.eu')
        item_loader.add_xpath('description', './/*[@id="listing-content"]/p/text()')
        item_loader.add_xpath('bathroom_count', './/span[contains(text(),"Bath")]/following-sibling::span/text()')
        item_loader.add_xpath('square_meters', './/span[contains(text(),"Area")]/following-sibling::span/text()')
        
        address = response.xpath("//p[contains(@class,'location')]/text()").get()
        if address:
            city = address.split(",")[1].strip()
            if not city.isdigit():
                item_loader.add_value("city", city.split("(")[0].strip())
                item_loader.add_value("zipcode", address.split(")")[1].split(",")[0].strip())
            else:
                item_loader.add_value("zipcode", city)
                item_loader.add_value("city", address.split(",")[0].strip())
                
        
        room_count = response.xpath('.//span[contains(text(),"Bed")]/following-sibling::span/text()').extract_first()
        if room_count:
            item_loader.add_value('room_count', room_count.split(',')[0])
        elif item_loader.get_output_value('property_type') == 'studio':
            item_loader.add_value('room_count', '1')
        energy=response.xpath('.//span[contains(text(),"Energy Class")]/following-sibling::span/text()').extract_first()
        if energy:
            item_loader.add_value('energy_label', energy)
        else:
            item_loader.add_xpath('energy_label', './/span[contains(text(),"Global Energy Performance")]/following-sibling::span/text()')

        furnished = response.xpath('.//li[contains(text(),"Furnished")]/text()').extract_first()
        if furnished:
            if "yes" in furnished.lower():
                item_loader.add_value('furnished', True)
            else:
                item_loader.add_value('furnished', False)

        elevator = response.xpath('.//li[contains(text(),"Elevator")]/text()').extract_first()
        if elevator:
            if "yes" in elevator.lower():
                item_loader.add_value('elevator', True)
            else:
                item_loader.add_value('elevator', False)

        terrace = response.xpath('.//li[contains(text(),"Terrace")]/text()').extract_first()
        if terrace:
            if "yes" in terrace.lower():
                item_loader.add_value('terrace', True)
            else:
                item_loader.add_value('terrace', False)

        javascript = response.xpath('.//script[contains(text(), "LatLng")]/text()').extract_first()
        if javascript:
            xml = lxml.etree.tostring(js2xml.parse(javascript), encoding='unicode')
            selector = Selector(text=xml)
            lat_lng = selector.xpath('.//identifier[@name="setMapAddress"]/../..//arguments//text()').extract()
            lat_lng = lat_lng[0].split(',')
            if len(lat_lng) == 2:
                item_loader.add_value('latitude', lat_lng[0])
                item_loader.add_value('longitude', lat_lng[1])
                # geolocator = Nominatim(user_agent=random_user_agent())
                # location = geolocator.reverse(", ".join(lat_lng))
                # if location:
                #     item_loader.add_value('zipcode', remove_white_spaces(location.address.split(',')[-2]))
                #     item_loader.add_value('city', remove_white_spaces(location.address.split(',')[-4]))
        
        parking = response.xpath("//span[contains(.,'Parking')]/following-sibling::span/text()[.!='0']").get()
        if parking:
            item_loader.add_value("parking", True)
        
        self.position += 1
        item_loader.add_value('position', self.position)
        item_loader.add_value("external_source", "Capitalrent_PySpider_{}_{}".format(self.country, self.locale))
        yield item_loader.load_item()
