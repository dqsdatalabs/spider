# -*- coding: utf-8 -*-
# Author: Pankaj Kalania
# Team: Sabertooth

import scrapy, copy, urllib
from ..loaders import ListingLoader
from ..helper import  extract_rent_currency
from geopy.geocoders import Nominatim
import re
import js2xml
import lxml.etree
from parsel import Selector
from ..user_agents import random_user_agent
import math


class OliverJamesLondonSpider(scrapy.Spider):
    name = 'oliverjames_london_com'
    allowed_domains = ['oliverjames-london.com']
    start_urls = ['http://www.oliverjames-london.com']
    execution_type = 'testing'
    country = 'united_kingdom'
    locale = 'en'
    api_url = "https://oliverjames-london.com/properties/page/1/"
    params = {'property-type': 'rentals',
              'sortby': 'price-high-low',
              'ap': '',
              'price-min': 'no-min',
              'price-max': 'no-max',
              'min-beds': 'any'}
    position = 0
    thousand_separator = ','
    scale_separator = '.'
        
    def start_requests(self):
        start_urls = [self.api_url + "?" + urllib.parse.urlencode(self.params)]
        for url in start_urls:
            yield scrapy.Request(url=url,
                                 callback=self.parse,
                                 meta={'request_url': url})
            
    def parse(self, response, **kwargs):
        for property_url in response.xpath('.//*[contains(@class,"properties-container")]//a/@href').extract():
            yield scrapy.Request(
                url=response.urljoin(property_url),
                callback=self.get_property_details,
                meta={'request_url': response.urljoin(property_url)}
            )

        if len(response.xpath('.//*[contains(@class,"properties-container")]//a')) > 0:
            current_url = response.meta['request_url']
            current_page = re.findall(r"(?<=page/)\d+", current_url)[0]
            next_page = re.sub(r"(?<=page/)\d+", str(int(current_page) + 1), current_url)
            next_page_url = next_page + "?" + urllib.parse.urlencode(self.params)
            yield scrapy.Request(
                url=next_page_url,
                callback=self.parse,
                meta={'request_url': next_page_url}
            )
        
    def get_property_details(self, response):
        item_loader = ListingLoader(response=response)
        item_loader.add_value('external_link', response.meta["request_url"])
        item_loader.add_xpath('title', './/h1/text()')
        item_loader.add_xpath('description', './/*[contains(@class,"half")]/following-sibling::div/p/text()')
        
        apartment_types = ["lejlighed", "appartement", "apartment", "piso", "flat", "atico",
                           "penthouse", "duplex", "dakappartement", "triplex"]
        house_types = ['hus', 'chalet', 'bungalow', 'maison', 'house', 'home', 'villa', 'huis', 'cottage']
        studio_types = ["studio"]
        
        # property_type
        # 3 properties get ignored because im unsure how to extract property type from them
        if any(i in ' '.join(item_loader.get_output_value('description').split('.')[:3]).lower() for i in studio_types):
            item_loader.add_value('property_type', 'studio')
        elif any(i in ' '.join(item_loader.get_output_value('description').split('.')[:3]).lower() for i in apartment_types):
            item_loader.add_value('property_type', 'apartment')
        elif any(i in ' '.join(item_loader.get_output_value('description').split('.')[:3]).lower() for i in house_types):
            item_loader.add_value('property_type', 'house')
        else:
            return

        addrs = response.xpath('.//div[@class="half"]//*[contains(text(),"Location")]/text()').extract_first()
        if addrs:
            item_loader.add_value('zipcode', addrs.split(' - ')[1])
            item_loader.add_value('city', addrs.split(' - ')[0].replace('Location ', ''))
        
        rent = response.xpath('.//p[@class="property-price"]/text()').extract_first()
        if rent:
            rent_string = '£ ' + str(extract_rent_currency(rent, OliverJamesLondonSpider)[0]*4)
            item_loader.add_value('rent_string', rent_string)
        
        item_loader.add_xpath('room_count', './/*[contains(@class,"fa-bed")]/following-sibling::strong/text()')
        item_loader.add_xpath('bathroom_count', './/*[contains(@class,"fa-bath")]/following-sibling::strong/text()')
        square_meters = response.xpath('.//*[contains(@class,"fa-ruler-combined")]/following-sibling::strong/text()').extract_first()
        if square_meters:
            square = square_meters.replace('m2', '').replace(',', '')
            item_loader.add_value('square_meters', str(math.ceil(float(square))))
            
        images = response.xpath('.//*[@id="property-slider"]/div/@style').extract()
        img_url = list(map(lambda x: re.search(r'\((.*?)\)', x).group(1).strip(), images))
        item_loader.add_value('images', list(set(img_url)))
        item_loader.add_xpath('floor_plan_images', './/*[@class="floor-plans container"]//img/@src')
        
        item_loader.add_value('landlord_name', 'Oliver James')
        item_loader.add_value('landlord_phone', '020 7866 2448')
        item_loader.add_value('landlord_email', 'info@oliverjames-london.com')
        
        javascript = response.xpath('.//script[contains(text(),"lng")]/text()').extract_first()
        if javascript:
            xml = lxml.etree.tostring(js2xml.parse(javascript), encoding='unicode')
            selector = Selector(text=xml)
            latitude = selector.xpath('.//property[@name="lat"]/number/@value').extract_first()
            longitude = selector.xpath('.//property[@name="lng"]/number/@value').extract_first()
            if latitude and longitude:
                item_loader.add_value('latitude', latitude)
                item_loader.add_value('longitude', longitude)
                geolocator = Nominatim(user_agent=random_user_agent())
                coordinates = ', '.join([latitude]+[longitude])
                location = geolocator.reverse(coordinates)
                if location:
                    item_loader.add_value('address', location.address)
                
        self.position += 1
        item_loader.add_value('position', self.position)
        item_loader.add_value("external_source", "OliverjamesLondon_PySpider_{}_{}".format(self.country, self.locale))
        yield item_loader.load_item()
