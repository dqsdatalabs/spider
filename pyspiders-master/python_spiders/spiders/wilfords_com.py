# -*- coding: utf-8 -*-
# Author: Mehmet Kurtipek

from time import strptime
import scrapy
from ..loaders import ListingLoader
from ..helper import remove_unicode_char, extract_rent_currency, format_date, extract_number_only
from math import ceil
from ..user_agents import random_user_agent
from geopy.geocoders import Nominatim
import lxml.etree
import js2xml
from scrapy import Selector
import requests
import re


class WilfordsSpider(scrapy.Spider):
    name = 'wilfords_com'
    allowed_domains = ['wilfords.com']
    start_urls = ['www.wilfords.com']
    execution_type = 'testing'
    country = 'united_kingdom'
    locale = 'en'
    thousand_separator = ','
    scale_separator = '.'
    position = 0

    def start_requests(self):
        start_url = ["http://www.wilfords.com/let/properties/","http://www.wilfords.com/short-let/properties"]
        for url in start_url:
            yield scrapy.Request(url=url,
                                 callback=self.parse,
                                 meta={'request_url': url})
                                 
    def parse(self, response, **kwargs):
        listings = response.xpath("//div[@class='description archive_desc']")
        for property_item in listings:
            url = property_item.xpath(".//a/@href").extract_first()
            yield scrapy.Request(
                url = url,
                callback=self.get_property_details,
                meta={'request_url' : url})

        next_page_url = response.xpath("//div[@class='pagination pagination-bottom']/a[contains(text(),'Next')]/@href").get()   
        if next_page_url:
            yield scrapy.Request(url=next_page_url, callback=self.parse)

    def get_property_details(self, response):
        item_loader = ListingLoader(response=response)
                           
        address = " ".join("".join(response.xpath(".//p[@class='location']/a/span/text()").extract()).split())
        if '\u2019' in address:
            address = address.replace('\u2019','\'')
        title = address
        rent_original = response.xpath(".//p[@class='price']/text()").extract_first()
        if 'week' in rent_original:
            rent, _ = extract_rent_currency(rent_original, "Wilfords_PySpider_{}_{}".format(self.country, self.locale), WilfordsSpider)
            currency = rent_original.strip()[0]
            item_loader.add_value('rent_string', currency + str(rent*4))
        else:
            item_loader.add_value('rent_string', rent_original)
        # rent_type = rent_original.split(" ")[2]
        # currency = rent_original.split(" ")[0][0]
        # rent =  rent_original.split(" ")[0][1:]
        # rent = "".join(rent.split(","))
        # if rent_type == "week":
        #     rent = int(rent)*4        
        square_meters = response.xpath(".//p[@class='repm-size']/text()").extract_first()
        if square_meters:
            square_meters = square_meters.split(": ")[1].split(" / ")
            for value in square_meters:
                if "sqm" in value:
                    square_meters = str(int(ceil(float(extract_number_only(value,thousand_separator=',',scale_separator='.'))))) 

        utilities = response.xpath(".//p[@class='additional-property_details']//text()").extract()
        if utilities != []:
            utilities = re.findall(r'Service Charge: £(\d(?:,)?\d+) p'," ".join(utilities))
            if utilities:
                item_loader.add_value('utilities', str(int(ceil(float(extract_number_only(utilities[0], thousand_separator=',', scale_separator='.')) / 12))))
        
        description = "".join(response.xpath(".//div[@class='description']//text()").extract())        

        apartment_types = ["appartement", "apartment", "flat",
                           "penthouse", "duplex", "triplex", "development"]
        house_types = ['chalet', 'bungalow', 'maison', 'house', 'home', 'villa']
        studio_types = ["studio"]
        property_type = None
        if any(i in description.lower() for i in studio_types):
            property_type = "studio"
        elif any(i in description.lower() for i in apartment_types):
            property_type = "apartment"
        elif any(i in description.lower() for i in house_types):
            property_type = "house"
        elif "room" in description.lower():
            property_type = "house"
            
        if property_type:
            item_loader.add_value('property_type', property_type)
        else: return
        
        item_loader.add_value('external_link', response.meta.get('request_url'))
        item_loader.add_value("title", title)
        item_loader.add_value('description', description)
        room_count = response.xpath('.//li[@class="num-beds"]/text()').extract_first()
        if room_count:
            item_loader.add_value('room_count', room_count)
        elif property_type == "studio":
            item_loader.add_value('room_count', "1")
        item_loader.add_xpath('bathroom_count', "//li[@class='num-baths']/text()")
        item_loader.add_value("square_meters", square_meters)
                          
        gallery_url = response.xpath(".//nav[@id='property-nav']/ul/li[@class='gallery']/a/@href").get()
        if gallery_url:
            images_request = requests.get(gallery_url)
            images_response = scrapy.Selector(images_request)
            images = images_response.xpath(".//div[@class='swiper-slide']//@src").extract()
            item_loader.add_value('images', images)

        floor_plan_url = response.xpath(".//nav[@id='property-nav']/ul/li[@class='floor-plans']/a/@href").get()
        if floor_plan_url:
            floor_images_request = requests.get(floor_plan_url)
            floor_images_response = scrapy.Selector(floor_images_request)
            floor_plan_images = floor_images_response.xpath(".//ul[@class='floorplans']//@src").extract_first()
            item_loader.add_value("floor_plan_images", floor_plan_images)

        map_url = response.xpath(".//nav[@id='property-nav']/ul/li[@class='map']/a/@href").get()
        if map_url:
            map_request = requests.get(map_url)
            map_response = scrapy.Selector(map_request)
            javascript = map_response.xpath('.//script[contains(text(),"latlng")]/text()').extract_first()
            if javascript:
                xml = lxml.etree.tostring(js2xml.parse(javascript), encoding='unicode')
                selector = Selector(text=xml)
                latitude = selector.xpath('.//var[@name="latlng"]//number/@value').extract()[0]
                longitude = selector.xpath('.//var[@name="latlng"]//number/@value').extract()[1]
                if latitude and longitude:
                    item_loader.add_value('latitude', latitude)
                    item_loader.add_value('longitude', longitude)

        item_loader.add_value('address', address)
        #Setting city as London since all the properties are in London
        item_loader.add_value('city', 'London')
        """
        geolocator = Nominatim(user_agent=random_user_agent())
        location = geolocator.geocode(item_loader.get_output_value('address'), addressdetails=True)
        if location:
            if not item_loader.get_output_value('latitude') and not item_loader.get_output_value('longitude'):
                item_loader.add_value('latitude', str(location.latitude))
                item_loader.add_value('longitude', str(location.longitude))
            if 'address' in location.raw:
                if 'postcode' in location.raw['address']:
                    item_loader.add_value('zipcode', location.raw['address']['postcode'])
                if 'city' in location.raw['address']:
                    item_loader.add_value('city', location.raw['address']['city'])
        """
                    
        item_loader.add_value('landlord_name', 'Wilfords London')
        item_loader.add_value('landlord_phone', "02073610400")
        item_loader.add_value('landlord_email', "info@wilfords.com")

        self.position += 1
        item_loader.add_value("position", self.position)
        item_loader.add_value("external_source", "Wilfords_PySpider_{}_{}".format(self.country, self.locale))
        yield item_loader.load_item()