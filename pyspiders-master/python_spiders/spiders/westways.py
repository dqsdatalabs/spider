# -*- coding: utf-8 -*-
# Author: Mehmet Kurtipek

import scrapy
import re
from ..loaders import ListingLoader

class WestwaysSpider(scrapy.Spider):
    name = "westways"
    allowed_domains = ["westways.co.uk"]
    start_urls = (
        'http://www.westways.co.uk/',
    )
    execution_type = 'testing'
    country = 'united_kingdom'
    locale ='en'
    thousand_separator=','
    scale_separator='.'

    def start_requests(self):
        start_urls = "https://www.westways.co.uk/properties/lettings"
        yield scrapy.Request( url=start_urls, callback=self.parse, dont_filter=True )

    def parse(self, response, **kwargs):
        for page in range(0, 8):
            link = 'https://www.westways.co.uk/properties/lettings/page-{}'.format(page)
            yield scrapy.Request(url=link, callback=self.get_list_urls, dont_filter=True) 
    
    def get_list_urls(self, response):
        links = response.xpath('//a[@class="property-preview__link"]')
        for link in links: 
            url = response.urljoin(link.xpath('./@href').extract_first())
            yield scrapy.Request(url=url, callback=self.get_property_details, dont_filter=True)
   
    def get_property_details(self, response):
        # parse details of the propery
        property_type = 'apartment'
        external_link = response.url
        external_id = str(response.url.split('/')[-2])
        address = response.xpath('//h1[contains(@class, "property-page__address")]/text()').extract_first()
        zipcode_city = address.split(', ')[1]
        zipcode = zipcode_city.split(' ')[-1]
        room_count = response.xpath('//div[@class="property-page__aside"]//span[@class="property-griditem__bedrooms"]/text()').extract_first('').strip()
        bathrooms = response.xpath('//div[@class="property-page__aside"]//span[@class="property-griditem__bathrooms"]/text()').extract_first('').strip() 
        lat = response.xpath('//div[@id="propertyMap"]/@data-lat').extract_first()
        lng = response.xpath('//div[@id="propertyMap"]/@data-lng').extract_first()
        rent_string = ''.join(response.xpath('//div[contains(@class, "property-page__price")]/text()').extract())
        features = ''.join(response.xpath('//ul[contains(@class, "property-page__features")]/li/text()').extract())
        city = "London"
        if address and int(room_count) > 0:
            item_loader = ListingLoader(response=response)
            if 'park' in features.lower():
                item_loader.add_value('parking', True)
            elif 'balcony' in features.lower():
                item_loader.add_value('balcony', True)
            item_loader.add_value('property_type', property_type)
            item_loader.add_value('external_link', external_link)
            item_loader.add_value('external_id', external_id)
            item_loader.add_value('address', address)
            item_loader.add_value('city', city)
            item_loader.add_xpath('title', '//title/text()')
            item_loader.add_value('zipcode', zipcode)
            item_loader.add_xpath('description', '//span[contains(@class, "property-page__description")]//text()')
            item_loader.add_value('rent_string', rent_string)
            item_loader.add_xpath('images', '//div[contains(@class, "carousel__gallery")]/div/@data-bg-src')
            if room_count:
                item_loader.add_value('room_count', str(room_count))
            item_loader.add_value('latitude', str(lat))
            item_loader.add_value('longitude', str(lng))
            if bathrooms: 
                item_loader.add_value('bathroom_count', str(bathrooms))
            item_loader.add_value('landlord_name', 'Robin Chalk')
            item_loader.add_value('landlord_email', 'info@westways.co.uk')
            item_loader.add_value('landlord_phone', '+44 (0)20 7286 5757')
            item_loader.add_value("external_source", "{}_PySpider_{}_{}".format(self.name.capitalize(), self.country, self.locale))
            
            status = response.xpath("//div[@class='property-page__status']/text()").get()
            if not status:
                yield item_loader.load_item()
