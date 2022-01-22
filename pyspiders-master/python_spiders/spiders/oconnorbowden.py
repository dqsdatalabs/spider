# -*- coding: utf-8 -*-
# Author: Mehmet Kurtipek

import scrapy
import re
from ..loaders import ListingLoader

class OconnorbowdenSpider(scrapy.Spider):
    name = "oconnorbowden"
    allowed_domains = ["www.oconnorbowden.co.uk"]
    start_urls = (
        'http://www.www.oconnorbowden.co.uk/',
    )
    execution_type = 'testing'
    country = 'united_kingdom'
    locale ='en'
    thousand_separator=','
    scale_separator='.'

    def start_requests(self):
        start_urls = [
            {'url': 'http://www.oconnorbowden.co.uk/estate-agents-lettings-and-management-greater-manchester-and-Cheshire/search_results/?department=residential-lettings&minimum_price=&maximum_price=&minimum_rent=&maximum_rent=&minimum_bedrooms=&property_type=24&minimum_floor_area=&maximum_floor_area=&commercial_property_type=&pgp=&post_type=property&view=grid', 'property_type': 'apartment'},
            {'url': 'http://www.oconnorbowden.co.uk/estate-agents-lettings-and-management-greater-manchester-and-Cheshire/search_results/?department=residential-lettings&minimum_price=&maximum_price=&minimum_rent=&maximum_rent=&minimum_bedrooms=&property_type=23&minimum_floor_area=&maximum_floor_area=&commercial_property_type=&view=grid&pgp=&post_type=property', 'property_type': 'apartment'},
            {'url': 'http://www.oconnorbowden.co.uk/estate-agents-lettings-and-management-greater-manchester-and-Cheshire/search_results/?department=residential-lettings&minimum_price=&maximum_price=&minimum_rent=&maximum_rent=&minimum_bedrooms=&property_type=11&minimum_floor_area=&maximum_floor_area=&commercial_property_type=&view=&pgp=&post_type=property', 'property_type': 'house'},
            {'url': 'http://www.oconnorbowden.co.uk/estate-agents-lettings-and-management-greater-manchester-and-Cheshire/search_results/?department=residential-lettings&minimum_price=&maximum_price=&minimum_rent=&maximum_rent=&minimum_bedrooms=&property_type=10&minimum_floor_area=&maximum_floor_area=&commercial_property_type=&view=&pgp=&post_type=property', 'property_type': 'house'}
        ]
        for url in start_urls:
            yield scrapy.Request(
                url=url.get('url'),
                callback=self.parse, 
                meta={'property_type': url.get('property_type')},
                dont_filter=True
            )

    def parse(self, response, **kwargs):
        # parse the detail url
        links = response.xpath('//ul[contains(@class, "properties")]/li/div/div/a')
        for link in links: 
            url = response.urljoin(link.xpath('./@href').extract_first())
            yield scrapy.Request(url=url, callback=self.get_property_details, dont_filter=True, meta={'property_type': response.meta.get('property_type')})

    def get_property_details(self, response):
        external_link = response.url
        property_type = response.meta.get('property_type')
        address = response.xpath('//span[@class="address"]/text()').extract_first('').strip()
        city = address.split(', ')[-2]
        zipcode = address.split(', ')[-1]    
        external_id = response.xpath('//span[@class="reference-no"]/text()').extract_first('').strip().split(': ')[-1]
        room_count = response.xpath('//li[@class="bedrooms"]//text()').extract_first('').strip()
        bathrooms = response.xpath('//li[@class="bathrooms"]//text()').extract_first('').strip() 
        rent_string = response.xpath('//div[@class="price"]/text()').extract_first('').strip()
       
        item_loader = ListingLoader(response=response)
        latitude = response.xpath("//div[contains(@class,'marker')]//@data-lat").get()
        if latitude:     
            item_loader.add_value("latitude", latitude)
        longitude = response.xpath("//div[contains(@class,'marker')]//@data-lng").get()
        if longitude:     
            item_loader.add_value("longitude", longitude)
        item_loader.add_value('property_type', property_type)
        item_loader.add_value('external_link', external_link)
        item_loader.add_xpath('title', '//h1[contains(@class, "title")]/text()')
        item_loader.add_value('external_id', str(external_id))
        item_loader.add_value('address', address)
        item_loader.add_value('city', city)
        item_loader.add_value('zipcode', zipcode)
        item_loader.add_xpath('description', '//div[@class="description-contents"]/p//text()')
        item_loader.add_value('rent_string', rent_string)
        item_loader.add_xpath('images', '//ul[contains(@class, "main-slider")]//img/@src')

        deposit = response.xpath("//ul/li[@class='deposit']/text()").get()
        if deposit:
            item_loader.add_value("deposit", deposit.split(":")[1].strip())
        
        furnished = response.xpath("//li[contains(@class,'furnished')]//text()[not(contains(.,'Unfurnished'))]").get()
        if furnished:
            item_loader.add_value("furnished", True)
        
        parking = response.xpath("//div[contains(@class,'features')]//li[contains(.,'Park')]//text()").get()
        if parking:
            item_loader.add_value("parking", True)

        item_loader.add_value('room_count', str(room_count))
        item_loader.add_value('bathroom_count', str(bathrooms))
        item_loader.add_value('landlord_name', "O'connor bowden")
        item_loader.add_value('landlord_email', 'info@oconnorbowden.co.uk')
        item_loader.add_value('landlord_phone', '01618080010')
        item_loader.add_value("external_source", "{}_PySpider_{}_{}".format(self.name.capitalize(), self.country, self.locale))
        yield item_loader.load_item() 