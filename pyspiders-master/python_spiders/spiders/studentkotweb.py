# -*- coding: utf-8 -*-
# Author: Mehmet Kurtipek
import scrapy
import re
from ..loaders import ListingLoader

def extract_city_zipcode(_address):
    city_zipcode = _address.split(', ')[-1]
    city = city_zipcode.split(' ')[-1]
    zipcode = city_zipcode.split(' ')[0]
    return zipcode, city

class StudentkotwebSpider(scrapy.Spider):
    name = "studentkotweb"
    external_source = "Studentkotweb_PySpider_belgium_nl"
    allowed_domains = ["www.studentkotweb.be"]
    start_urls = (
        'http://www.studentkotweb.be/',
    )
    execution_type = 'testing'
    country = 'belgium'
    locale ='nl'
    thousand_separator='.'
    scale_separator=','

    def start_requests(self):
        start_urls = [
            {'url': 'https://www.studentkotweb.be/nl/search?search=belgium&latlon=&f%5B0%5D=field_room_type%3Aapartment&page=1', 'property_type': 'apartment'},
        ]
        for url in start_urls:
            yield scrapy.Request(
                url=url.get('url'),
                callback=self.parse, 
                meta={'property_type': url.get('property_type')},
                dont_filter=True
            )

    def parse(self, response, **kwargs):
        for page in range(0, 27):
            link = response.url.replace('page=1', 'page={}'.format(page))
            yield scrapy.Request(url=link, callback=self.get_property_urls, dont_filter=True, meta={'property_type': response.meta.get('property_type')})
    def get_property_urls(self, response):
        links = response.xpath('//div[@class="m-results-content-rooms-inner"]//div[@class="m-teaser-content"]')
        for link in links: 
            url = response.urljoin(link.xpath('./h4/a/@href').extract_first())
            yield scrapy.Request(url=url, callback=self.get_property_details, dont_filter=True, meta={'property_type': response.meta.get('property_type')})
    
    def get_property_details(self, response):
        property_type = response.meta.get('property_type')
        external_link = response.url
        address = ''.join(response.xpath('//div[@class="m-icon-list-wrapper"]/ul/li[1]/span[2]/text()').extract()).strip()   
        zipcode, city = extract_city_zipcode(address)

        url = response.url
        if "bedroom" in url:
            room_count = url.split("-bedroom")[0].split("/")[-1]
        else:
            room_count = 1

        square_meters_text = response.xpath('//span[@class="icon-house"]/following-sibling::text()').extract_first()
        
        if '-' in square_meters_text:
            square_meters = square_meters_text.split('-')[-1]
        else:
            square_meters = ''
        description = ''.join(response.xpath('//div[@class="m-icon-list-wrapper"]/following-sibling::p//text()').extract()) + ''.join(response.xpath('//div[@class="o-detail-block"]/ul/li//text()').extract())
        rent_string = ''.join(response.xpath('//span[@class="o-card-price"]/text()').extract())
        if room_count and square_meters: 
            item_loader = ListingLoader(response=response)
            item_loader.add_value('property_type', property_type)
            item_loader.add_value('external_link', external_link)
            item_loader.add_value('address', address)
            item_loader.add_value('city', city)
            item_loader.add_value('zipcode', zipcode)
            item_loader.add_value('description', description)
            item_loader.add_value('rent_string', rent_string)
            item_loader.add_xpath('images', '//li[@class="m-photobook-item"]//img/@src')
            if room_count and int(room_count) > 0: 
                item_loader.add_value('room_count', str(room_count))
            item_loader.add_value('square_meters', square_meters)
            item_loader.add_value('landlord_name', 'Studentkotweb')
            item_loader.add_value('landlord_email', 'info@studentkotweb.be')
            item_loader.add_value('landlord_phone', '+32 3 265 23 99')
            item_loader.add_value("external_source", self.external_source)
            yield item_loader.load_item()