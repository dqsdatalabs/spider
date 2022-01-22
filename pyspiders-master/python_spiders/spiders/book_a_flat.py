# -*- coding: utf-8 -*-
# Author: Mehmet Kurtipek
import scrapy
import re
from ..loaders import ListingLoader
from python_spiders.helper import string_found, format_date, remove_white_spaces
import dateparser

class BookAFlatSpider(scrapy.Spider):
    name = "book_a_flat"
    # allowed_domains = ["https://book-a-flat.com/en"]
    execution_type = 'testing'
    country = 'united_kingdom'
    locale ='en'
    thousand_separator=','
    scale_separator='.'

    def start_requests(self):
        start_urls = {'url': 'https://book-a-flat.com/en/search.php?search=1&rooms=2,3,4,5', 'property_type': 'apartment'}
        yield scrapy.Request(
                url=start_urls.get('url'),
                callback=self.parse, 
                meta={'property_type': start_urls.get('property_type')},
                dont_filter=True
        )

    def parse(self, response, **kwargs):

        for link in response.xpath('//div[@class="listing"]/a'):
            url = response.urljoin(link.xpath('./@href').extract_first())
            yield scrapy.Request(url=url, callback=self.get_property_details, meta={'property_type': response.meta.get('property_type')}, dont_filter=True )
        if response.xpath('//li[@class="next"]/a/@href'):
            
            next_link = response.urljoin(response.xpath('//li[@class="next"]/a/@href').extract_first())
            yield scrapy.Request(url=next_link, callback=self.parse, meta={'property_type': response.meta.get('property_type')}, dont_filter=True)
    
    def get_property_details(self, response):
        item_loader = ListingLoader(response=response)

        external_id = response.xpath('//div[@class="ref"]/text()').extract_first()
        if external_id:
            item_loader.add_value("external_id", external_id.split(":")[-1].strip())
        property_type = response.meta.get('property_type')
        home_set = response.xpath('//li[contains(text(), "Bedroom")]/text()').extract_first('')
        if home_set and '-' in home_set: 
            room_count_text = home_set.split(' - ')
            room_count = str(re.findall(r'\d+', room_count_text[0])[0])
            square_meters = str(room_count_text[-1].split(' ')[0])
        else:
            room_count = ''
            square_meters = ''
        floor_text = response.xpath('//li[contains(text(), "floor")]/text()').extract_first('').strip()
        try:
            floor = str(re.findall(r'\d+', floor_text)[0])
        except:
            floor = ''
        latitude = str(response.xpath('//meta[contains(@property, "latitude")]/@content').extract_first())
        longitude = str(response.xpath('//meta[contains(@property, "longitude")]/@content').extract_first())
        street = response.xpath('//meta[contains(@property, "street_address")]/@content').extract_first()
        city = response.xpath('//meta[contains(@property, "street_address")]/@content').extract_first()
        zipcode = str(response.xpath('//meta[contains(@property, "postal_code")]/@content').extract_first()) 
        address = street + ' ' + zipcode + ' ' + city  
        available_date_text = response.xpath('//div[@class="date"]/text()[contains(.,"Available:")]').extract_first()
        if available_date_text:
            date_parsed = dateparser.parse(available_date_text.split(":")[-1].strip(), date_formats=["%d/%B/%Y"])
            if date_parsed:
                item_loader.add_value("available_date", date_parsed.strftime("%Y-%m-%d"))
        
        item_loader.add_value("external_link", response.url)
        item_loader.add_value("property_type", property_type)
        item_loader.add_value("room_count", room_count)
        if floor:
            item_loader.add_value("floor", floor)
        item_loader.add_value("address", address)
        item_loader.add_value("latitude", latitude)
        item_loader.add_value("longitude", longitude)
        item_loader.add_value("city", city)
        item_loader.add_value("zipcode", zipcode)
        item_loader.add_xpath("images", '//picture//img[@class="lazyImg"]/@src')
        item_loader.add_value("square_meters", square_meters)
        item_loader.add_xpath('description', '//h3[contains(text(), "Description")]/following-sibling::p//text()')
        item_loader.add_xpath("title", '//header/h1/text()')
        item_loader.add_xpath("rent_string", '//header/div[@class="price"]/span/text()')
        item_loader.add_value('landlord_name', 'BOOK-A-FLAT')
        item_loader.add_value('landlord_email', 'info@book-a-flat.com')
        item_loader.add_value('landlord_phone', '+33 1 47 03 24 48')
        item_loader.add_value("external_source", "{}_PySpider_{}_{}".format(self.name.capitalize(), self.country, self.locale))
        yield item_loader.load_item()