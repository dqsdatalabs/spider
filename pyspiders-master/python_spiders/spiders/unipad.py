# -*- coding: utf-8 -*-
# Author: Mehmet Kurtipek

import scrapy
import re
from ..loaders import ListingLoader

class UnipadSpider(scrapy.Spider):
    name = "unipad"
    allowed_domains = ["unipad-uk.com"]
    start_urls = (
        'http://www.unipad-uk.com/',
    )
    execution_type = 'testing'
    country = 'united_kingdom'
    locale ='en'
    thousand_separator=','
    scale_separator='.'

    def start_requests(self):
        start_urls = "http://www.unipad-uk.com/wp/list-layout-full-width/"
        yield scrapy.Request( url=start_urls, callback=self.parse, dont_filter=True )

    def parse(self, response, **kwargs):
        links = response.xpath('//div[contains(@class,"property-description")]//a[contains(text(), "Show Details")]')
        for link in links: 
            url = response.urljoin(link.xpath('./@href').extract_first())
            yield scrapy.Request(url=url, callback=self.get_property_details, dont_filter=True )
        if response.xpath('//a[@class="next page-numbers"]/@href'):
            next_link = response.xpath('//a[@class="next page-numbers"]/@href').extract_first()
            yield scrapy.Request(url=next_link, callback=self.parse, dont_filter=True)
    
    def get_property_details(self, response):
        item_loader = ListingLoader(response=response)
        # parse details of the propery
        property_type = 'studio'
        external_link = response.url

        title = response.xpath('//h1[contains(@class, "single-property-title")]/text()').extract_first()
        zipcode = title.split(', ')[-1].replace("10 Cross Road","")
        city = title.split(', ')[-2] 
        address = city + ' ' + zipcode
        if city:
            city_list=["London","Oxford","West Walk"]
            for x in city_list:
                if x.lower() in city.lower():
                    item_loader.add_value('city', x)

        external_id = response.xpath('//span[contains(text(), "Property ID")]/following-sibling::span/text()').extract_first()
        room_count = response.xpath('//span[contains(text(), "Bedrooms")]/following-sibling::span/text()').extract_first('').strip()
        bathrooms = response.xpath('//span[contains(text(), "Bathrooms")]/following-sibling::span/text()').extract_first('').strip() 
        square_meters = response.xpath('//span[contains(text(), "Area")]/following-sibling::span/text()').extract_first('').strip()
        rent_string = response.xpath('//span[contains(@class, "single-property-price")]/text()').extract_first('').strip()
        if rent_string and ("pw" in rent_string.lower() or "week" in rent_string.lower()):
            rent_value = re.findall(r'([\d|,|\.]+)', rent_string)[0].replace(',', '')
            rent_month = str(int(rent_value) * 4) + '£'
            item_loader.add_value('rent_string', rent_month)
        else:
            item_loader.add_value('rent_string', rent_string)
        
        item_loader.add_value('property_type', property_type)
        item_loader.add_value('external_id', external_id)
        item_loader.add_value('external_link', external_link)
        item_loader.add_value('title', title)
        item_loader.add_value('address', address)
       
        item_loader.add_value('zipcode', zipcode)
        item_loader.add_xpath('description', '//div[@class="property-content"]/p/text()')
        item_loader.add_xpath('city', "//div[@class='meta-inner-wrapper']//span[contains(.,'Location')]/following-sibling::span/text()")
        
        if square_meters: 
            item_loader.add_value('square_meters', square_meters)
        item_loader.add_xpath('images', '//ul[@class="slides"]/li//img/@src')
        if room_count:
            item_loader.add_value('room_count', str(room_count))
        if bathrooms: 
            item_loader.add_value('bathroom_count', str(bathrooms))
        furnished = response.xpath("//dd[contains(.,'FURNISHED') or contains(.,'Furnished')]/text()").extract_first()
        if furnished:
            if "unfurnished" in furnished.lower():
                item_loader.add_value("furnished", False)
            elif "furnished" in furnished.lower():
                item_loader.add_value("furnished", True)
        item_loader.add_value('landlord_name', 'Parmars Estate Finance Insurance')
        item_loader.add_value('landlord_email', 'Enquiries@Unipad.Co.Uk')
        item_loader.add_value('landlord_phone', '0116 285 8000')
        item_loader.add_value("external_source", "{}_PySpider_{}_{}".format(self.name.capitalize(), self.country, self.locale))
        yield item_loader.load_item() 

