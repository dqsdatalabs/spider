# -*- coding: utf-8 -*-
# Author: Mehmet Kurtipek

import scrapy
import re
from ..loaders import ListingLoader

class JwandsonsSpider(scrapy.Spider):
    name = "jwandsons"
    allowed_domains = ["jwandsons.co.uk"]
    start_urls = (
        'http://www.jwandsons.co.uk/',
    )
    execution_type = 'testing'
    country = 'united_kingdom'
    locale ='en'
    thousand_separator=','
    scale_separator='.'
    custom_settings = {
        "PROXY_ON":True
    }
    def start_requests(self):
        start_urls = [
            {'url': 'http://www.jwandsons.co.uk/properties/?department=residential-lettings&address_keyword=&property_type=22', 'property_type': 'apartment'},
            {'url': 'http://www.jwandsons.co.uk/properties/?department=residential-lettings&address_keyword=&property_type=9', 'property_type': 'house'}
        ]
        for url in start_urls:
            yield scrapy.Request(
                url=url.get('url'),
                callback=self.parse, 
                meta={'property_type': url.get('property_type')},
                dont_filter=True
            )

    def parse(self, response, **kwargs):
        links = response.xpath('//a[contains(@class, "qt-property-thumb")]')
        for link in links: 
            url = response.urljoin(link.xpath('./@href').extract_first())
            yield scrapy.Request(url=url, callback=self.get_property_details, dont_filter=True, meta={'property_type': response.meta.get('property_type')})
        if response.xpath('//a[contains(@class, "next")]/@href'):
            next_link = response.xpath('//a[contains(@class, "next")]/@href').extract_first()
            yield scrapy.Request(url=next_link, callback=self.parse, dont_filter=True, meta={'property_type': response.meta.get('property_type')})

    def get_property_details(self, response):
        # parse details of the propery
        property_type = response.meta.get('property_type')
        external_link = response.url
        external_id = str(response.xpath('//div[contains(text(), "Reference")]/following-sibling::div/text()').extract_first().strip())
        address = response.xpath('//h1[@class="qt-page-title"]/text()').extract_first()
        room_count_text = response.xpath('//span[@class="qt-property-bedrooms"]/text()').extract_first('').strip()
        if room_count_text:
            try:
                room_count = re.findall(r'\d+', room_count_text)[0]    
            except:
                room_count = '1'
        else:
            room_count = '1'
        bathrooms_text = response.xpath('//span[@class="qt-property-bathrooms"]/text()').extract_first('').strip() 
        if bathrooms_text:
            try:
                bathrooms = re.findall(r'\d+', bathrooms_text)[0]      
            except:
                bathrooms = ''
        else:
            bathrooms = ''
        
        try:
            lat = re.search(r'var map_lat\s=\s(.*?)\;', response.text).group(1)
            lon = re.search(r'var map_lng\s=\s(.*?)\;', response.text).group(1)
        except:
            lat = ""
            lon = ""
        rent_string = response.xpath('//div[@class="qt-property-single-header"]/h2/text()').extract_first('').strip()
        item_loader = ListingLoader(response=response)
        item_loader.add_value('property_type', property_type)
        item_loader.add_value('external_link', external_link)
        item_loader.add_value('external_id', external_id)
        
        if address:
            item_loader.add_value('address', address)
            item_loader.add_value('title', address)
            
            item_loader.add_value("city", address.split(",")[-1].strip())
            
        item_loader.add_xpath('description', '//h3[contains(text(), "Summary")]/following-sibling::p//text() | //h3[contains(text(), "Detail")]/following-sibling::p//text()')
        item_loader.add_value('rent_string', rent_string)
        item_loader.add_xpath('images', '//div[contains(@class,"property-gallery")]/img/@src')
        if room_count:
            item_loader.add_value('room_count', str(room_count))
        item_loader.add_value('latitude', str(lat))
        item_loader.add_value('longitude', str(lon))
        if bathrooms: 
            item_loader.add_value('bathroom_count', str(bathrooms))
        item_loader.add_value('landlord_name', 'J W & SONS')
        item_loader.add_value('landlord_email', 'enquiries@jwandsons.co.uk')
        item_loader.add_value('landlord_phone', '0208 993 0056')
        item_loader.add_value("external_source", "{}_PySpider_{}_{}".format(self.name.capitalize(), self.country, self.locale))
        
        status = response.xpath("//div[contains(@class,'property-info')][contains(.,'Availability')]/following-sibling::div/text()").get()
        if status and "agreed" in status.lower(): return
        
        yield item_loader.load_item()