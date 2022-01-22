# -*- coding: utf-8 -*-
# Author: Mehmet Kurtipek

import scrapy
import re
from ..loaders import ListingLoader

class ChaseapartmentsSpider(scrapy.Spider):
    name = "chaseapartments"
    allowed_domains = ["chaseapartments"]
    start_urls = (
        'http://www.chaseapartments.com/',
    )
    execution_type = 'testing'
    country = 'united_kingdom'
    locale ='en'
    thousand_separator=','
    scale_separator='.'

    def start_requests(self):
        for page in range(0, 8):
            link = 'https://www.chaseapartments.com/search/{}.html?instruction_type=Letting&address_keyword=&address_keyword_fields=address_1%2Caddress_2%2Caddress_3%2Caddress_4%2Carea%2Ctown%2Ccounty%2Clocation%2Ccountry%2Cdisplay_address%2Cpostcode&address_keyword_postcode_starts_with=1&minpricew=&maxpricew='.format(page)
            yield scrapy.Request(url=link, callback=self.parse, dont_filter=True)    
    def parse(self, response, **kwargs):
        links = response.xpath('//div[contains(@class, "property-grid")]//a[@class="highlighted-property"]')
        for link in links: 
            url = response.urljoin(link.xpath('./@href').extract_first())
            yield scrapy.Request(url=url, callback=self.get_property_details, dont_filter=True)
    def get_property_details(self, response):
        external_link = response.url
        address = response.xpath('//span[@itemprop="name"]/text()').extract_first('').strip()
        item_loader = ListingLoader(response=response)
        city = response.xpath("//h2/span/text()").get()
        if city: 
            city = city.replace(", ,","")
            if  city.split(",")[-1].strip():
                item_loader.add_value('city', city.split(",")[-1].strip())
            else:
                item_loader.add_value('city', city.split(",")[-2].strip())

        item_loader.add_xpath('title', "//h2/span/text()")

        room_count_text = response.xpath('//img[@alt="Bedrooms"]/../text()').extract_first('').strip()
        if room_count_text:
            room_count = re.findall(r'\d+', room_count_text)[0]     
        bathrooms_text = response.xpath('//img[@alt="Bathrooms"]/../text()').extract_first('').strip() 
        if bathrooms_text:
            bathrooms = re.findall(r'\d+', bathrooms_text)[0]
        try:
            rent_string = ''.join(response.xpath('//span[@itemprop="price"]/text()').extract()).split(' p/wk ')[-1]
        except:
            rent_string = ''.join(response.xpath('//span[@itemprop="price"]/text()').extract())
        parking = response.xpath("//li[contains(.,'Parking') or contains(.,'parking')]/text()").extract_first() 
        if parking:
            item_loader.add_value('parking', True)
        square_meters = response.xpath("//span[@itemprop='description']//text()[contains(.,'sq ft')]").extract_first() 
        if square_meters:
            unit_pattern = re.findall(r"[+-]? *((?:\d+(?:\.\d*)?|\.\d+)(?:[eE][+-]?\d+)?)\s*(sq ft|sq. ft.|Sq. ft.|sq. Ft.|sq|Sq)",square_meters.replace(",",""))
            if unit_pattern:
                square_title=unit_pattern[0][0]
                sqm = str(int(float(square_title) * 0.09290304))
                item_loader.add_value("square_meters", sqm)

        item_loader.add_value('property_type', 'apartment')
        item_loader.add_value('external_link', external_link)
        item_loader.add_value('address', address)
        item_loader.add_xpath('description', '//span[@itemprop="description"]//text()')
        item_loader.add_value('rent_string', rent_string)
        item_loader.add_xpath('images', '//img[contains(@class, "property-main-image")]/@src')
        item_loader.add_value('room_count', str(room_count))
        item_loader.add_value('bathroom_count', str(bathrooms))
        
        furnished = response.xpath("//li[contains(.,'Furnished')]//text()[not(contains(.,'Unfurnished'))]").get()
        if furnished:
            item_loader.add_value("furnished", True)
        item_loader.add_value('landlord_name', 'Chase apartments')
        item_loader.add_value('landlord_email', 'info@chaseapartments.com')
        item_loader.add_value('landlord_phone', '0207 722 5022')
        item_loader.add_value("external_source", "{}_PySpider_{}_{}".format(self.name.capitalize(), self.country, self.locale))
        yield item_loader.load_item()

    