# -*- coding: utf-8 -*- 
# Author: Mehmet Kurtipek 
import scrapy
import re
from ..loaders import ListingLoader
from math import floor
from math import *

def cleantext(text):
    sq_value = re.findall(r'([\d|,|\.]+)', text)[0]
    key_text = sq_value.replace('.00', '')
    return key_text 
class DimensionestatesSpider(scrapy.Spider): 
    name = "dimensionestates"
    allowed_domains = ["www.dimension-estates.co.uk"]
    start_urls = ( 
        'http://www.www.dimension-estates.co.uk/',
    )
    execution_type = 'testing'
    country = 'united_kingdom'
    locale ='en'
    thousand_separator=','
    scale_separator='.'

    def start_requests(self):
        start_urls = [
            {'url': 'https://www.dimension-estates.co.uk/?id=41597&do=search&for=2&minbeds=0&minprice=0&type%5B%5D=8&maxbeds=9999&maxprice=99999999999&Search=&id=41597&order=2&page=0&do=search', 'property_type': 'apartment'},
            {'url': 'https://www.dimension-estates.co.uk/?id=41597&do=search&for=2&minbeds=0&minprice=0&type%5B%5D=6&maxbeds=9999&maxprice=99999999999&Search=&id=41597&order=2&page=0&do=search', 'property_type': 'house'},
            {'url': 'https://www.dimension-estates.co.uk/?id=41597&do=search&for=2&minbeds=0&minprice=0&type%5B%5D=13&maxbeds=9999&maxprice=99999999999&Search=&id=41597&order=2&page=0&do=search', 'property_type': 'apartment'},
            {'url': 'https://www.dimension-estates.co.uk/?id=41597&do=search&for=2&minbeds=0&minprice=0&type%5B%5D=11&maxbeds=9999&maxprice=99999999999&Search=&id=41597&order=2&page=0&do=search', 'property_type': 'house'}
        ]
        for url in start_urls:
            yield scrapy.Request(
                url=url.get('url'),
                callback=self.parse, meta={'property_type': url.get('property_type')},
                dont_filter=True
            )

    def parse(self, response, **kwargs):
        if response.xpath('//div[contains(@class, "hideTo")]/following-sibling::div[contains(text(), "Showing")]/a'):
            for page in response.xpath('//div[contains(@class, "hideTo")]/following-sibling::div[contains(text(), "Showing")]/a'):
                next_page = response.urljoin(page.xpath('./@href').extract_first())
                yield scrapy.Request(url=next_page, callback=self.get_detail_urls, dont_filter=True, meta={'property_type': response.meta.get('property_type')})
        else:
            yield scrapy.Request(url=response.url, callback=self.get_detail_urls, dont_filter=True, meta={'property_type': response.meta.get('property_type')})
    
    def get_detail_urls(self, response):  
        links = response.xpath('//div[contains(@class, "hideTo")]/div/a')
        for link in links: 
            url = response.urljoin(link.xpath('./@href').extract_first())
            yield scrapy.Request(url=url, callback=self.get_property_details, dont_filter=True, meta={'property_type': response.meta.get('property_type')})
    def get_property_details(self, response):
        item_loader = ListingLoader(response=response)
        property_type = response.meta.get('property_type') 
        external_link = response.url
        leta=response.xpath("//h2[@class='title-small']/span/text()[contains(.,'Let')]").get()
        if leta:
            return 


        address = ''.join(response.xpath('//div[@id="property-details"]//h1//text()').extract())
        zipcode = address.split(', ')[-1].strip()
        city = address.split(', ')[-2]
        square_meters = cleantext(response.xpath('//i[contains(@class, "icon-rooms")]/following-sibling::text()').extract_first('').strip())
        try:
            room_count = int(response.xpath('//i[contains(@class, "icon-area")]/following-sibling::text()').extract_first('').strip())
        except:
            room_count = ''
        bathrooms_text = response.xpath('//i[contains(@class, "icon-bathrooms")]/following-sibling::text()').extract_first('').strip()
        bathrooms = re.findall(r'\d+', bathrooms_text)[0]
        rent_string = ''.join(response.xpath('//h2[@class="title-small"]/text()').extract())
        rent_string = re.sub(r'[\s]+', '', rent_string)
        floor_plan_images = response.urljoin(response.xpath('//iframe[contains(@src, "floorplan")]/@src').extract_first())
     
        
        if property_type:
            item_loader.add_value('property_type', property_type)
        item_loader.add_value('title', address)
        item_loader.add_value('external_link', external_link)
        item_loader.add_value('external_id', external_link.split("pid=")[-1])
        item_loader.add_value('address', address)
        
        item_loader.add_value('city', city) 
        item_loader.add_value('zipcode', zipcode)
        if square_meters:
            item_loader.add_value('square_meters', str(floor(float(square_meters))))
        item_loader.add_xpath('description', '//div[@id="property-details"]//p//text()')
   

        item_loader.add_value('rent_string', rent_string)
        item_loader.add_xpath('images', '//div[@id="galleria"]//img/@src')
        item_loader.add_value('room_count', str(room_count))
        item_loader.add_value('floor_plan_images', floor_plan_images)
        balcony = response.xpath("//ul[contains(@class, 'features-list')]/text()[contains(.,'Balcony')]").extract_first()
        if balcony:
            item_loader.add_value('balcony', True)
        parking = response.xpath("//ul[contains(@class, 'features-list')]/text()[contains(.,'Parking') or contains(.,'parking')]").extract_first()
        if parking:
            item_loader.add_value('parking', True) 
        furnished = response.xpath("//ul[contains(@class, 'features-list')]/text()[contains(.,'Furnished') or contains(.,'furnished')]").extract_first()
        if furnished:
            if "unfurnished" in furnished.lower().replace("-",""):
                item_loader.add_value('furnished', False)
            elif "furnished" in furnished.lower().replace("-",""):
                item_loader.add_value('furnished', True)
        item_loader.add_value('bathroom_count', str(bathrooms))
        item_loader.add_value('landlord_name', 'Hackney Branch')
        item_loader.add_value('landlord_email', 'contact@dimensionestates.co.uk')
        item_loader.add_value('landlord_phone', '0208 510 9290')
        item_loader.add_value("external_source", "{}_PySpider_{}_{}".format(self.name.capitalize(), self.country, self.locale))

        yield item_loader.load_item()


