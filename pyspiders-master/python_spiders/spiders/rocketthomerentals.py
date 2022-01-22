# -*- coding: utf-8 -*-
# Author: Mehmet Kurtipek
import scrapy
import re
from ..loaders import ListingLoader

class RocketthomerentalsSpider(scrapy.Spider):
    name = "rocketthomerentals"
    allowed_domains = ["rocketthomerentals.com"]
    start_urls = (
        'http://www.rocketthomerentals.com/',
    )
    execution_type = 'testing'
    country = 'united_kingdom'
    locale ='en'
    thousand_separator=','
    scale_separator='.'

    def start_requests(self):
        start_urls = [
            {'url': 'https://www.rocketthomerentals.com/search/?showstc=on&showsold=on&address_keyword=&minprice=&maxprice=&property_type=Apartment', 'property_type': 'apartment'},
            {'url': 'https://www.rocketthomerentals.com/search/?showstc=on&showsold=on&address_keyword=&minprice=&maxprice=&property_type=Town+House', 'property_type': 'house'},
            {'url': 'https://www.rocketthomerentals.com/search/?showstc=on&showsold=on&address_keyword=&minprice=&maxprice=&property_type=Shared+House', 'property_type': 'house'},
            {'url': 'https://www.rocketthomerentals.com/search/?showstc=on&showsold=on&address_keyword=&minprice=&maxprice=&property_type=Detached+House', 'property_type': 'house'}
        ]
        for url in start_urls:
            yield scrapy.Request(
                url=url.get('url'),
                callback=self.parse, 
                meta={'property_type': url.get('property_type')},
                dont_filter=True
            )

    def parse(self, response, **kwargs):
        links = response.xpath('//div[@class="thumbnails-container"]')
        for link in links: 
            url = response.urljoin(link.xpath('.//a[contains(@href, "property-details")]/@href').extract_first())
         
            room_count = response.xpath('//div[@class="thumbnails-container"]//span[@class="property-bedrooms"]/text()').extract_first()
            bathrooms = response.xpath('//div[@class="thumbnails-container"]//span[@class="property-bathrooms"]/text()').extract_first()    
            yield scrapy.Request(url=url, callback=self.get_property_details, dont_filter=True, meta={'property_type': response.meta.get('property_type'), 'room_count': room_count, 'bathrooms': bathrooms})

    def get_property_details(self, response):
        # parse details of the propery
        property_type = response.meta.get('property_type')
        external_link = response.url
        external_id = str(response.xpath('//input[@name="propertyReference"]/@value').extract_first().strip())
        room_count = response.meta.get('room_count')
        bathrooms = response.meta.get('bathrooms') 
        address = response.xpath('//h2/text()').extract_first().strip().split(' in ')[-1]
        zipcode = address.split(', ')[-1].strip()
        city = address.split(', ')[-2] 
        lat_lon = re.search(r'\&q=(.*?)\"', response.text).group(1)
        lat = lat_lon.split('%2C-')[0]
        lon = lat_lon.split('%2C-')[1]  
        rent_string = response.xpath('//small[contains(text(), "Per Month")]/../text()').extract_first('').strip()
        item_loader = ListingLoader(response=response)
        item_loader.add_value('property_type', property_type)
        item_loader.add_value('external_link', external_link)
        item_loader.add_value('external_id', external_id)
        item_loader.add_value('address', address)
        item_loader.add_value('city', address)
        item_loader.add_value('zipcode', zipcode)
        item_loader.add_xpath('description', '//div[@class="result-desc"]/text()')
        item_loader.add_value('rent_string', rent_string)
        item_loader.add_xpath('images', '//div[@class="item"]/img/@src')
        if room_count:
            item_loader.add_value('room_count', str(room_count))
        item_loader.add_value('latitude', str(lat))
        item_loader.add_value('longitude', str(lon))
        if bathrooms: 
            item_loader.add_value('bathroom_count', str(bathrooms))
        item_loader.add_value('landlord_name', 'Rockett Home Rentals')
        item_loader.add_value('landlord_email', 'info@rocketthomerentals.com')
        item_loader.add_value('landlord_phone', '01782 638 111')
        item_loader.add_value("external_source", "{}_PySpider_{}_{}".format(self.name.capitalize(), self.country, self.locale))
        yield item_loader.load_item() 