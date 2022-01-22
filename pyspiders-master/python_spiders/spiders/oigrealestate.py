# -*- coding: utf-8 -*-
# Author: Mehmet Kurtipek

import scrapy
import re
from ..loaders import ListingLoader

class OigrealestateSpider(scrapy.Spider):
    name = "oigrealestate"
    allowed_domains = ["oigrealestate.com"]
    start_urls = (
        'http://www.oigrealestate.com/',
    )
    execution_type = 'testing'
    country = 'united_kingdom'
    locale ='en'
    thousand_separator=','
    scale_separator='.'

    def start_requests(self):
        start_urls = [
            {'url': 'https://oigrealestate.com/lettings/?minimum_rent=&maximum_rent=&minimum_bedrooms=&property_type=27&location=&department=residential-lettings', 'property_type': 'apartment'},
            {'url': 'https://oigrealestate.com/lettings/?minimum_rent=&maximum_rent=&minimum_bedrooms=&property_type=14&location=&department=residential-lettings', 'property_type': 'house'}
        ]
        for url in start_urls:
            yield scrapy.Request(
                url=url.get('url'),
                callback=self.parse, 
                meta={'property_type': url.get('property_type')},
                dont_filter=True
            )

    def parse(self, response, **kwargs):
        links = response.xpath('//ul[contains(@class, "properties")]//div[@class="thumbnail"]/a')
        for link in links: 
            url = response.urljoin(link.xpath('./@href').extract_first())
            yield scrapy.Request(url=url, callback=self.get_property_details, dont_filter=True, meta={'property_type': response.meta.get('property_type')})
        if response.xpath('//ul[@class="page-numbers"]//a[contains(@class, "next")]/@href'):
            next_link = response.xpath('//ul[@class="page-numbers"]//a[contains(@class, "next")]/@href').extract_first()
            yield scrapy.Request(url=next_link, callback=self.parse, dont_filter=True, meta={'property_type': response.meta.get('property_type')})
    
    def get_property_details(self, response):
        # parse details of the propery
        item_loader = ListingLoader(response=response)
        property_type = response.meta.get('property_type')
        external_link = response.url
        external_id = response.xpath('//li[@class="reference-number"]/text()').extract_first().strip()
        title=response.xpath("//title//text()").get()
        if title:
            item_loader.add_value("title",title)
         

        address = response.xpath('//h1[contains(@class, "entry-title")]/text()').extract_first()
        if address and "page" in address:
            return 
        city=response.xpath('//h1[contains(@class, "property_title")]/text()').extract_first()
        if city:
            item_loader.add_value("city",city.split(",")[-1])
        room_count_text = response.xpath('//li[@class="bedrooms"]/text()').extract_first('').strip()
        if room_count_text:
            try:
                room_count = re.findall(r'\d+', room_count_text)[0]    
            except:
                room_count = ''
        else:
            room_count = ''
        bathrooms_text = response.xpath('//li[@class="bathrooms"]/text()').extract_first('').strip() 
        if bathrooms_text:
            try:
                bathrooms = re.findall(r'\d+', bathrooms_text)[0]      
            except:
                bathrooms = ''
        else:
            bathrooms = ''

        
        lat_lon = re.search(r'new google\.maps\.LatLng\((.*?)\)', response.text)
        if lat_lon:
            lat_lon = lat_lon.group(1)
            lat = lat_lon.split(',')[0]
            lon = lat_lon.split(',')[1]  
            item_loader.add_value('latitude', str(lat))
            item_loader.add_value('longitude', str(lon))

        rent_string = response.xpath('//div[@class="price"]/text()').extract_first('').strip()
        rent_value = re.findall(r'([\d|,|\.]+)', rent_string)[0].replace(',', '')
        rent_month = str(int(rent_value) * 4) + '£'
        desc = "".join(response.xpath("//div[@class='description-contents']//text()").extract())
        if desc:
            item_loader.add_value('description', desc)  
        if "studio" in desc:
            item_loader.add_value('property_type', "studio")
            item_loader.add_value('room_count', "1")
        else:
            item_loader.add_value('property_type', property_type)
        item_loader.add_value('external_link', external_link)
        item_loader.add_value('external_id', external_id)
        item_loader.add_value('address', address)

        item_loader.add_value('rent_string', rent_month)
        item_loader.add_xpath('images', '//div[@id="slider"]//img/@src')
        if room_count:
            item_loader.add_value('room_count', str(room_count))     
        if bathrooms: 
            item_loader.add_value('bathroom_count', str(bathrooms))
        item_loader.add_value('landlord_name', 'OIG Real Estate')
        item_loader.add_value('landlord_email', 'office@oigrealestate.com')
        item_loader.add_value('landlord_phone', '020 3500 1650')
        item_loader.add_value("external_source", "{}_PySpider_{}_{}".format(self.name.capitalize(), self.country, self.locale))
        yield item_loader.load_item() 