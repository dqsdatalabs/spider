# -*- coding: utf-8 -*-
# Author: Valerii Nikitiuk
import scrapy
import re
from ..loaders import ListingLoader

class CityhutsSpider(scrapy.Spider):
    name = "cityhuts"
    allowed_domains = ["www.cityhuts.co.uk"]
    start_urls = (
        'http://www.cityhuts.co.uk/',
    )
    execution_type = 'testing'
    country = 'united_kingdom'
    locale ='en'
    thousand_separator=','
    scale_separator='.'

    def start_requests(self):
        
        start_urls = [
            {'url': 'https://www.cityhuts.co.uk/properties/?page={}&pageSize=10&orderBy=PriceSearchAmount&orderDirection=DESC&propInd=L&isOverseas=False', 'property_type': 'apartment'},
        ]
        for url in start_urls:
            yield scrapy.Request(
                url=url.get('url'),
                callback=self.parse, 
                meta={'property_type': url.get('property_type')},
                dont_filter=True
            )

    def parse(self, response, **kwargs):
        links = response.xpath('//div[@class="searchprop"]')
        for link in links: 
            let_status = link.xpath('.//div[@class="status"]/img/@alt').extract_first('')
            if not let_status: 
                url = response.urljoin(link.xpath('.//div[@class="address"]/a/@href').extract_first())
                yield scrapy.Request(url=url, callback=self.get_property_details, dont_filter=True, meta={'property_type': response.meta.get('property_type')})
        if response.xpath('//a[contains(text(), "next")]/@href'):
            next_link = response.urljoin(response.xpath('//a[contains(text(), "next")]/@href').extract_first())
            yield scrapy.Request(url=next_link, callback=self.parse, dont_filter=True, meta={'property_type': response.meta.get('property_type')})
    
    def get_property_details(self, response):
        external_link = response.url
        property_type = response.meta.get('property_type')
        address = response.xpath('//div[@class="headline"]/text()').extract_first('').strip()
        city_zipcode = address.split(', ')[-1]
        zipcode = city_zipcode.split(' ')[-1]
        city = city_zipcode.replace(zipcode, '')   
        room_count_text = response.xpath('//div[@class="beds"]/text()').extract_first('').strip()
        if room_count_text:
            try:
                room_count = re.findall(r'\d+', room_count_text)[0]     
            except:
                room_count = ''
        else:
            room_count = ''
        rent_string = response.xpath('//span[@class="displayprice"]/text()').extract_first('').strip()
        detail_lists = response.xpath('//div[@class="description"]//text()').extract()
        bathroom_count = ''
        floor = ''
        for detail in detail_lists:
            if '2 bathroom' in detail.lower() or '2 tiled bathrooms' in detail.lower():
                bathroom_count = '2'
            elif '3 Bathroom' in detail.lower() or '3 Tiled Bathrooms' in detail.lower():
                bathroom_count = '3'
            elif '1 Bathroom' in detail.lower() or '1 Tiled Bathrooms' in detail.lower():
                bathroom_count = '1'
            if 'floor' in detail.lower():
                if 'first' in detail.lower(): 
                    floor = '1'
                elif 'second' in detail.lower(): 
                    floor = '2'
                elif 'third' in detail.lower():
                    floor = '3'
                elif 'forth' in detail.lower():
                    floor = '4'
                elif 'fifth' in detail.lower():
                    floor = '5'
        lat_lon = re.search(r'javascript:loadGoogleMapv3\((.*?)\)', response.text).group(1)
        latitude = lat_lon.split(',')[0] 
        longitude = lat_lon.split(',')[1]  
        item_loader = ListingLoader(response=response)
        item_loader.add_value('property_type', property_type)
        item_loader.add_value('external_link', external_link)
        item_loader.add_value('address', address)
        item_loader.add_value('city', city)
        item_loader.add_value('zipcode', zipcode)
        item_loader.add_xpath('description', '//div[@class="description"]//text()')
        item_loader.add_value('rent_string', rent_string)
        item_loader.add_xpath('images', '//img[@class="propertyimage"]/@src')
        item_loader.add_value('latitude', str(latitude))
        item_loader.add_value('longitude', str(longitude))
        item_loader.add_value('room_count', str(room_count))
        if str(floor).strip() !='':
            item_loader.add_value('floor', floor)
        if bathroom_count.strip() != '':
            item_loader.add_value('bathroom_count', str(bathroom_count))     
        item_loader.add_value('landlord_name', 'City Huts')
        item_loader.add_value('landlord_email', 'info@cityhuts.co.uk')
        item_loader.add_value('landlord_phone', '020 3960 3336')
        item_loader.add_value("external_source", "{}_PySpider_{}_{}".format(self.name.capitalize(), self.country, self.locale))
        yield item_loader.load_item() 