# -*- coding: utf-8 -*-
# Author: Valerii Nikitiuk
import scrapy
import re
from ..loaders import ListingLoader

def extract_city_zipcode(_address):
    city = _address.split(", ")[-2]
    zipcode = _address.split(", ")[-1]
    return city, zipcode 

class ArlingtonresidentialSpider(scrapy.Spider):
    name = "arlingtonresidential"
    allowed_domains = ["arlingtonresidential.com"]
    start_urls = (
        'http://www.arlingtonresidential.com/',
    )
    execution_type = 'testing'
    country = 'united_kingdom'
    locale ='en'
    thousand_separator=','
    scale_separator='.'

    def start_requests(self):
        start_urls = [
            {'url': 'https://arlingtonresidential.com/search-page/?channel=2&searchview=grid&status=0&searchview=grid&location=any&type=flat&bedrooms=any&min-price=any&max-price=any&price=high', 'property_type': 'apartment'},
            {'url': 'https://arlingtonresidential.com/search-page/?channel=2&searchview=grid&status=0&searchview=grid&location=any&type=house&bedrooms=any&min-price=any&max-price=any&price=high', 'property_type': 'house'}
        ]
        for url in start_urls:
            yield scrapy.Request(
                url=url.get('url'),
                callback=self.parse, 
                meta={'property_type': url.get('property_type')},
                dont_filter=True
            )

    def parse(self, response, **kwargs):
        links = response.xpath('//div[@class="feturd_pic"]')
        for link in links:
            let_ex = link.xpath('./div[@class="prdct_tag"]/text()').extract_first('').strip()
            if 'to let' in let_ex.lower():  
                url = response.urljoin(link.xpath('./a/@href').extract_first())
                yield scrapy.Request(url=url, callback=self.get_property_details, dont_filter=True, meta={'property_type': response.meta.get('property_type')})
        if response.xpath('//a[contains(text(), ">")]/@href'):
            next_link = response.xpath('//a[contains(text(), ">")]/@href').extract_first()
            yield scrapy.Request(url=next_link, callback=self.parse, dont_filter=True, meta={'property_type': response.meta.get('property_type')})

    def get_property_details(self, response):
        # parse details of the propery
        property_type = response.meta.get('property_type')
        external_link = response.url
        address = response.xpath('//div[@class="sale_prodct"]/h3/text()').extract_first('')
        city, zipcode = extract_city_zipcode(address) 
        try:
            room_count = int(response.xpath('//div[@class="sale_prodct"]/ul/li[1]/a/text()').extract_first('').strip())
        except:
            room_count = ''
        bathrooms = response.xpath('//div[@class="sale_prodct"]/ul/li[2]/a/text()').extract_first('').strip() 
        rent_string = ''.join(response.xpath('//div[@class="sale_prodct"]//h4/text()').extract()).strip().split('|')[1]
        try:
            lat_lon = re.search(r'position\: new google\.maps\.LatLng\((.*?)\)', response.text).group(1)
            lat = lat_lon.split(',')[0]
            lon = lat_lon.split(',')[1]  
        except:
            lat = ''
            lon = ''
        if room_count: 
            item_loader = ListingLoader(response=response)
            item_loader.add_value('property_type', property_type)
            item_loader.add_value('external_link', external_link)
            item_loader.add_value('address', address)
            item_loader.add_value('city', city)
            item_loader.add_value('zipcode', zipcode)
            item_loader.add_xpath('description', '//h4[contains(text(), "DESCRIPTION")]/following-sibling::p/text()')
            item_loader.add_value('rent_string', rent_string)
            item_loader.add_xpath('images', '//div[@class="product_bx"]/img/@src')
            item_loader.add_xpath('floor_plan_images', '//div[@class="floorplans"]/img/@src')
            if room_count:
                item_loader.add_value('room_count', str(room_count))
            if bathrooms: 
                item_loader.add_value('bathroom_count', str(bathrooms))
            if lat:
                item_loader.add_value('latitude', str(lat))
            if lon:
                item_loader.add_value('longitude', str(lon))
            item_loader.add_value('landlord_name', 'Arlington Residential')
            item_loader.add_value('landlord_email', 'office@arlingtonresidential.com')
            item_loader.add_value('landlord_phone', '+44 20 7722 3322')
            item_loader.add_value("external_source", "{}_PySpider_{}_{}".format(self.name.capitalize(), self.country, self.locale))
            yield item_loader.load_item() 