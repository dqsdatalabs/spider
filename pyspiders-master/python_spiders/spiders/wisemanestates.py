# -*- coding: utf-8 -*-
# Author: Mehmet Kurtipek

import scrapy
import re
from ..loaders import ListingLoader

def getSqureMtr(text):
    list_text = re.findall(r'\d+', text, re.S | re.M | re.I)

    if len(list_text) == 3:
        output = float(list_text[0]+"."+list_text[1])
    elif len(list_text) == 2:
        output = float(list_text[0]+"."+list_text[1])
    elif len(list_text) == 1:
        output = int(list_text[0])
    else:
        output=0

    return int(output)

def extract_city_zipcode(_address):
    zipcode = _address.split(", ")[-1]
    city = _address.split(", ")[-2] 
    return zipcode, city 

class WisemanestatesSpider(scrapy.Spider):
    name = "wisemanestates"
    allowed_domains = ["www.wisemanestates.com"] 
    start_urls = (
        'http://www.www.wisemanestates.com/',
    )
    execution_type = 'testing'
    country = 'united_kingdom'
    locale ='en'
    thousand_separator=','
    scale_separator='.'

    def start_requests(self):
        start_urls = [
            {'url': 'http://www.wisemanestates.com/results.asp?proptype=Apartment&pricelow=0&searchurl=%2Fdefault%2Easp%3F&pricehigh=0&market=1&pricetype=2&search=Search&', 'property_type': 'apartment'},
            {'url': 'http://www.wisemanestates.com/property/1-bedroom--to-rent-on-lancaster-gardens-w13/wise2-009377/1', 'property_type': 'house'}
        ]
        for url in start_urls:
            yield scrapy.Request(
                url=url.get('url'),
                callback=self.parse, 
                meta={'property_type': url.get('property_type')},
                dont_filter=True
            )
    
    def parse(self, response, **kwargs):
        if response.xpath('//a[contains(text(), "Property Details")]'):         
            links = response.xpath('//a[contains(text(), "Property Details")]')
            for link in links: 
                url = response.urljoin(link.xpath('./@href').extract_first())
                yield scrapy.Request(url=url, callback=self.get_property_details, dont_filter=True, meta={'property_type': response.meta.get('property_type')})
            if response.xpath('//a[contains(text(), "Next")]/@href'):
                next_link = response.urljoin(response.xpath('//a[contains(text(), "Next")]/@href').extract_first())
                yield scrapy.Request(url=next_link, callback=self.parse, dont_filter=True, meta={'property_type': response.meta.get('property_type')})
        else:
            yield scrapy.Request(url=response.url, callback=self.get_property_details, dont_filter=True, meta={'property_type': response.meta.get('property_type')})
 
    def get_property_details(self,response):
        property_type = response.meta.get('property_type')
        external_link = response.url
        
        address = response.xpath('//div[@id="detail-content"]/h1/a/text()').extract_first('').strip()
        zipcode, city = extract_city_zipcode(address)
        room_count_text = response.xpath('//div[@id="detail-header"]//span[@class="bedrooms"]/text()').extract_first('').strip()
        if room_count_text: 
            room_count = room_count_text.split(' ')[0]  
        else:
            room_count = ''
        bathroom_text = response.xpath('//div[@id="detail-header"]//span[@class="bathrooms"]/text()').extract_first('').strip()
        if bathroom_text: 
            bathrooms = bathroom_text.split(' ')[0]  
        else:
            bathrooms = '' 
        rent_string = response.xpath('//div[@id="detail-header"]//span[@class="priceask"]/text()').extract_first('').strip()
        rent = ""
        if rent_string:
            if "pw" in rent_string:
                rent_string = rent_string.split(" ")[0].replace("£","").replace(",","")
                rent = int(float(rent_string))*4
            else:
                rent = rent_string.split(" ")[0].replace("£","").replace(",","")
                
        lat_lon_text = re.search(r'new google.maps.LatLng\((.*?)\)', response.text).group(1)
        latitude = lat_lon_text.split(',')[0]
        longitude = lat_lon_text.split(',')[-1]  
        deposit_text = response.xpath('//div[@class="dialog-fees"]/p/text()').extract()
        if '£' in deposit_text[2].split(', ')[0]:
            deposit = re.findall(r'([\d|,|\.]+)', deposit_text[2].split(', ')[0], re.S | re.M | re.I)[0]
            deposit = re.sub(r',', '', deposit)
        else:
            deposit = ''
        
        
        item_loader = ListingLoader(response=response)

        item_loader.add_value('property_type', property_type)
        item_loader.add_xpath('title', '//title/text()')
        item_loader.add_value('external_link', external_link)
        if external_link:
            external_id=external_link.split("/")[-2]
            item_loader.add_value("external_id",external_id)
        item_loader.add_value('address', address)
        item_loader.add_value('city', city)
        item_loader.add_value('zipcode', zipcode)
        item_loader.add_xpath('description', '//div[@id="detail-content"]/p//text()')
        item_loader.add_value('rent_string', str(rent)+"£")
        item_loader.add_xpath('images', '//div[@id="detail-images"]//a/@data-rsbigimg')
        item_loader.add_value('latitude', latitude)
        item_loader.add_value('longitude', longitude)
        item_loader.add_value('room_count', str(room_count))
        item_loader.add_value('deposit', deposit)
        item_loader.add_value('bathroom_count', str(bathrooms))
        
        furnished = response.xpath("//p//text()[contains(.,'FURNISHED')]").get()
        if furnished:
            item_loader.add_value("furnished", True)
            
        item_loader.add_value('landlord_name', 'Wisemanestates')
        item_loader.add_value('landlord_email', ' info@wisemanestates.com')
        item_loader.add_value('landlord_phone', '0207 713 1111')
        item_loader.add_value("external_source", "{}_PySpider_{}_{}".format(self.name.capitalize(), self.country, self.locale))
        yield item_loader.load_item()