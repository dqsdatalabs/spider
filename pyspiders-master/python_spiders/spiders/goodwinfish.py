# -*- coding: utf-8 -*-
# Author: Mehmet Kurtipek

import scrapy
import re
from ..loaders import ListingLoader
import dateparser

class GoodwinfishSpider(scrapy.Spider):
    name = "goodwinfish"
    allowed_domains = ["www.goodwinfish.com"]
    start_urls = (
        'http://www.www.goodwinfish.com/',
    )
    execution_type = 'testing'
    country = 'united_kingdom'
    locale ='en'
    thousand_separator=','
    scale_separator='.'
   
    def start_requests(self):
        start_urls = [
            {'url': 'https://www.goodwinfish.com/properties/?category=1&Areas=ALL%7C0&Bedrooms=0&PriceMin=0&PriceMax=0&StatusID=&PropertyDevelopmentID=&SearchText=&PerPage=120&sort=Highest+Price', 'property_type': 'apartment'},
        ]
        for url in start_urls:
            yield scrapy.Request(
                url=url.get('url'),
                callback=self.parse, 
                meta={'property_type': url.get('property_type')},
                dont_filter=True
            )
    def parse(self, response, **kwargs):
        links = response.xpath('//div[contains(@class, "imageGrowth")]')
        for link in links: 
            url = response.urljoin(link.xpath('./a/@href').extract_first())
            room_count = ''.join(link.xpath('./following-sibling::div/div[@class="propertyLine1"]/text()').extract()).strip()
            rent = ''.join(link.xpath('./following-sibling::div/div[@class="propertyPrice"]/text()').extract()).strip()
            address = ''.join(link.xpath('./following-sibling::div/div[@class="propertyAddress"]/text()').extract()).strip()
            yield scrapy.Request(url=url, callback=self.get_property_details, dont_filter=True, meta={'property_type': response.meta.get('property_type'), 'room_count': room_count, 'rent': rent, 'address': address})
    
    def get_property_details(self, response):
        property_type = response.meta.get('property_type')
        external_link = response.url
        external_id = response.xpath('//small[contains(text(), "Ref")]/text()').extract_first().split('. ')[-1]
        lat_lon_text = response.xpath('//a[contains(text(), "MAP / DIRECTIONS")]/@href').extract_first()
        lat_lon = re.search(r'q=(.*)', lat_lon_text).group(1)
        latitude = lat_lon.split(',')[0]
        longitude = lat_lon.split(',')[1]  
        features = response.xpath('//h2/following-sibling::ul/li//text()').extract()
        terrace = ''
        parking = ''
        furnished = ''
        floor = ''
        avaialbe_date = ''
        date2 = ''
        for feature in features:
            if 'terrace' in feature.lower():
                terrace = True
            if 'parking' in feature.lower():
                parking = True
            if 'furnished' in feature.lower():
                furnished = True 
            if 'sixth floor' in feature.lower():
                floor = '6' 
            elif 'first floor' in feature.lower():
                floor = '1'
            elif 'second floor' in feature.lower():
                floor = '2'
            elif 'third floor' in feature.lower():
                floor = '3' 
            if 'available' in feature.lower():
                avaialbe_date = feature.lower().replace('available ', '')
                date_parsed = dateparser.parse( avaialbe_date, date_formats=["%m %d %Y"] ) 
                try:
                    date2 = date_parsed.strftime("%Y-%d-%m")
                except:
                    date2 = '' 
        description = ''.join(response.xpath('//h2/following-sibling::p//text()').extract())
        if 'balconies' in description.lower():
            balcony = True 
        else:
            balcony = ''
        address = response.meta.get('address')
        if response.meta.get('room_count'):
            try:
                room_count = re.findall(r'\d+', response.meta.get('room_count'))[0]
            except:
                room_count = ''
      
        rent = response.meta.get('rent')
        item_loader = ListingLoader(response=response)
        if property_type:
            item_loader.add_value('property_type', property_type)
        item_loader.add_value('external_link', external_link)
        item_loader.add_xpath('title', '//title/text()')
        item_loader.add_value('external_id', str(external_id))
        if address:
            item_loader.add_value('address', address)
            item_loader.add_value('city', address.split(",")[-1].strip())
        item_loader.add_xpath('description', '//h2/following-sibling::p//text()')
        item_loader.add_value('rent_string', rent)
        item_loader.add_xpath('images', '//img[@class="img-responsive"]/@src')
        if terrace:
            item_loader.add_value('terrace', True)
        if furnished:
            item_loader.add_value('furnished', True)
        if balcony:
            item_loader.add_value('balcony', True)
        if date2:
            item_loader.add_value('available_date', date2)
        if floor:
            item_loader.add_value('floor', str(floor))
        if parking:
            item_loader.add_value('parking', True)
        item_loader.add_value('latitude', str(latitude))
        item_loader.add_value('longitude', str(longitude))
        item_loader.add_value('room_count', str(room_count))
        item_loader.add_value('landlord_name', 'Goodwin Fish')
        item_loader.add_value('landlord_email', 'info@goodwinfish.com')
        item_loader.add_value('landlord_phone', '0161 794 5000')
        item_loader.add_value("external_source", "{}_PySpider_{}_{}".format(self.name.capitalize(), self.country, self.locale))

        from word2number import w2n
        
        bathroom_count = response.xpath("//li[contains(.,'Bath') or contains(.,'bath')]//text()").get()
        if bathroom_count:
            bathroom_count = bathroom_count.strip().split(" ")[0]
            if bathroom_count.isdigit():
                item_loader.add_value("bathroom_count", bathroom_count)
            else:
                try:
                    item_loader.add_value("bathroom_count", w2n.word_to_num(bathroom_count))
                except: pass
                
        yield item_loader.load_item()