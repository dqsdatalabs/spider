# -*- coding: utf-8 -*-
# Author: Mehmet Kurtipek

import re
import scrapy
import lxml.etree
import js2xml
from scrapy import Selector
from ..loaders import ListingLoader
from ..helper import remove_white_spaces
 

class LiveNobleCoUkSpider(scrapy.Spider):
    name = 'livenoble_co_uk'
    allowed_domains = ['livenoble.co.uk']
    execution_type = 'testing'
    country = 'united_kingdom'
    locale ='en'
    thousand_separator=','
    scale_separator='.'
    position=0

    def start_requests(self):
        start_urls = [
            'https://livenoble.co.uk/property/?wppf_search=to-rent'
        ]

        for url in start_urls:
            yield scrapy.Request(
                url=url,  
                callback=self.parse,
                )

    def parse(self, response, **kwargs):
       
        listings = response.xpath('//div[contains(@class,"card-alt")]')
        for property_item in listings:
            #Let agreed check
            let_check = property_item.xpath('.//div/div[contains(@class,"danger")]')
            if let_check:
                continue
            property_url = property_item.xpath(".//@href").extract_first()
                
            yield scrapy.Request(
                url=property_url,
                callback=self.get_property_details,
                meta={'request_url': property_url}
                )
        next_page_url=response.xpath('//a[contains(@class,"next")]/@href').extract_first()
        if next_page_url:
            yield scrapy.Request(
                url=next_page_url, 
                callback=self.parse
             )
 
    def get_property_details(self, response):

        item_loader = ListingLoader(response=response)
        external_id = response.xpath("//link[contains(@rel,'shortlink')]//@href").get()
        if external_id:
            external_id = external_id.split("p=")[-1]
            item_loader.add_value('external_id', external_id)
        desc = response.xpath('.//div[contains(@class,"text-secondary")]//text()').extract()
        if desc=="":
            return 
        description = "".join([remove_white_spaces(d) for d in desc])
        bedrooms = response.xpath('.//b[contains(text(),"Bedrooms")]/text()').extract_first()
        room_count = re.findall(r'\d', bedrooms)
        bathrooms = response.xpath('.//b[contains(text(),"Bathrooms")]/text()').extract_first()
        bathrooms = re.findall(r'\d', bathrooms)
        # Checking for office/commercial space
        if ('office' in description.lower() or 'commercial space' in description.lower()) and (not room_count and not bathrooms):
            return
        item_loader.add_value("external_link", response.meta.get('request_url'))
        external_link= response.meta.get('request_url')
        if "lane-denstone-st14-5hu" in external_link:
            return
        if "daddlebrook-hollinswood" in external_link:
            item_loader.add_value('property_type', 'House')
        
        item_loader.add_xpath('title', '//h1[contains(@class,"display-5")]//text()')        
        if room_count:
            item_loader.add_value('room_count', room_count[0])        
        if bathrooms:
            item_loader.add_value('bathroom_count', bathrooms[0])        
        address = item_loader.get_output_value('title')
        item_loader.add_value('address', address)
        city = response.xpath('.//h4[contains(@class,"text-secondary")]/text()').extract_first()
        if city!=' ':
            item_loader.add_value('city', city.split(',')[0])
            zipcode = re.search(r'[A-Z]{1,2}\d{1,2} *\d*\w*', address)
            if zipcode:
                item_loader.add_value('zipcode', zipcode[0])
        elif city == ' ': 
            city = response.xpath("//h1//text()").extract_first()  
            if city and "," in city:
                city = city.split(",")[-2]
            item_loader.add_value('city', city.strip())
            zipcode = re.search(r'[A-Z]{1,2}\d{1,2} *\d*\w*', address)
            if zipcode: 
                item_loader.add_value('zipcode', zipcode[0])

            
        
        javascript = response.xpath('//script[contains(text(),"propertyLocation")]/text()').extract_first()
        if javascript:
            xml = lxml.etree.tostring(js2xml.parse(javascript), encoding='unicode')
            selector = Selector(text=xml)
            latitude = selector.xpath('.//var[@name="propertyLocation"]//property[@name="lat"]/number/@value').extract_first()
            longitude = selector.xpath('.//var[@name="propertyLocation"]//property[@name="lng"]/number/@value').extract_first()
            if latitude and longitude:
                item_loader.add_value('latitude', latitude)
                item_loader.add_value('longitude', longitude)

        rent_string = remove_white_spaces(response.xpath('//span[contains(@class,"text-primary")]/text()').extract_first())
        item_loader.add_value('rent_string', rent_string)
 

        item_loader.add_value('description', description)
        apartment_types = ["apartment", "flat", "penthouse", "duplex", "dakappartement", "triplex", 'bungalow']
        house_types = ['bungalow', 'maison', 'house', 'home', 'cottage', 'detached house', 'house terrace',
                    'terrace', 'maisonette', 'mid terrace', 'semi-detached', 'detached', 'mid terraced house', 'property']
        house = ['bedroom']
        studio_types = ["studio", 'studio flat']
        prop = ""
        if any(i in description.lower() for i in studio_types):
            prop = "studio"
        elif any(i in description.lower() for i in apartment_types):
            prop = "apartment"
        elif any(i in description.lower() for i in house_types):
            prop = "house"
        elif any(i in description.lower() for i in house):
            prop = "house"
        else:
            prop = "apartment"

        item_loader.add_value('property_type', prop)    
        
        item_loader.add_xpath('images', './/img[contains(@alt,"Property image")]/@src')
        item_loader.add_value('landlord_name', "Noble Living Estate Agents")
        city_list = ['Burton', 'Derby', 'Doncaster', 'Lichfield', 'Nottingham', 'Sheffield', 'Stoke on trent', 'Telford', 'Uttoxeter','Burton-On-Trent','Stafford']
        map_dict = {
                    'Burton' : ['burton@livenoble.co.uk', '01283 205880'],
                    'Derby' : ['derby@livenoble.co.uk', '01332 527779'],
                    'Doncaster' : ['doncaster@livenoble.co.uk', '01302 771708'],
                    'Lichfield' : ['lichfield@livenoble.co.uk', '01543 758011'],
                    'Nottingham' : ['nottingham@livenoble.co.uk', '0115 9476994'],
                    'Sheffield' : ['sheffield@livenoble.co.uk', '0114 276 2777'],
                    'Stoke on trent' : ['stokeontrent@livenoble.co.uk', '01782 480057'],
                    'Telford' : ['telford@livenoble.co.uk', '01952 257168'],
                    'Uttoxeter' : ['uttoxeter@livenoble.co.uk', '01889 565669'],
                    'Burton-On-Trent' : ['burton@livenoble.co.uk', '01283 205880'],
                    'Stafford': ['stokeontrent@livenoble.co.uk', '01782 480057'],
                    'Staffs':['uttoxeter@livenoble.co.uk','01889 565669']
    
                    }
        if item_loader.get_output_value('city') in city_list:
            item_loader.add_value('landlord_email', map_dict[item_loader.get_output_value('city')][0])
            item_loader.add_value('landlord_phone', map_dict[item_loader.get_output_value('city')][1])

        elif not item_loader.get_output_value('city') in city_list:
            item_loader.add_value('landlord_email', "info@livenoble.co.uk")
            item_loader.add_value('landlord_phone', "0114 276 2777")

        self.position += 1
        item_loader.add_value("position", self.position)
        item_loader.add_value("external_source", "Livenoble_PySpider_{}_{}".format(self.country, self.locale))
        yield item_loader.load_item()
