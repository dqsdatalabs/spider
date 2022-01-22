# -*- coding: utf-8 -*-
# Author: Mehmet Kurtipek

import scrapy, copy, urllib, re
from ..loaders import ListingLoader
from python_spiders.helper import extract_number_only, remove_white_spaces
import js2xml
import lxml.etree
from parsel import Selector
import math
from math import ceil

class PomppropertiesSpider(scrapy.Spider):
    name = "pompproperties_com"
    allowed_domains = ["pompproperties.com"]
    execution_type = 'testing'
    country = 'united_kingdom'
    locale ='en'
    thousand_separator=','
    scale_separator='.'
    api_url = 'https://www.pompproperties.com/lettings/properties-available-to-rent'
    params = {'page':1}
    position = 0

    def start_requests(self):
        start_urls = [self.api_url + "?" + urllib.parse.urlencode(self.params)]
        for url in start_urls:
            yield scrapy.Request(url=url,
                                 callback=self.parse,
                                 meta={'request_url': url,
                                       'params': self.params})

    def parse(self, response, **kwargs):
        room_count_lst = []
        bathroom_count_lst = []
        listing = response.xpath('.//a[contains(text(),"View Property Details")]/@href').extract()
        property_details = response.xpath('.//*[@class="sm"]')
        for i in property_details:
            room_count = i.xpath('.//../p[@class="sm"]//i[contains(@class,"bath")]/preceding-sibling::text()').extract_first()
            bathroom_count = i.xpath('.//../p[@class="sm"]//i[contains(@class,"bath")]/following-sibling::text()').extract_first()
            room_count_lst.append(room_count)
            bathroom_count_lst.append(bathroom_count)
        new_list = zip(room_count_lst,bathroom_count_lst,listing)
        for x,y,z in new_list:
            yield scrapy.Request(
                url='https://www.pompproperties.com'+z,
                callback=self.get_property_details,
                meta={'request_url': 'https://www.pompproperties.com'+z,
                      'bathroom_count':y,
                      'room_count':x}
            )

        if len(response.xpath('.//*[@class="but"]/a[contains(@class,"btn-default")]')) > 0:
            current_page = response.meta["params"]["page"]
            params1 = copy.deepcopy(self.params)
            params1["page"] = current_page + 1
            next_page_url = self.api_url + "?" + urllib.parse.urlencode(params1)
            yield scrapy.Request(
                url=response.urljoin(next_page_url),
                callback=self.parse,
                meta={'request_url': next_page_url,
                      'params': params1}
            )

    def get_property_details(self, response):
        item_loader = ListingLoader(response=response)
        tag = response.xpath('.//*[@class="property-tag-static"]/div/text()').extract_first()
        if not re.search(r'let[^\w]*agree',tag,re.IGNORECASE):
            title = response.xpath('.//*[contains(@class,"leftc")]/p[@class="lg"]//text()').extract_first()
            
            item_loader.add_xpath('description','.//p[@class="xlg4"]/following-sibling::p/text()')
            text = item_loader.get_output_value('description')

            apartment_types = ["appartement", "apartment", "flat",
                               "penthouse", "duplex", "triplex", "residential complex"]
            house_types = ['chalet', 'bungalow', 'maison', 'house', 'home']
            studio_types = ["studio"]

            prop_type = ""
            if any (i in title.lower() for i in studio_types) or any (i in text.lower() for i in studio_types):
                prop_type = 'studio'
            if any (i in title.lower() for i in house_types) or any (i in text.lower() for i in house_types):
                prop_type = 'house'
            if any (i in title.lower() for i in apartment_types) or any(i in text.lower() for i in apartment_types):
                prop_type = 'apartment'
            
            if prop_type:
                item_loader.add_value("property_type", prop_type)
            elif "bedroom" in text.lower():
                item_loader.add_value("property_type", "apartment")
            else: return
            
            external_link = response.meta.get('request_url')
            item_loader.add_value("external_link", external_link)
    
            external_id = external_link.split('/')[-1]
            item_loader.add_value('external_id',external_id)
    
            item_loader.add_xpath('images','.//*[@id="images"]//a/@href')
    
            floor_plan_images = response.xpath('.//*[@id="floorplan"]//a/@href').extract()
            if floor_plan_images:
                floor_plan_images = [floor_plan_image for floor_plan_image in floor_plan_images]
                item_loader.add_value('floor_plan_images',floor_plan_images)
    
            titles_str = response.xpath('.//*[@name="viewport"]/following-sibling::title/text()').extract_first()
            if titles_str:
                item_loader.add_value('title',remove_white_spaces(titles_str.split('|')[0]))
            else:
                item_loader.add_value('title',remove_white_spaces(title))
    
            rent_string = response.xpath('.//*[contains(@class,"rightc")]/p[@class="xlg4"]//text()').extract_first()
            if 'p/w' in rent_string:
                rent = str(ceil(float(extract_number_only(rent_string,thousand_separator=',', scale_separator='.'))*4))
                item_loader.add_value('rent_string','£'+rent)
            else:
                item_loader.add_value('rent_string',rent_string)
                
            item_loader.add_xpath('address','.//*[contains(@class,"leftc")]/p[@class="xlg4"]//text()')
            
            address = item_loader.get_output_value('address')
            zipcode1 = re.search(r'(GIR|[A-Za-z]\d[A-Za-z\d]?|[A-Za-z]{2}\d[A-Za-z\d]?)[ ]??(\d[A-Za-z]{0,2})??$',address,re.IGNORECASE)
            zipcode2 = re.search(r'(([A-Z][A-HJ-Y]?\d[A-Z\d]?|ASCN|STHL|TDCU|BBND|[BFS]IQQ|PCRN|TKCA) ?\d[A-Z]{2}|BFPO ?\d{1,4}|(KY\d|MSR|VG|AI)[ -]?\d{4}|[A-Z]{2} ?\d{2}|GE ?CX|GIR ?0A{2}|SAN ?TA1)$',address,re.IGNORECASE)
            
            if zipcode1:
                item_loader.add_value('zipcode',remove_white_spaces(zipcode1.group()))
                city = remove_white_spaces(address.split(', ')[-2])
                item_loader.add_value('city',city)
            elif zipcode2:
                item_loader.add_value('zipcode',remove_white_spaces(zipcode2.group()))
                city = remove_white_spaces(address.split(', ')[-2])
                item_loader.add_value('city',city) 
            elif zipcode1==None and zipcode2 == None and len(address.split(', '))>1:
                if ''.join(address.split(', ')[-1].split()).isalpha()==False and not any(i for i in ''.join(address.split(', ')[-1].split()) for i in ['-','–','—',',',"'"]):
                    item_loader.add_value('zipcode',remove_white_spaces(address.split(', ')[-1]))
                    item_loader.add_value('city',remove_white_spaces(address.split(', ')[-2]))
                else:
                    city = remove_white_spaces(address.split(', ')[-1])
                    item_loader.add_value('city',city.strip(' , '))
            else:
                city = remove_white_spaces(address.split(', ')[-1])
                item_loader.add_value('city',city.strip(' , '))
                    
            room_count = response.meta.get('room_count')
            if room_count and remove_white_spaces(room_count)=='0' and any (i in title.lower() for i in studio_types):
                item_loader.add_value('room_count','1')
            elif room_count and remove_white_spaces(room_count)!='0':
                item_loader.add_value('room_count',remove_white_spaces(room_count))
            elif room_count==None and any (i in title.lower() for i in studio_types):
                item_loader.add_value('room_count','1')
    
            bathroom_count = response.meta.get('bathroom_count')
            if bathroom_count and remove_white_spaces(bathroom_count)!='0':
                item_loader.add_value('bathroom_count',remove_white_spaces(bathroom_count))
                
            area_ft = re.search(r'\d+.{0,1}\d+\s{0,1}(?=sq[^\w]*ft)',text.lower())
            area_m = re.search(r'\d+.{0,1}\d+\s{0,1}(?=sq[^\w]*m)',text.lower())
            if area_ft:
                square = math.ceil(float(extract_number_only(area_ft.group(),thousand_separator=',',scale_separator='.'))*0.092903)
                item_loader.add_value('square_meters',str(int(square)))
            elif area_m:
                square = math.ceil(float(extract_number_only(area_m.group(),thousand_separator=',',scale_separator='.'))*0.092903)
                item_loader.add_value('square_meters',str(int(square)))               
                
            item_loader.add_value("external_source", "Pompproperties_PySpider_{}_{}".format(self.country, self.locale))
    
            javascript = response.xpath('.//script[contains(text(),"propertyLatitude")]/text()').extract_first()
            if javascript:
                xml = lxml.etree.tostring(js2xml.parse(javascript), encoding='unicode')
                selector = Selector(text=xml)
                lat = selector.xpath('.//var[@name="propertyLatitude"]/number/@value').extract_first()
                lng = selector.xpath('.//var[@name="propertyLongitude"]/number/@value').extract_first()
                if lat and lng:
                    item_loader.add_value('latitude', lat)
                    item_loader.add_value('longitude', lng)
    
    
            item_loader.add_value('landlord_name','Pomp Properties')
            item_loader.add_xpath('landlord_phone','.//*[@class="col-xs-12 "]//a[contains(@href,"tel")]/text()')
            item_loader.add_xpath('landlord_email','.//*[@class="col-xs-12 "]//a[contains(@href,"mail")]/text()')
    
            self.position += 1
            item_loader.add_value('position',self.position)
            yield item_loader.load_item()
            
    
