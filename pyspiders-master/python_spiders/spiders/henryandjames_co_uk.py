# -*- coding: utf-8 -*-
# Author: Karan Katle
# Team: Sabertooth 
import scrapy
from ..loaders import ListingLoader
from python_spiders.helper import extract_number_only, remove_white_spaces, convert_string_to_numeric
import js2xml
import lxml.etree
from parsel import Selector
import math, re

class HenryandjamesSpider(scrapy.Spider):
    name = "henryandjames_co_uk"
    allowed_domains = ["henryandjames.co.uk"]
    execution_type = 'testing'
    country = 'united_kingdom'
    locale ='en'
    thousand_separator=','
    scale_separator='.'
    position=0

    def start_requests(self):
        start_urls = ['https://www.henryandjames.co.uk/property-lettings/?sort=high_to_low']
        for url in start_urls:
            yield scrapy.Request(url=url,
                                 callback=self.parse,
                                 meta={'request_url':url})
            
    def parse(self, response, **kwargs):
        room_count_lst = []
        bathroom_count_lst = []
        listing = response.xpath('.//div[@class="property-thumbnail"]//following-sibling::a/@href').extract()
        property_details = response.xpath('.//section[contains(@class,"property-instance")]')
        for i in property_details:
            room_count = i.xpath('.//*[contains(@id,"svg-receptionroom")]/preceding-sibling::text()').extract_first()
            bathroom_count = i.xpath('.//*[contains(@id,"svg-bathroom")]/preceding-sibling::text()').extract_first()
            room_count_lst.append(room_count)
            bathroom_count_lst.append(bathroom_count)
        new_list = zip(room_count_lst,bathroom_count_lst,listing)
        for x,y,z in new_list:
            yield scrapy.Request(
                url=z,
                callback=self.get_property_details,
                meta={'request_url': z,
                      'bathroom_count':y,
                      'room_count':x}
            )          

    def get_property_details(self, response):
        item_loader = ListingLoader(response=response)
        room_count = response.meta.get('room_count')
        bathroom_count = response.meta.get('bathroom_count')
        title = response.xpath('.//*[@class="lead"]/text()').extract_first()
        accomodation = response.xpath('.//*[@class="accommodation"]/p/text()').extract()
        apartment_types = ["appartement", "apartment", "flat",
                           "penthouse", "duplex", "triplex", "residential complex"]
        house_types = ['chalet', 'bungalow', 'maison', 'house', 'home',"villa","maisonette"]
        studio_types = ["studio"]
        external_id = response.xpath('.//*[@class="ref-number"]/text()').extract_first()       
        if external_id:
            item_loader.add_value('external_id',remove_white_spaces(external_id.split(':')[-1]))
        item_loader.add_value('external_link', response.meta.get('request_url'))
        item_loader.add_xpath('description','.//*[@id="property-description-tab"]/p/text()')
        description = item_loader.get_output_value('description').lower()
        item_loader.add_xpath('images','.//div[@data-tab-name="Property images"]//img/@src')
        item_loader.add_xpath('floor_plan_images','.//a/@data-floorplan-large')
        item_loader.add_xpath('title','.//*[@class="lead"]/text()')
        features = ' '.join(response.xpath('.//*[@class="features"]//li/text()').extract())
        if any (i in title.lower() for i in studio_types) \
            or any (i in features.lower() for i in studio_types) \
            or any (i in description.lower() for i in studio_types):
            item_loader.add_value('property_type','studio')
        elif any (i in title.lower() for i in house_types) \
            or any (i in features.lower() for i in house_types ) \
            or any (i in description.lower() for i in house_types):
            item_loader.add_value('property_type','house')
        elif any (i in title.lower() for i in apartment_types) \
            or any (i in features.lower() for i in apartment_types ) \
            or any (i in description.lower() for i in apartment_types):
            item_loader.add_value('property_type','apartment')
        property_type = item_loader.get_output_value('property_type')
        
        if room_count and remove_white_spaces(room_count)=='0' and property_type == 'studio':
            item_loader.add_value('room_count','1')
        elif room_count and remove_white_spaces(room_count)!='0':
            item_loader.add_value('room_count',remove_white_spaces(room_count))
        
        if bathroom_count and remove_white_spaces(bathroom_count)!='0':
            item_loader.add_value('bathroom_count',remove_white_spaces(bathroom_count))

        # Loop had to used here because of the way it is on the website      
        for item in accomodation:
            # if 'bedroom' in remove_white_spaces(i).lower():
            #     room = extract_number_only(i,thousand_separator=',', scale_separator='.')
            #     if room == '0' and any (i in title.lower() for i in studio_types):
            #         item_loader.add_value('room_count','1')
            #     elif room=='0' and any (i in char.lower() for i in studio_types for char in features):
            #         item_loader.add_value('room_count','1')
            #     elif room=='0' and any (i in description.lower() for i in studio_types):
            #         item_loader.add_value('room_count','1')
            #     elif room != '0':
            #         item_loader.add_value('room_count', room)
            # elif 'bathroom' in remove_white_spaces(i).lower():
            #     bathroom = extract_number_only(i,thousand_separator='.', scale_separator=',')
            #     if bathroom!='0':
            #         item_loader.add_value('bathroom_count',bathroom)
            if "sq ft" in remove_white_spaces(item).lower():
                square_feet = convert_string_to_numeric(item,HenryandjamesSpider)*0.092903
                item_loader.add_value('square_meters', str(math.ceil(square_feet)))
            elif "sq m" in remove_white_spaces(item).lower() or "sqm" in remove_white_spaces(item).lower() or "m²" in remove_white_spaces(item).lower():
                item_loader.add_value('square_meters',extract_number_only(item,thousand_separator='.', scale_separator=','))

        rent_string = response.xpath('.//h3/text()').extract_first()
        if "per week" in response.xpath('.//h3/span/text()').extract_first():
            rent = convert_string_to_numeric(rent_string,HenryandjamesSpider)*4
            item_loader.add_value('rent_string','£'+str(rent))
        else:
            rent = convert_string_to_numeric(rent_string,HenryandjamesSpider)
            item_loader.add_value('rent_string','£'+str(rent))

        if 'unfurnished' in features.lower() or 'unfurnished' in title.lower():
            item_loader.add_value('furnished',False)
        elif 'furnished' in features.lower() or 'furnished' in title.lower():
            item_loader.add_value('furnished',True)
        if 'parking' in features.lower() or 'parking' in title.lower():
            item_loader.add_value('parking',True)
        if 'lift' in features.lower() or 'lift' in title.lower():
            item_loader.add_value('elevator',True)
        if 'terrace' in features.lower() or 'terrace' in title.lower():
            item_loader.add_value('terrace',True)
        if 'balcony' in features.lower() or 'balcony' in title.lower():
            item_loader.add_value('balcony',True)
        if 'dishwasher' in features.lower() or 'dishwasher' in title.lower():
            item_loader.add_value('dishwasher',True)
        if 'washing machine' in features.lower() or 'washing machine' in title.lower():
            item_loader.add_value('washing_machine',True)
        if 'swimming pool' in features.lower() or 'swimming pool' in title.lower():
            item_loader.add_value('swimming_pool',True)

        item_loader.add_xpath('address','.//div[contains(@id,"property-detail")]//h2/text()')
        address = item_loader.get_output_value('address')
        post = remove_white_spaces(address.split(',')[-1])
        zipcode = re.search(r'(([A-Z][A-HJ-Y]?\d[A-Z\d]?|ASCN|STHL|TDCU|BBND|[BFS]IQQ|PCRN|TKCA) ?\d[A-Z]{2}|BFPO ?\d{1,4}|(KY\d|MSR|VG|AI)[ -]?\d{4}|[A-Z]{2} ?\d{2}|GE ?CX|GIR ?0A{2}|SAN ?TA1)$',address,re.IGNORECASE)
        if zipcode:
            item_loader.add_value('zipcode',zipcode.group())
            item_loader.add_value('city',address.split(', ')[-2])
        elif zipcode == None:
            item_loader.add_value('city',address.split(', ')[-1])
            if ''.join(post.split()).isalpha()==False:
                item_loader.add_value('zipcode',post)
                
        javascript = response.xpath('.//script[contains(text(),"lng")]/text()').extract_first()
        if javascript:
            xml = lxml.etree.tostring(js2xml.parse(javascript), encoding='unicode')
            selector = Selector(text=xml)
            lat = selector.xpath('.//property[@name="lat"]/number/@value').extract_first()
            lng = selector.xpath('.//property[@name="lng"]/number/@value').extract_first()
            if lat and lng:
                item_loader.add_value('latitude', lat)
                item_loader.add_value('longitude', lng)

        item_loader.add_value("external_source", "Henryandjames_PySpider_{}_{}".format(self.country, self.locale))
        item_loader.add_value('landlord_name','Henry & James')
        item_loader.add_value('landlord_phone','+ 44 (0) 20 7235 8861')
        item_loader.add_value('landlord_email','belgraviaoffice@henryandjames.co.uk')        
        #self.position += 1
        #item_loader.add_value('position', self.position)
        yield item_loader.load_item()