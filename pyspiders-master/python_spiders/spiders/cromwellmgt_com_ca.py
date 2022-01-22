# Author : Mohamed Helmy

import scrapy
from scrapy import Request, Spider, Item, Field
from ..loaders import ListingLoader
from ..items import ListingItem
import json
from datetime import date, datetime
import dateparser

class CromWellMgtSpider(Spider):
    name = 'cromwellmgt'
    execution_type = 'development'
    country = 'canada'
    locale = 'en'
    external_source = "{}_PySpider_{}_{}".format(
        name.capitalize(), country, locale)

    def get_property(self,prop_type):
        if (prop_type == '123' or prop_type == '243' or prop_type=='247' or prop_type=='122' or prop_type=='245'):
            return 'apartment'
        elif (prop_type == '138'):
            return 'student_apartment'

    def get_city(self, city_code):
        if (city_code == '115' or city_code == '296' or city_code == '297'):
            return 'Montreal'
        elif (city_code == '116'):
            return 'Toronto'
        elif (city_code=='114' or city_code == '464'):
            return 'Quebec'
    def availability(self, avail_code):
        if (avail_code == '177'):
            return dateparser.parse('2021-12-01')
        elif (avail_code == '166'):
            return dateparser.parse('2022-01-01')
        elif (avail_code == '171'):
            return dateparser.parse('2022-06-01')
    def start_requests(self):
        yield Request(
            url = 'https://cromwellmgt.ca/wp-content/uploads/lava_all_property_1_en.json',
            callback=self.parse
            )

    
    def parse(self, response):
        data = json.loads(response.body)
        
        for item in data:
            # Skip Commercial Buildings
            if item['property_type']=='' or 'commercial' in item['post_url'] or 'Commercial' in item['post_title'] or item['_price']=='':
                continue
            request = Request(
                url=item['post_url'],
                callback=self.populate_item,
                meta={'item':item},
                
                )            
            yield request
            
    def populate_item(self, response):
        item = response.meta["item"]   
        item_loader = ListingLoader(response=response)
        property_type = self.get_property(str(item['property_type'][0]))
        rent = 0 
        area = 0
        balcony = False
        dishwasher=False
        washing_machine = False
        parking = False
        terrace = False
        lanlord_name = ''
        landlord_phone = ''
        
       
            
        images = response.xpath('//img[@class="do-img-slider"]/@src').extract()
        item_loader.add_value('external_id', str(item['post_id']))
        item_loader.add_value('external_source', self.external_source)
        item_loader.add_value('external_link', item['post_url'])
        item_loader.add_value('title', item['post_title'])
        item_loader.add_value('property_type', property_type)
        if (item['_area'] != ''):
            area = float(item['_area'])
            area = int(area)/10.7639
            item_loader.add_value('square_meters', int(int(int(area))*10.764))

        item_loader.add_value('room_count', item['_rooms'])
        item_loader.add_value('bathroom_count', item['_bathrooms'])
        item_loader.add_value('longitude', item['lng'])
        item_loader.add_value('latitude', item['lat'])
        item_loader.add_value("images", images)
        item_loader.add_value('external_images_count',
                              len(images))
        item_loader.add_value('address', response.xpath('.//div[@class="resume-immeuble"]/h5[1]/text()').extract_first())
        try:
            zip_code = response.xpath('.//div[@class="resume-immeuble"]/p/text()').extract()[2].strip()      
            item_loader.add_value('zipcode', zip_code)
        except IndexError:
            pass

        
        city_1 = item['property_city'][0]
        if (self.get_city(city_1) != ''):
            item_loader.add_value('city', self.get_city(city_1))
        elif (self.get_city(city_1) == ''):
            city_2 = item['property_city'][1]
            item_loader.add_value('city', self.get_city(city_2))

         
        # Floor plan
        if (item['_blueprint'] != ''):
            item_loader.add_value('floor_plan_images', item['_blueprint'])

        if (item['_price']!= ''):
            rent = int(float(item['_price']))
            #rent price
            item_loader.add_value('rent', int(float(item['_price'])))
            item_loader.add_value('currency', 'CAD')

        
        # Amenities List
        if (item['property_status'][0] != '' and item['property_status'][0] != '160'):
            item_loader.add_value('available_date', self.availability(item['property_status'][0]))
            

        for il in response.xpath('//ul[@class="liste-puce-blanche deux-colonnes"]/li/text()').extract():
            if ('balcony' in il):
                balcony = True
                item_loader.add_value('balcony', True)
            if ('Dishwasher' in il):
                dishwasher=True
                item_loader.add_value('dishwasher', True)
            if ('terrace' in il):
                terrace = True
                item_loader.add_value('terrace', True)
        if response.xpath('.//div[@class="resume-immeuble"]/p[3]/text()').extract_first() != None:
            if 'parking' in response.xpath('.//div[@class="resume-immeuble"]/p[3]/text()').extract_first():
                parking = True
                item_loader.add_value('parking', True)
        
        # Contact Information
        if (response.xpath('.//div[@class="col-xs-11 col-lg-8"]/text()').extract_first() != None):
            landlord_name = response.xpath('.//div[@class="col-xs-11 col-lg-8"]/text()').extract_first().strip()
            item_loader.add_value('landlord_name', landlord_name)
        if (response.xpath('.//div[@class="col-xs-11 col-lg-8"]/a/text()').extract_first() != None):
            landlord_phone = response.xpath('.//div[@class="col-xs-11 col-lg-8"]/a/text()').extract_first()
            item_loader.add_value('landlord_phone', landlord_phone)
        
        item_loader.add_value('landlord_email', 'info@cromwellmgt.ca')
        
        yield item_loader.load_item()
