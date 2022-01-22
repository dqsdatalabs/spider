# -*- coding: utf-8 -*-
# Author: Mehmet Kurtipek

import scrapy
import json
from ..loaders import ListingLoader

class AlphabitatSpider(scrapy.Spider):
    name = "alphabitat"
    allowed_domains = ["www.alphabitat.be"]
    start_urls = (
        'http://www.www.alphabitat.be/',
    )
    execution_type = 'testing'
    country = 'belgium'
    locale ='nl'
    thousand_separator=','
    scale_separator='.'
    
    def start_requests(self):
        start_urls = [
            {
                'url': 'https://www.alphabitat.be/.netlify/functions/estate-request?params={%22ClientId%22:%22API_TOKEN%22,%22Page%22:%200,%22Language%22:%22en-gb%22,%22RowsPerPage%22:10,%22CategoryIDList%22:[2],%20%22PriceRange%22:%20[0,%201000000000]%20,%22RegionIDList%22:%20[],%22PurposeStatusIDList%22:%20[2],%20%22MinRooms%22:%20null,%20%22MinBathRooms%22:null,%20%22AreaRange%22:%20[0,%201000],%20%22OrderByFields%22:[%22Price%20ASC%22]%20}',
                        
                'property_type': 'apartment'
            },
            {
                'url': 'https://www.alphabitat.be/.netlify/functions/estate-request?params={%22ClientId%22:%22API_TOKEN%22,%22Page%22:%200,%22Language%22:%22en-gb%22,%22RowsPerPage%22:10,%22CategoryIDList%22:[1],%20%22PriceRange%22:%20[0,%201000000000]%20,%22RegionIDList%22:%20[],%22PurposeStatusIDList%22:%20[2],%20%22MinRooms%22:%20null,%20%22MinBathRooms%22:null,%20%22AreaRange%22:%20[0,%201000],%20%22OrderByFields%22:[%22Price%20ASC%22]%20}',
                'property_type': 'house'
            }
        ]
        for url in start_urls:
            yield scrapy.Request(
                url=url.get('url'),
                callback=self.parse, meta={'property_type': url.get('property_type')},
            )

    def parse(self, response, **kwargs):
        
        page = response.meta.get("page", 1)
        seen = False
        datas = json.loads(response.text)
        for data in datas['d']['EstateList']:
            seen = True
            property_type = response.meta.get('property_type')
            external_id = str(data['EstateID'])
            rent = str(data['Price']) + data['Currency']
            city = data['City']
            zipcode = str(data['Zip'])
            address = data['Address1'] + ' ' + str(data['Number']) + ' ' + zipcode + ' ' + city
            square_meters = str(data['Area'])
            bathrooms = str(data['BathRooms'])
            room_count = str(data['Rooms'])
            terrace = str(data['Terrace'])
            if 'None' not in terrace:
                if int(terrace) > 0:
                   terrace = True
                else:
                    terrace = ''
            else:
                terrace = ''
            parking = str(data['Parking'])
            if 'None' not in parking:
                if int(parking) > 0:
                    parking = True
                else:
                    parking = ''
            else:
                parking = '' 
            furnished = str(data['Furnished'])
            if 'None' not in furnished:
                if int(furnished) > 0:
                    furnished = True
                else:
                    furnished = ''
            else:
                furnished = '' 
            description = data['ShortDescription']
            floor = data['Floor']
            external_link = 'https://www.alphabitat.be/property?id={}'.format(external_id)
            images = []
            for picture in data['Pictures']:
                img = picture['UrlLarge']
                images.append(img) 
            
            item_loader = ListingLoader(response=response)
            item_loader.add_value('property_type', property_type)
            item_loader.add_value('external_id', external_id)
            item_loader.add_value('external_link', external_link)
            item_loader.add_value('title', address)
            item_loader.add_value('address', address)
            item_loader.add_value('city', city)
            item_loader.add_value('zipcode', zipcode)
            item_loader.add_value('description', description)
            item_loader.add_value('rent_string', rent)
            item_loader.add_value('images', images)            
            if room_count.isdigit() and  int(room_count) > 0:                
                item_loader.add_value('room_count', room_count)
            item_loader.add_value('bathroom_count', bathrooms)
            if terrace:
                item_loader.add_value('terrace', True)
            if parking:
                item_loader.add_value('parking', True)
            if furnished: 
                item_loader.add_value('furnished', True)
            item_loader.add_value('square_meters', square_meters)
            item_loader.add_value('landlord_name', 'Immo Alphabitat')
            item_loader.add_value('landlord_email', ' info@alphabitat.be')
            item_loader.add_value('landlord_phone', '02-770 31 21')
            item_loader.add_value("external_source", "{}_PySpider_{}_{}".format(self.name.capitalize(), self.country, self.locale))
            yield item_loader.load_item()

        if page == 1 or seen:
            f_url = response.url.replace(f"Page%22:%20{page-1}", f"Page%22:%20{page}")
            yield scrapy.Request(f_url, callback=self.parse, meta={"property_type":response.meta.get('property_type'), "page":page+1})