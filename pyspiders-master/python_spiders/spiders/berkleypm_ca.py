# -*- coding: utf-8 -*-
# Author: Noor, Mohamed Zakaria
import json
from math import ceil

import scrapy
from scrapy.exceptions import CloseSpider

from ..helper import sq_feet_to_meters
from ..loaders import ListingLoader

class MySpider(scrapy.Spider):
    name = 'berkleypm_ca'
    execution_type = 'testing'
    country = 'canada'
    locale = 'en'
    external_source = "{}_PySpider_{}_{}".format(name.capitalize(), country, locale)
    thousand_separator = ','
    scale_separator = '.'
    start_urls = [
        'https://api.theliftsystem.com/v2/search?client_id=15&auth_token=27q4axyry2fh8phrVmnM&city_id=3133&geocode=&min_bed=-1&max_bed=3&min_bath=1&max_bath=3&min_rate=0&max_rate=2500&region=&keyword=false&property_types=&amenities=&order=min_rate+ASC%2C+max_rate+ASC%2C+min_bed+ASC%2C+max_bed+ASC&limit=20&offset=0&count=false']

    def parse(self, response):
        jsonresponse = json.loads(response.text)
        for i in range(len(jsonresponse)):
            yield scrapy.Request(url=jsonresponse[i]['permalink'],
                                 callback=self.get_property_details,
                                 cb_kwargs={'jsonresponse': jsonresponse, 'index': i},
                                 dont_filter=True)


    def get_property_details(self, response, jsonresponse, index):
        
        amenities = response.css('li.-amenity::text').getall()
        amenities = " ".join(amenities).lower()
        balcony = "balcon" in amenities
        parking = "parking" in amenities
        elevator = "elevator" in amenities
        images = response.css("div.img img::attr(src)").getall()

        item_loader = ListingLoader(response=response)
        item_loader.add_value('external_link', jsonresponse[index]['permalink'])
        item_loader.add_value('title', 'For Lease')
        item_loader.add_value('external_id', str(jsonresponse[index]['id']))
        item_loader.add_value('property_type', 'apartment')
        desc = jsonresponse[index]['details']['overview']
        item_loader.add_value('description', desc)
        address = jsonresponse[index]['address']
        item_loader.add_value('address', address['address'])
        item_loader.add_value('city', address['city'])
        item_loader.add_value('zipcode', address['postal_code'])
        contact = jsonresponse[index]['contact']
        item_loader.add_value('landlord_name', contact['name'])
        item_loader.add_value('landlord_phone', contact['phone'])
        item_loader.add_value('landlord_email', contact['email'])
        item_loader.add_value('currency', 'CAD')
        item_loader.add_value('external_source', self.external_source)
        info=jsonresponse[index]['statistics']['suites']
        room=info['bedrooms']['average']
        item_loader.add_value('room_count', ceil(float(room)))
        bath=info['bathrooms']['average']
        item_loader.add_value('bathroom_count', ceil(int(bath)))
        sq=info['square_feet']['average']
        item_loader.add_value('square_meters', int(int(sq_feet_to_meters(sq))*10.764))
        rent=ceil(info['rates']['average'])
        item_loader.add_value('rent_string',str(rent))
        geo=jsonresponse[index]['geocode']
        item_loader.add_value('latitude',geo['latitude'])
        item_loader.add_value('longitude', geo['longitude'])
        item_loader.add_value('images',images)
        item_loader.add_value('balcony',balcony)
        item_loader.add_value('parking',parking)
        item_loader.add_value('elevator',elevator)
        yield item_loader.load_item()
        if index==len(jsonresponse)-1:
            raise CloseSpider("Testing force close")

