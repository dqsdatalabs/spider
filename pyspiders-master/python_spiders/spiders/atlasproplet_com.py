# -*- coding: utf-8 -*-
# Author: Mehmet Kurtipek

from scrapy.loader.processors import MapCompose
from scrapy import Spider
from scrapy import Request,FormRequest
from scrapy.selector import Selector
from w3lib.html import remove_tags
from python_spiders.loaders import ListingLoader
import json
import re
from word2number import w2n
import dateparser

class MySpider(Spider):
    name = 'atlasproplet_com'    
    execution_type='testing'
    country='united_kingdom'
    locale='en'

    def start_requests(self):
        start_urls = [
            {
                "type" : ",,,,2,,,23",
                "property_type" : "house"
            },
            {
                "type" : "28,17,26,3",
                "property_type" : "apartment"
            },
            {
                "type" : ",,,,,,22",
                "property_type" : "room"
            },
        ]
        for url in start_urls:

            formdata = {
                "action": "yd_properties_return",
                "locations": "5,4,10,12,14",
                "radius": "10.00",
                "property_types": url.get("type"),
                "min_price": "0",
                "max_price": "10000",
                "min_beds": "0",
                "max_beds": "10",
                "saved": "0",
            }

            yield FormRequest(
                url="https://atlasproplet.com/wp-admin/admin-ajax.php",
                callback=self.parse,
                formdata=formdata,
                meta={'property_type': url.get('property_type')}
            )

    # 1. FOLLOWING
    def parse(self, response):

        for item in json.loads(response.body):
            if item["available"]:
                follow_url = response.urljoin(item["link"])
                title = item['title']
                rent = item['price']
                description = item['desc']
                bathroom_count = item['bathrooms']
                room_count = item['bedrooms']
                latitude = item['lat']
                longitude = item['lng']
                images = item['gallery']
                external_id = item['id']
                yield Request(follow_url, callback=self.populate_item, meta={'property_type': response.meta.get('property_type'),
                'title': title, 'rent': rent, 'description': description, 'bathroom_count': bathroom_count, 'room_count': room_count, 'latitude': latitude, 'longitude': longitude,
                'images': images, 'external_id': external_id})
            
    
    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        item_loader.add_value("property_type", response.meta.get('property_type'))
        item_loader.add_value("external_link", response.url)

        item_loader.add_value("external_source", "Atlasproplet_PySpider_"+ self.country + "_" + self.locale)

        address = response.xpath("//div[contains(@class,'single-property-content')]/h2/text()").get()
        if address:
            item_loader.add_value("address", address.strip())

        title = response.meta.get('title')
        if title:
            item_loader.add_value("title", title.strip())

        rent = response.meta.get('rent')
        if rent:
            item_loader.add_value("rent", rent.strip().replace(',', ''))
            item_loader.add_value("currency", 'GBP')

        description = " ".join(response.xpath("//div[contains(@class,'d-md-block')]/p/text()").getall()).strip()
        if description:
            item_loader.add_value("description", description.replace('\xa0', ''))

            if 'sq ft' in description.lower():
                square_meters = "".join(filter(str.isnumeric, description.lower().split('sq ft')[0].strip().split(' ')[-1]))
                item_loader.add_value("square_meters", square_meters)
            if 'floor' in description.lower():
                floor = "".join(filter(str.isnumeric, description.lower().split('floor')[0].strip().split(' ')[-1]))
                try:
                    floor = w2n.word_to_num(floor.strip())
                    item_loader.add_value("floor", str(floor))
                except:
                    pass
            if 'parking' in description.lower():
                item_loader.add_value("parking", True)
            if 'balcony' in description.lower():
                item_loader.add_value("balcony", True)
            if 'no lift' in description.lower():
                item_loader.add_value("elevator", False)
            elif ' lift' in description.lower():
                item_loader.add_value("elevator", True)
            if 'dishwasher' in description.lower():
                item_loader.add_value("dishwasher", True)
            if 'washing machine' in description.lower():
                item_loader.add_value("washing_machine", True)
        
        available_date = response.xpath("//p[contains(text(),'Available from')]/span/text()").get()
        if available_date:
            date_parsed = dateparser.parse(available_date.strip(), date_formats=["%d %B %Y"], languages=['en'])
            if date_parsed:
                date2 = date_parsed.strftime("%Y-%m-%d")
                item_loader.add_value("available_date", date2)
        
        floor_plan_images = [x for x in response.xpath("//div[@data-slide='4' and contains(@class, 'content-center')]//img/@src").getall()]
        if floor_plan_images:
            item_loader.add_value("floor_plan_images", floor_plan_images)
        
        item_loader.add_value("landlord_name", 'Atlas Property Lettings')
        item_loader.add_value("landlord_phone", '020 7139 1718')

        bathroom_count = response.meta.get('bathroom_count')
        if bathroom_count:
            item_loader.add_value("bathroom_count", bathroom_count.strip())

        room_count = response.meta.get('room_count')
        if room_count:
            item_loader.add_value("room_count", room_count.strip())

        latitude = response.meta.get('latitude')
        if latitude:
            item_loader.add_value("latitude", latitude.strip())

        longitude = response.meta.get('longitude')
        if longitude:
            item_loader.add_value("longitude", longitude.strip())

        images = response.meta.get('images')
        if images:
            item_loader.add_value("images", images)
            item_loader.add_value("external_images_count", len(images))

        external_id = response.meta.get('external_id')
        if external_id:
            item_loader.add_value("external_id", str(external_id))

        yield item_loader.load_item()
