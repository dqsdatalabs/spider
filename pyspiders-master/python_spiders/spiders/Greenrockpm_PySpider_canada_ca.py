from io import StringIO

import requests
import scrapy
from lxml import etree

from ..loaders import ListingLoader
import json

counter = 2
prob = ''
pos = 1


class Greenrockpm(scrapy.Spider):
    name = 'greenrockpm_ca'
    allowed_domains = ['www.greenrockpm.ca']
    execution_type = 'testing'
    country = 'canada'
    locale = 'en'
    external_source = f"{name.capitalize()}_PySpider_{country}_{locale}"

    def start_requests(self):

        start_urls = [
            'https://api.theliftsystem.com/v2/search?locale=en&client_id=14&auth_token=sswpREkUtyeYjeoahA2i&city_id=3133&geocode=&min_bed=-1&max_bed=100&min_bath=0&max_bath=10&min_rate=0&max_rate=4000&min_sqft=0&max_sqft=10000&show_custom_fields=true&show_promotions=true&region=&keyword=false&property_types=apartments%2C+houses&ownership_types=&exclude_ownership_types=&custom_field_key=&custom_field_values=&order=min_rate+ASC&limit=66&neighbourhood=&amenities=&promotions=&city_ids=&pet_friendly=&offset=0&count=false'

        ]
        for url in start_urls:
            yield scrapy.Request(url=url, callback=self.parse)

    def parse(self, response, **kwargs):
        global pos
        resp = json.loads(response.body)
        for x in range(len(resp)):
            item = resp[x]
            item_loader = ListingLoader(response=response)
            req2 = requests.get(f'https://booking.theliftsystem.com/liftapi-units/{item.get("id")}')
            request2 = req2.json()
            item_loader.add_value('title', item.get('name'))
            item_loader.add_value('external_source', self.external_source)
            link = item.get('permalink').replace('greenrockpm', 'greenrockrs').replace('apartments', 'communities')
            parser = etree.HTMLParser()
            page = requests.get(link)
            html = page.content.decode("utf-8")
            tree = etree.parse(StringIO(html), parser=parser)
            item_loader.add_value('city', item.get('address').get('city'))
            item_loader.add_value('address', item.get('address').get('address'))
            over = item.get('details').get('overview')
            over = over[0:over.find("Contact"):] + over[over.find("home.") + 4::]
            over = over[0:over.find("Contact"):] + over[over.find(".ca.") + 4::]
            item_loader.add_value('description', over)
            item_loader.add_value('zipcode', item.get('address').get('postal_code'))
            item_loader.add_value('landlord_name', item.get('contact').get('name'))
            item_loader.add_value('landlord_phone', item.get('contact').get('phone'))
            item_loader.add_value('landlord_email', "info@davisville-village.ca")
            item_loader.add_value('latitude', item.get('geocode').get('latitude'))
            item_loader.add_value('longitude', item.get('geocode').get('longitude'))
            images=tree.xpath('/html/body/section[2]/section/section/div/a/@href')
            item_loader.add_value('images', images)
            amen=tree.xpath('/html/body/section[5]/div/div[1]/div/div[2]/div/text()')
            for i in amen :
                if "Laundry" in i :
                    item_loader.add_value('washing_machine', True)
                if "Elevator" in i :
                    item_loader.add_value('elevator', True)
                if "Terrace" in i:
                    item_loader.add_value('terrace', True)
                if "Pool" in i:
                    item_loader.add_value('swimming_pool', True)
                if "Parking" in i:
                    item_loader.replace_value('parking', True)
                if "Balcony" in i:
                    item_loader.replace_value('balcony', True)
            if item.get("pet_friendly") :
                item_loader.add_value('pets_allowed', True)
            item_loader.add_value('images', item.get("photo_path"))
            if item.get("parking").get("indoor") or item.get("parking").get("outdoor")or item.get("parking").get("additional") :
                item_loader.replace_value('parking', True)
            for i,y in enumerate (request2):
                item_loader.replace_value('external_link', link+f"#{i}")
                item_loader.add_value('external_id', str(y.get('building_id')))
                rates = y.get("rate")
                item_loader.replace_value('rent', int(rates))
                item_loader.add_value('currency', "CAD")
                sq_ft = round(float(y.get("sq_ft")) / 10.764)
                if int(y.get('bed')):
                    item_loader.replace_value('room_count', int(y.get('bed')))
                else:
                    item_loader.replace_value('room_count', 1)
                item_loader.replace_value('bathroom_count', int(y.get('bath')))
                item_loader.replace_value('square_meters', int(int(sq_ft)*10.764))
                if "Studio" in y.get('type_name'):
                    item_loader.replace_value('property_type', "studio")
                if "Bedroom" in y.get('type_name'):
                    item_loader.replace_value('property_type', "apartment")
                if "house" in y.get('type_name') or "TH" in y.get('type_name'):
                    item_loader.replace_value('property_type', "house")
                item_loader.add_value('position', pos)
                pos += 1
                yield item_loader.load_item()
