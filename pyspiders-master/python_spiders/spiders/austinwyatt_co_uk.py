# -*- coding: utf-8 -*-
# Author: Mehmet Kurtipek

import scrapy
from scrapy.http.headers import Headers
from ..loaders import ListingLoader
from ..helper import extract_number_only, format_date, remove_white_spaces, remove_unicode_char
import re
from scrapy import Selector
from datetime import datetime
import dateparser
import json
from scrapy import Request,FormRequest

class AustinWyattCoUk(scrapy.Spider):
    name = "austinwyatt_co_uk"
    allowed_domains = ["austinwyatt.co.uk"]
    execution_type = 'testing'
    country = 'united_kingdom'
    locale ='en'
    thousand_separator=','
    scale_separator='.'
    position = 0
    external_source="AustinWyatt_PySpider_united_kingdom_en"

    
    def start_requests(self):
        url = "https://www.austinwyatt.co.uk/search.ljson?channel=lettings&fragment=most-expensive-first/status-all/page-1"
        yield Request(url, callback=self.parse)   

    # 1. FOLLOWING
    def parse(self, response):
        page = response.meta.get('page', 2)
        seen = False
        data=json.loads(response.body)['properties']
        if data:
            for url in data:
                follow_url=url['url']
                yield Request(response.urljoin(follow_url), callback=self.get_property_details, meta={"item": url})
                seen = True
            if page == 2 or seen:
                url = f"https://www.austinwyatt.co.uk/search.ljson?channel=lettings&fragment=most-expensive-first/status-all/page-{page}"
                yield Request(url, callback=self.parse, meta={"page": page+1})

    def get_property_details(self, response):
        item_loader = ListingLoader(response=response)
        item=response.meta.get('item')
        item_loader.add_value("external_source", self.external_source)
        item_loader.add_value('external_link', response.url)
        title = response.xpath('//head/title/text()').extract_first()
        if title:
            item_loader.add_value('title',title)
                
        property_type=property_type=item['type']
        if get_p_type_string(property_type):
            item_loader.add_value("property_type", get_p_type_string(property_type))
        address = item['displayAddress']
        if address:
            item_loader.add_value("address",address)
        description =item['shortDescription']
        if description:
            item_loader.add_value("description",description)
        rent=str(item['rent_pcm'])
        if rent:
            item_loader.add_value("rent",rent)
        item_loader.add_value("currency","EUR")
        room=str(item['bedrooms'])
        if room:
            item_loader.add_value("room_count",room)
        bathroom=str(item['bathrooms'])
        if bathroom:
            item_loader.add_value("bathroom_count",bathroom)
        external_id=str(item['property_id'])
        if external_id:
            item_loader.add_value("external_id",external_id)
        lat=item['lat']
        if lat:
            item_loader.add_value("latitude",lat)
        lng=item['lng']
        if lng:
            item_loader.add_value("longitude",lng)
        item_loader.add_value("landlord_name",item['agency_name'])
        images=item['photo']
        if images:
            item_loader.add_value("images",images)
        
        yield item_loader.load_item()

def get_p_type_string(p_type_string):
    if p_type_string and "student" in p_type_string.lower():
        return "student_apartment"
    elif p_type_string and "studio" in p_type_string.lower():
        return "studio"
    elif p_type_string and ("apartment" in p_type_string.lower() or "flat" in p_type_string.lower() or "maisonette" in p_type_string.lower()):
        return "apartment"
    elif p_type_string and ("house" in p_type_string.lower() or "bungalow" in p_type_string.lower() or "huis" in p_type_string.lower() or "home" in p_type_string.lower() or "cottage" in p_type_string.lower()):
        return "house"
    else:
        return None