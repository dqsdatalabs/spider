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

class MySpider(Spider):
    name = 'paulcarrlettings_co_uk'    
    start_urls = ["https://paulcarrestateagents.co.uk/showprop.php"]
    execution_type='testing'
    country='united_kingdom'
    locale='en' 

    form_data = {
        "location": "",
        "min_price": "0",
        "max_price": "0",
        "property_type": "Flat",
        "bedrooms": "0",
        "radius": "0",
        "type": "LETTINGS",
        "page": "1",
        "exc_rural": "0",
        "land_newhome": "0",
        "avail_prop": "0",
        "search_order": "",
    }
    current_index = 0
    other_prop = ["House", "Bungalow"]
    other_type = "house"
    def start_requests(self):
        yield FormRequest(
            self.start_urls[0],
            formdata=self.form_data,
            dont_filter=True,
            callback=self.parse,
            meta={"property_type": "apartment"}
        )
    
    # 1. FOLLOWING
    def parse(self, response):
        page = response.meta.get('page', 2)
        seen = False
        data = json.loads(response.body)["resultval"]
        for item in data:
            follow_url = f"https://paulcarrestateagents.co.uk/property-detail-lettings?propertyid={item['encid']}"
            yield Request(
                follow_url,
                callback=self.populate_item,
                meta={
                    'property_type': response.meta.get('property_type'),
                    "item": item
                }
            )
            seen = True
            
        if page ==2 or seen:
            self.form_data["page"] = str(page)
            yield FormRequest(
                self.start_urls[0],
                dont_filter=True,
                formdata=self.form_data,
                callback=self.parse,
                meta={
                    'property_type': response.meta.get('property_type'),
                    "page": page+1,
                }
            )
        elif self.current_index<len(self.other_prop):
            self.form_data["property_type"] = self.other_prop[self.current_index]
            yield FormRequest(
                self.start_urls[0],
                dont_filter=True,
                formdata=self.form_data,
                callback=self.parse,
                meta={
                    "property_type": self.other_type,
                }
            )
            self.current_index += 1
    
    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        item_loader.add_value("external_link", response.url)
        item_loader.add_value("property_type", response.meta.get('property_type'))

        title = response.xpath("//title/text()").get()
        if title:
            item_loader.add_value("title", re.sub('\s{2,}', ' ', title.strip()))
        item_loader.add_value("external_source", "Paulcarrlettings_PySpider_"+ self.country + "_" + self.locale)
        dontallow=response.xpath("//p[.='Let Agreed']").get()
        if dontallow:
            return 

        data = response.meta.get('item')
        
        item_loader.add_value("external_id", data['propid'])
        
        zipcode = data['pcode']
        item_loader.add_value("address", data["all_area"])
        item_loader.add_value("city", data["all_area"].split(",")[-1])
        item_loader.add_value("zipcode", zipcode)
        
        rent = data['price']
        item_loader.add_value("rent", rent.replace(",",""))
        item_loader.add_value("currency", "GBP")
        deposit=data['ldesc']
        if deposit:
            item_loader.add_value("deposit",deposit.split("Deposit")[-1].replace("£","").replace(",","").strip())

        
        room_count = data['nobed']
        item_loader.add_value("room_count", room_count)
        
        bathroom_count = data['nobath']
        item_loader.add_value("bathroom_count", bathroom_count)
        
        description = data['ldesc']
        item_loader.add_value("description", re.sub('\s{2,}', ' ', description))
        
        latitude = data['prop_lat']
        longitude = data['prop_long']
        
        item_loader.add_value("latitude", latitude)
        item_loader.add_value("longitude", longitude)
        
        images = [f"https://paulcarrestateagents.co.uk/{x}" for x in response.xpath("//div[contains(@class,'owl-carousel')]//@href").getall()]
        if images:
            item_loader.add_value("images", images)
        
        item_loader.add_value("landlord_name", 'Paul Carr Estate Agents')
        item_loader.add_value("landlord_phone", '01922 454014')
        item_loader.add_value("landlord_email", 'aldridge@paulcarrestateagents.co.uk')

        yield item_loader.load_item()