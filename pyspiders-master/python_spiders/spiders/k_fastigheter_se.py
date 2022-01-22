# -*- coding: utf-8 -*-
# Author: Mehmet Kurtipek

from scrapy.http.headers import Headers
from scrapy.loader.processors import MapCompose
from scrapy import Spider
from scrapy import Request,FormRequest
from scrapy.selector import Selector
from w3lib.html import remove_tags
from python_spiders.loaders import ListingLoader
import json

class MySpider(Spider):
    name = 'k_fastigheter_se'
    external_source = "Kfastigheter_PySpider_sweden"
    execution_type = 'testing'
    country = 'sweden' 
    locale ='sv'

    def start_requests(self):
        start_urls = [
            {
                "url": [
                    "https://wordpress.k-fastigheter.se/wp-json/core/cpt?type=apartment&lang=sv",
                ],
                "property_type": "apartment"
            },
	       
        ]  # LEVEL 1
        
        for url in start_urls:
            for item in url.get('url'):
                yield Request(
                    url=item,
                    callback=self.parse,
                    # headers=self.headers,
                    meta={'property_type': url.get('property_type')}
                )

    # 1. FOLLOWING
    def parse(self, response):
        data = json.loads(response.body)
        for item in data:
            url = "https://k-fastigheter.se/ledigt/" + item["post_name"]
            
            yield Request(url, callback=self.populate_item, meta={"property_type": response.meta.get('property_type'),'item':item})

    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)
        item=response.meta.get('item')

        item_loader.add_value("external_link", response.url)
        item_loader.add_value("property_type", response.meta.get('property_type'))
        item_loader.add_value("external_source", self.external_source)

        item_loader.add_value("title", item["post_title"])
        try:
            item_loader.add_value("description",item["custom_fields"]["estate_parent"][0]["post_content"])
        except:
            pass

        address=item["post_title"]
        if address:
            address=address.split(",")[0]
            item_loader.add_value("address",address)


        data = ""
        try:
            data = item["custom_fields"]["estate_parent"][0]["custom_fields"]["single_data"]
        except:
            data = item ["custom_fields"]["single_data"]
        if data:

            item_loader.add_value("room_count", data["apartments"][0]["room"])
            item_loader.add_value("currency", "SEK")
            item_loader.add_value("latitude", data["map"]["lat"])
            item_loader.add_value("longitude", data["map"]["lng"])
            item_loader.add_value("images", data["gallery"][0]["link"])
            try:
                city=item ["custom_fields"]["single_data"]["map"]["country"]
                if city:
                    item_loader.add_value("city",city)
                else:
                    city=item["custom_fields"]["estate_parent"][0]["custom_fields"]["single_data"]["map"]["country"]
                    if city:
                        item_loader.add_value("city",city)
            except:
                pass

            rent=item["custom_fields"]["single_data"]["apartments"][0]["price"]
            if rent:
                item_loader.add_value("rent",rent.replace(" ",""))
            else:
                try:
                    price=item["custom_fields"]["estate_parent"][0]["custom_fields"]["single_data"]["apartments"][0]["price"]
                    if price:
                        item_loader.add_value("rent",price.replace(" ",""))
                except:
                    pass
            try:
                square_meters=item["custom_fields"]["single_data"]["apartments"][0]["size"]
                if square_meters:
                    item_loader.add_value("square_meters",square_meters)
                else:
                    square=item["custom_fields"]["estate_parent"][0]["custom_fields"]["single_data"]["apartments"][0]["size"]
                    if square:
                        item_loader.add_value("square_meters",square)
            except:
                pass
            try:
                parking=data["parking"]
                if 'hidden' in parking: 
                    item_loader.add_value("parking", True)

                parking=data["parking"]
                if 'hidden' in parking: 
                    item_loader.add_value("parking", True)
                else:
                    item_loader.add_value("parking", False)
            except:
                pass
            try:
                balcony=data["balcony"]
                if 'hidden' in balcony: 
                    item_loader.add_value("balcony", True)
                else:
                    item_loader.add_value("balcony", False)
            except:
                pass
            try:
                elevator=data["elevator"]
                if 'hidden' in elevator: 
                    item_loader.add_value("elevator", True)
                else:
                    item_loader.add_value("elevator", False)
            except:
                pass

        item_loader.add_value("landlord_phone", "010-330 00 69")
        item_loader.add_value("landlord_email", "info@k-fastigheter.se")
        item_loader.add_value("landlord_name", "K-Fast Holding AB (publ)")
        yield item_loader.load_item()

