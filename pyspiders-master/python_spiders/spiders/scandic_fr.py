# -*- coding: utf-8 -*-
# Author: Mehmet Kurtipek

from scrapy.loader.processors import MapCompose
from scrapy import Spider
from scrapy import Request,FormRequest
from scrapy.selector import Selector
from w3lib.html import remove_tags
from python_spiders.loaders import ListingLoader
import json

class MySpider(Spider):
    name = 'scandic_fr'
    execution_type='testing'
    country='france'
    locale='fr'

    def start_requests(self):
        start_url = "https://api.scandic.fr/api/properties/filter?"
        header = {"referer": "https://scandic.fr/location/recherche"}
        yield Request(start_url, callback=self.parse, headers=header)

    # 1. FOLLOWING
    def parse(self, response):
        data = json.loads(response.body)
        for item in data:
            prop_type = "apartment"
            if item["size"]["label_en"] == "Studio":
                prop_type = "studio"
            else:
                prop_type = prop_type
            desc = item["description_fr"]
            external_id = item["reference"]
            rent = item["price"]
            room = item["size"]["label_fr"].split("chambre")[0]
            furnished = str(item["furnished"])
            address = item["city"]["name"]
            zipcode = item["city"]["zipcode"]
            title = item["title"]
            image = []
            images = item["pictures"]
            for i in  images:
                img = i["url"]
                image.append(img)  

            follow_url = f"https://scandic.fr/bien/{item['slug']}"
            yield Request(follow_url, callback=self.populate_item, meta={'property_type': prop_type, "desc":desc,"external_id":external_id,"rent":rent,"room":room,"furnished":furnished,"address":address,"zipcode":zipcode,"title":title,"images":image})
        
    
    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        furnished = response.meta["furnished"]

        item_loader.add_value("external_source", "Scandic_PySpider_"+ self.country)
        item_loader.add_value("property_type", response.meta["property_type"])
        item_loader.add_value("description", response.meta["desc"])
        item_loader.add_value("external_link", response.url.replace("/en/property/", "/bien/"))
        item_loader.add_value("external_id", response.meta["external_id"])
        item_loader.add_value("rent", response.meta["rent"])
        item_loader.add_value("currency", "EUR")
        item_loader.add_value("room_count", response.meta["room"])
        item_loader.add_value("city", response.meta["address"])
        item_loader.add_value("address", response.meta["title"])
        item_loader.add_value("zipcode", response.meta["zipcode"])
        item_loader.add_value("title", response.meta["title"])
        item_loader.add_value("images", response.meta["images"])

        meters = "".join(response.xpath("//div[@class='infos']/ul/li[contains(.,'Area') or contains(.,'Surface')]/text()").extract())
        if meters:
            s_meters = meters.split(":")[1].strip()
            item_loader.add_value("square_meters", s_meters)

        floor = "".join(response.xpath("normalize-space(//div[@class='infos']/ul/li[contains(.,'Floor')]/text())").extract())
        if floor:
            floor = floor.split(":")[1].replace("m", "").strip()
            item_loader.add_value("floor", floor)

        if furnished == "1":
            item_loader.add_value("furnished", True)
        elif furnished== "0":
            item_loader.add_value("furnished", False)

        item_loader.add_value("landlord_phone", "+33 1 45 79 68 52")
        item_loader.add_value("landlord_name", "SCANDIC Immobilier")
      
        yield item_loader.load_item()  



