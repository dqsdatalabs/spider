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
    name = 'valierecortez_com'
    execution_type='testing'
    country='france'
    locale='fr'
    def start_requests(self):
        start_url = "https://www.valierecortez.com/wp-admin/admin-ajax.php?cat_slug=a-louer&action=load_more_posts&post_type=bien&cat_tax=type&offset=0"
        yield Request(start_url, callback=self.parse)

    # 1. FOLLOWING
    def parse(self, response):

        page = response.meta.get("page", 6)
        seen = False

        for item in response.xpath("//ul[@class='property-list wrapper']/li/a"):
            seen = True
            follow_url = response.urljoin(item.xpath("./@href").get())
            property_type = item.xpath(".//h2/text()").get()
            if get_p_type_string(property_type):
                yield Request(follow_url, callback=self.populate_item, meta={"property_type":get_p_type_string(property_type)})
        
        if page == 6 or seen:
            yield Request(response.url.replace("&offset=" + str(page - 6), "&offset=" + str(page)), callback=self.parse, meta={'page':page+6})

    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        item_loader.add_value("property_type", response.meta.get('property_type'))
        item_loader.add_value("external_link", response.url)
        
        from python_spiders.helper import ItemClear
        ItemClear(response=response, item_loader=item_loader, item_name="external_source", input_value="Valierecortez_PySpider_france", input_type="VALUE")
        ItemClear(response=response, item_loader=item_loader, item_name="title", input_value="//section//h1//text()", input_type="F_XPATH")
        ItemClear(response=response, item_loader=item_loader, item_name="city", input_value="Paris", input_type="VALUE")
        
        features = response.xpath("//section//h1//text()").get()
        if features:
            address = features.lower().split("m²")[1].strip().replace(",","").replace("-","").strip()
            item_loader.add_value("address", address.capitalize())

            square_meters = features.lower().split("m²")[0].strip().split(" ")[-1].replace(",",".")
            if "-" in square_meters:
                square_meters = square_meters.split("-")[1].strip()
            item_loader.add_value("square_meters", int(float(square_meters)))
            
            if "studio" in features.lower():
                item_loader.add_value("room_count", "1")
            else:
                room_count = features.replace("Pièces", "PIECES").split("PIECES")[0].strip().split(" ")[-1]
                if room_count.isdigit():
                    item_loader.add_value("room_count", room_count)
                elif "deux" in room_count.lower():
                    item_loader.add_value("room_count", "2")
                elif "trois" in room_count.lower():
                    item_loader.add_value("room_count", "3")
                    
        desc = " ".join(response.xpath("//div[@class='text-content']//text()").getall())
        if desc:
            desc = re.sub('\s{2,}', ' ', desc.strip())
            item_loader.add_value("description", desc)
        
        if "salle d" in desc:
            bathroom_count = desc.split("salle d")[0].strip().split(" ")[-1]
            if "une" in bathroom_count:
                item_loader.add_value("bathroom_count", "1")
        
        if "\u00e9tage" in desc:
            floor = desc.split("\u00e9tage")[0].strip().split(" ")[-1].replace("er","").replace("ème","")
            if floor.isdigit():
                item_loader.add_value("floor", floor)
                    
        ItemClear(response=response, item_loader=item_loader, item_name="rent", input_value="//div[@class='text-content']//text()[contains(.,'Loyer Hors charges')]", input_type="F_XPATH", get_num=True, split_list={".":0})
        ItemClear(response=response, item_loader=item_loader, item_name="currency", input_value="EUR", input_type="VALUE")
        ItemClear(response=response, item_loader=item_loader, item_name="utilities", input_value="//div[@class='text-content']//text()[contains(.,'Provisions charges')]", input_type="F_XPATH", get_num=True, split_list={".":0})
        ItemClear(response=response, item_loader=item_loader, item_name="deposit", input_value="//div[@class='text-content']//text()[contains(.,'Dépôt de garantie')]", input_type="F_XPATH", get_num=True, split_list={".":0, ":":-1})
        ItemClear(response=response, item_loader=item_loader, item_name="images", input_value="//ul[@class='slideshow-container']//@src", input_type="M_XPATH")
        ItemClear(response=response, item_loader=item_loader, item_name="landlord_name", input_value="VALIERE CORTEZ", input_type="VALUE")
        ItemClear(response=response, item_loader=item_loader, item_name="landlord_phone", input_value="0140029208", input_type="VALUE")
        ItemClear(response=response, item_loader=item_loader, item_name="landlord_email", input_value="locations@valierecortez.com", input_type="VALUE")

        status = response.xpath("//div[@class='statut']//text()[contains(.,'Loué')]").get()
        if not status:
            yield item_loader.load_item()

def get_p_type_string(p_type_string):
    if p_type_string and "studio" in p_type_string.lower():
        return "studio"
    elif p_type_string and "appartement" in p_type_string.lower():
        return "apartment"
    elif p_type_string and "maison" in p_type_string.lower():
        return "house"
    else:
        return None