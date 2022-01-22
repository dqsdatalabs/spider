# -*- coding: utf-8 -*-
# Author: Mehmet Kurtipek

import re
from scrapy.loader.processors import MapCompose
from scrapy import Spider
from scrapy import Request,FormRequest
from scrapy.selector import Selector
from w3lib.html import remove_tags
from python_spiders.loaders import ListingLoader
import json

class MySpider(Spider):
    name = 'studiofori_com'
    external_source = "Studiofori_PySpider_italy"
    start_urls = ['https://www.studiofori.com/wp-admin/admin-ajax.php?keyword=&status%5B%5D=affitto&location%5B%5D=&bedrooms=&bathrooms=&min-area=&max-area=&property_id=&min-price=50&max-price=25000&search_args=YTo2OntzOjk6InBvc3RfdHlwZSI7czo4OiJwcm9wZXJ0eSI7czoxNDoicG9zdHNfcGVyX3BhZ2UiO3M6MjoiMTAiO3M6NToicGFnZWQiO3M6MToiMiI7czoxMToicG9zdF9zdGF0dXMiO3M6NzoicHVibGlzaCI7czo5OiJ0YXhfcXVlcnkiO2E6Mjp7aTowO2E6Mzp7czo4OiJ0YXhvbm9teSI7czoxNToicHJvcGVydHlfc3RhdHVzIjtzOjU6ImZpZWxkIjtzOjQ6InNsdWciO3M6NToidGVybXMiO2E6MTp7aTowO3M6NzoiYWZmaXR0byI7fX1zOjg6InJlbGF0aW9uIjtzOjM6IkFORCI7fXM6MTA6Im1ldGFfcXVlcnkiO2E6Mzp7czo4OiJyZWxhdGlvbiI7czozOiJBTkQiO2k6MDtzOjA6IiI7aToxO2E6Mjp7czo4OiJyZWxhdGlvbiI7czozOiJBTkQiO2k6MDthOjE6e2k6MDthOjQ6e3M6Mzoia2V5IjtzOjE5OiJmYXZlX3Byb3BlcnR5X3ByaWNlIjtzOjU6InZhbHVlIjthOjI6e2k6MDtkOjUwO2k6MTtkOjI1MDAwO31zOjQ6InR5cGUiO3M6NzoiTlVNRVJJQyI7czo3OiJjb21wYXJlIjtzOjc6IkJFVFdFRU4iO319fX19&search_URI=status%255B%255D%3Daffitto%26min-price%3D50%26max-price%3D25000&search_geolocation=&houzez_save_search_ajax=40380b46e3&action=houzez_half_map_listings&paged=1&sortby=&item_layout=v1']
    # 1. FOLLOWING
    def parse(self, response):
        data = json.loads(response.body)
        if "properties" in data:
            page = response.meta.get('page', 2)
            
            seen = False
            for item in data["properties"]:
                if get_p_type_string(item["property_type"]):
                    yield Request(item["url"], callback=self.populate_item, meta={"property_type": get_p_type_string(item['property_type'])})
                seen = True
            
            if page == 2 or seen:
                url = f"https://www.studiofori.com/wp-admin/admin-ajax.php?keyword=&status%5B%5D=affitto&location%5B%5D=&bedrooms=&bathrooms=&min-area=&max-area=&property_id=&min-price=50&max-price=25000&search_args=YTo2OntzOjk6InBvc3RfdHlwZSI7czo4OiJwcm9wZXJ0eSI7czoxNDoicG9zdHNfcGVyX3BhZ2UiO3M6MjoiMTAiO3M6NToicGFnZWQiO3M6MToiMiI7czoxMToicG9zdF9zdGF0dXMiO3M6NzoicHVibGlzaCI7czo5OiJ0YXhfcXVlcnkiO2E6Mjp7aTowO2E6Mzp7czo4OiJ0YXhvbm9teSI7czoxNToicHJvcGVydHlfc3RhdHVzIjtzOjU6ImZpZWxkIjtzOjQ6InNsdWciO3M6NToidGVybXMiO2E6MTp7aTowO3M6NzoiYWZmaXR0byI7fX1zOjg6InJlbGF0aW9uIjtzOjM6IkFORCI7fXM6MTA6Im1ldGFfcXVlcnkiO2E6Mzp7czo4OiJyZWxhdGlvbiI7czozOiJBTkQiO2k6MDtzOjA6IiI7aToxO2E6Mjp7czo4OiJyZWxhdGlvbiI7czozOiJBTkQiO2k6MDthOjE6e2k6MDthOjQ6e3M6Mzoia2V5IjtzOjE5OiJmYXZlX3Byb3BlcnR5X3ByaWNlIjtzOjU6InZhbHVlIjthOjI6e2k6MDtkOjUwO2k6MTtkOjI1MDAwO31zOjQ6InR5cGUiO3M6NzoiTlVNRVJJQyI7czo3OiJjb21wYXJlIjtzOjc6IkJFVFdFRU4iO319fX19&search_URI=status%255B%255D%3Daffitto%26min-price%3D50%26max-price%3D25000&search_geolocation=&houzez_save_search_ajax=40380b46e3&action=houzez_half_map_listings&paged={page}&sortby=&item_layout=v1"
                yield Request(url, callback=self.parse, meta={"page": page+1})

    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        item_loader.add_value("external_link", response.url)
        item_loader.add_value("property_type", response.meta.get('property_type'))
        item_loader.add_value("external_source", self.external_source)
        
        title = response.xpath("//h1/text()").get()
        item_loader.add_value("title", title)

        address = response.xpath("//address/text()").get()
        if address:
            item_loader.add_value("address", address)            
            if address.split(",")[-2].strip().isdigit():
                item_loader.add_value("zipcode", address.split(",")[-2].strip())
        
        city = response.xpath("//li[strong[contains(.,'Città')]]/span/text()").get()
        item_loader.add_value("city", city)
        
        rent = response.xpath("//li[@class='item-price']/text()").get()
        if rent:
            rent = rent.split("/")[0].split("€")[1].replace(",","")
            item_loader.add_value("rent", rent)
        item_loader.add_value("currency", "EUR")
        
        room_count = response.xpath("//i[contains(@class,'bed-')]/following-sibling::strong/text()").get()
        item_loader.add_value("room_count", room_count)
        
        bathroom_count = response.xpath("//i[contains(@class,'shower-')]/following-sibling::strong/text()").get()
        item_loader.add_value("bathroom_count", bathroom_count)

        square_meters = response.xpath("//i[contains(@class,'dimensions-')]/following-sibling::strong/text()").get()
        item_loader.add_value("square_meters", square_meters)
        
        description = "".join(response.xpath("//div[@class='block-content-wrap']//p//text()").getall())
        if description:
            item_loader.add_value("description", re.sub('\s{2,}', ' ', description.strip()))
        
        
        external_id = response.xpath("//li[strong[contains(.,'ID')]]/span/text()").get()
        item_loader.add_value("external_id", external_id)
        
        energy_label = response.xpath("//li[strong[contains(.,'energetica')]]/span/text()").get()
        item_loader.add_value("energy_label", energy_label)
        
        images = [x for x in response.xpath("//img[@class='img-fluid']//@src").getall()]
        if images:
            item_loader.add_value("images", images)
        
        landlord_name = response.xpath("//li[@class='agent-name']/text()").get()
        if landlord_name and landlord_name.strip():
            item_loader.add_value("landlord_name", landlord_name)
        else:
            item_loader.add_value("landlord_name", "Studio Fori")

        elevator = response.xpath("//i[contains(@class,'circle')]/following-sibling::a/text()[contains(.,'ascensore')]").get()
        if elevator:
            item_loader.add_value("elevator", True)
        
        terrace = response.xpath("//i[contains(@class,'circle')]/following-sibling::a/text()[contains(.,'terrazzo')]").get()
        if elevator:
            item_loader.add_value("terrace", True)
        
        balcony = response.xpath("//i[contains(@class,'circle')]/following-sibling::a/text()[contains(.,'balcone')]").get()
        if elevator:
            item_loader.add_value("balcony", True)
        
        item_loader.add_value("landlord_phone", "0698184453")
        item_loader.add_value("landlord_email", "info@studiofori.com")
        
        
        
        # yield item_loader.load_item()
        
def get_p_type_string(p_type_string):
    if p_type_string and ("appartament" in p_type_string.lower() or "apartment" in p_type_string.lower()):
        return "apartment"
    elif p_type_string and ("trilocale" in p_type_string.lower() or "house" in p_type_string.lower() or "villetta" in p_type_string.lower() or "villino" in p_type_string.lower() or "villa" in p_type_string.lower() or "attico" in p_type_string.lower()):
        return "house"
    elif p_type_string and "monolocale" in p_type_string.lower():
        return "studio"
    elif p_type_string and "bedroom" in p_type_string.lower():
        return "apartment"
    else:
        return None