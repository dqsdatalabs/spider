# -*- coding: utf-8 -*-
# Author: Mehmet Kurtipek

from typing import NewType
from parsel.utils import extract_regex
from scrapy.loader.processors import MapCompose
from scrapy import Spider
from scrapy import Request,FormRequest
from scrapy.selector import Selector
from w3lib.html import remove_tags
from python_spiders.loaders import ListingLoader
import json
import dateparser
import re

class MySpider(Spider):
    name = 'bushby_com_au'
    execution_type='testing'
    country='australia'
    locale='en'
    external_source='Bushby_PySpider_australia'
    custom_settings = {
    "HTTPCACHE_ENABLED": False
    }
    def start_requests(self):
        start_urls = [
            {"url": "https://www.bushby.com.au/listings/?post_type=listings&count=20&orderby=meta_value&meta_key=dateListed&sold=0&saleOrRental=Rental&order=dateListed-desc&paged=1"}
        ]  # LEVEL 1       
        for url in start_urls:
            yield Request(url=url.get('url'),callback=self.parse,)
    # 1. FOLLOWING
    def parse(self, response):
        page = response.meta.get("page", 2)
        seen = False
        print(response.status)
        print(response.url)
        with open("xxx_response_body","w",encoding='utf-8') as file:
            file.write(str(response.body))

        script_text = response.xpath("//script[contains(text(),'MapDataStore')]/text()").get()
        if script_text:
            with open("xxx_script_text2","w",encoding='utf-8') as file:
                script_text2 = "[" + script_text.split("[")[-1].split("]")[0] + "]"
                file.write(script_text2)
                json_data = json.loads(script_text2)
                print(type(json_data))
            seen=True

        for item in json_data:
            follow_url = item["url"].replace("\\","")
            yield Request(follow_url,callback=self.populate_item,meta={"item":item})

        if  seen:
            nextpage=f"https://www.bushby.com.au/listings/?post_type=listings&count=20&orderby=meta_value&meta_key=dateListed&sold=0&saleOrRental=Rental&order=dateListed-desc&paged={page}"
            if nextpage:
                yield Request(nextpage, callback=self.parse,meta={'page':page+1})
            
    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)
        item = response.meta.get("item")
        item_loader.add_value("external_link", response.url)
        item_loader.add_value("external_source", self.external_source)

        item_loader.add_value("external_id",item["id"])
        item_loader.add_value("latitude",item["Lat"])
        item_loader.add_value("longitude",item["Long"])
        item_loader.add_value("address",item["address"])
        if "p.a" in item["price"]:
            return
        price = int(re.search("\d+",item["price"].split(".")[0].replace("$",""))[0])
        item_loader.add_value("images",item["image"].replace("\\",""))
        
        item_loader.add_value("rent",price*4)


        room_count = response.xpath("//p[@class='listing-attr icon-bed']/text()").get()
        if room_count:
            item_loader.add_value("room_count",room_count.strip())

        bathroom_count = response.xpath("//p[@class='listing-attr icon-bath']/text()").get()
        if bathroom_count:
            item_loader.add_value("bathroom_count",bathroom_count.strip())

        images = response.xpath("//div[@class='listing-media-slide']//img/@src").getall()
        if images:
            item_loader.add_value("images",images)
        
        desc = " ".join(response.xpath("//div[@class='b-description__text']/p//text()").getall())
        if desc:
            item_loader.add_value("description",desc)

        phone = response.xpath("//span[@class='phone-number brand-fg ']/@data-phonenumber").get()
        if phone:
            item_loader.add_value("landlord_phone",phone)

        if "apartment" in desc.lower():
            item_loader.add_value("property_type","apartment")
        else :
            item_loader.add_value("property_type","house")

        mail = response.xpath("//strong[text()='email']/following-sibling::text()").get()
        if mail:
            item_loader.add_value("landlord_email",mail)

        landlord_name = response.xpath("//h5[@class='staff-card-title']/a/text()").get()
        if landlord_name:
            item_loader.add_value("landlord_name",landlord_name)

        garage = response.xpath("//p[@class='listing-attr icon-car']")
        if garage:
            item_loader.add_value("parking",True)

        if "furnished" in str(response.body).lower():
            item_loader.add_value("furnished",True)

        item_loader.add_value("currency","USD")
        item_loader.add_value("city","Tasmania")

        title = response.xpath("//p[@class='single-listing-address ']/text()").get()
        if title:
            item_loader.add_value("title",title)






        yield item_loader.load_item()


def energy_label_calculate(energy_number):
    energy_number = int(energy_number)
    energy_label = ""
    if energy_number <= 50:
        energy_label = "A"
    elif energy_number > 50 and energy_number <= 90:
        energy_label = "B"
    elif energy_number > 90 and energy_number <= 150:
        energy_label = "C"
    elif energy_number > 150 and energy_number <= 230:
        energy_label = "D"
    elif energy_number > 230 and energy_number <= 330:
        energy_label = "E"
    elif energy_number > 330 and energy_number <= 450:
        energy_label = "F"
    elif energy_number > 450:
        energy_label = "G"
    return energy_label