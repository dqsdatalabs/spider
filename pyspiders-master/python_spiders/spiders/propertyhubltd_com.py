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

class MySpider(Spider):
    name = 'propertyhubltd_com'
    execution_type='testing'
    country='united_kingdom'
    locale='en'
    def start_requests(self):
        start_urls = [
            {
                "url" : [
                    "https://www.propertyhubltd.com/search/{}.html?instruction_type=Letting&department=Residential&showstc=on&showsold=on&address_keyword=&minprice=&maxprice=&property_type=Apartment",
                    "https://www.propertyhubltd.com/search/{}.html?instruction_type=Letting&department=Residential&showstc=on&showsold=on&address_keyword=&minprice=&maxprice=&property_type=Flat",
                    "https://www.propertyhubltd.com/search/{}.html?instruction_type=Letting&department=Residential&showstc=on&showsold=on&address_keyword=&minprice=&maxprice=&property_type=Ground+Floor+Flat",
                    "https://www.propertyhubltd.com/search/{}.html?instruction_type=Letting&department=Residential&showstc=on&showsold=on&address_keyword=&minprice=&maxprice=&property_type=Ground+Floor+Maisonette",
                    "https://www.propertyhubltd.com/search/{}.html?instruction_type=Letting&department=Residential&showstc=on&showsold=on&address_keyword=&minprice=&maxprice=&property_type=Maisonette",
                    
                ],
                "property_type" : "apartment",
            },
            {
                "url" : [
                    "https://www.propertyhubltd.com/search/{}.html?instruction_type=Letting&department=Residential&showstc=on&showsold=on&address_keyword=&minprice=&maxprice=&property_type=Studio",
                    
                ],
                "property_type" : "studio"
            },
            {
                "url" : [
                    "https://www.propertyhubltd.com/search/{}.html?instruction_type=Letting&department=Residential&showstc=on&showsold=on&address_keyword=&minprice=&maxprice=&property_type=Detached+House",
                    "https://www.propertyhubltd.com/search/{}.html?instruction_type=Letting&department=Residential&showstc=on&showsold=on&address_keyword=&minprice=&maxprice=&property_type=End+Terraced+House",
                    "https://www.propertyhubltd.com/search/{}.html?instruction_type=Letting&department=Residential&showstc=on&showsold=on&address_keyword=&minprice=&maxprice=&property_type=Mid+Terraced+House",
                    "https://www.propertyhubltd.com/search/{}.html?instruction_type=Letting&department=Residential&showstc=on&showsold=on&address_keyword=&minprice=&maxprice=&property_type=Semi-Detached+House",
                    "https://www.propertyhubltd.com/search/{}.html?instruction_type=Letting&department=Residential&showstc=on&showsold=on&address_keyword=&minprice=&maxprice=&property_type=Penthouse",
                    
                ],
                "property_type" : "house"
            },
            {
                "url" : [
                    "https://www.propertyhubltd.com/search/{}.html?instruction_type=Letting&department=Residential&showstc=on&showsold=on&address_keyword=&minprice=&maxprice=&property_type=Bedsit",
                    
                ],
                "property_type" : "room"
            },
        ]
        for url in start_urls:
            for item in url.get("url"):
                yield Request(item.format(1),
                            callback=self.parse,
                            meta={'property_type': url.get('property_type'), "base_url":item})


    # 1. FOLLOWING
    def parse(self, response):
        page = response.meta.get("page", 2)
        seen = False
        for item in response.xpath("//div[contains(@class,'property')]/div/a"):
            status = "".join(item.xpath(".//text()").getall())
            if status and "agreed" in status.lower():
                continue
            follow_url = response.urljoin(item.xpath("./@href").get())
            yield Request(follow_url, callback=self.populate_item, meta={"property_type":response.meta["property_type"]})
            seen = True
        
        if page == 2 or seen:
            base_url = response.meta["base_url"]
            p_url = base_url.format(page)
            yield Request(
                p_url,
                callback=self.parse,
                meta={"page":page+1, "property_type":response.meta["property_type"], "base_url":base_url})
         
    
    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)
        title = response.xpath("//title/text()").get()
        if title:
            item_loader.add_value("title", re.sub('\s{2,}', ' ', title.strip()))
        
        item_loader.add_value("external_link", response.url.split("?")[0])
        
        from python_spiders.helper import ItemClear
        ItemClear(response=response, item_loader=item_loader, item_name="external_source", input_value="Propertyhubltd_PySpider_united_kingdom", input_type="VALUE")
        ItemClear(response=response, item_loader=item_loader, item_name="address", input_value="//h1/text()", input_type="F_XPATH")
        ItemClear(response=response, item_loader=item_loader, item_name="rent_string", input_value="//h2//text()", input_type="F_XPATH", get_num=True, split_list={"Let":1, "PCM":0}, replace_list={",":""})
        ItemClear(response=response, item_loader=item_loader, item_name="currency", input_value="GBP", input_type="VALUE")
        ItemClear(response=response, item_loader=item_loader, item_name="parking", input_value="//ul[@class='bullets']/li[contains(.,'parking') or contains(.,'Parking')]/text()", input_type="F_XPATH", tf_item=True)
        ItemClear(response=response, item_loader=item_loader, item_name="balcony", input_value="//ul[@class='bullets']/li[contains(.,'balcon') or contains(.,'Balcon')]/text()", input_type="F_XPATH", tf_item=True)
        ItemClear(response=response, item_loader=item_loader, item_name="furnished", input_value="//ul[@class='bullets']/li[contains(.,'Furnished')]/text()", input_type="F_XPATH", tf_item=True)
        ItemClear(response=response, item_loader=item_loader, item_name="elevator", input_value="//ul[@class='bullets']/li[contains(.,'Lift') or contains(.,'lift')]/text()", input_type="F_XPATH", tf_item=True)
        ItemClear(response=response, item_loader=item_loader, item_name="terrace", input_value="//ul[@class='bullets']/li[contains(.,'terrace') or contains(.,'Terrace')]/text()", input_type="F_XPATH", tf_item=True)
        ItemClear(response=response, item_loader=item_loader, item_name="swimming_pool", input_value="//ul[@class='bullets']/li[contains(.,'Swimming')]/text()", input_type="F_XPATH", tf_item=True)
        description = " ".join(response.xpath("//div[contains(@class,'description')]//text()").getall())
        if description:
            item_loader.add_value("description", re.sub('\s{2,}', ' ', description.strip()))
        
        ItemClear(response=response, item_loader=item_loader, item_name="images", input_value="//div[@class='carousel-inner']//img/@src", input_type="M_XPATH")
        ItemClear(response=response, item_loader=item_loader, item_name="latitude", input_value="//script[contains(.,'lng')]/text()", input_type="F_XPATH", split_list={"&q=":1,"%2C":0})
        ItemClear(response=response, item_loader=item_loader, item_name="longitude", input_value="//script[contains(.,'ln')]/text()", input_type="F_XPATH", split_list={"%2C":1, '"':0})
        ItemClear(response=response, item_loader=item_loader, item_name="landlord_name", input_value="PROPERTY HUB", input_type="VALUE")
        ItemClear(response=response, item_loader=item_loader, item_name="landlord_phone", input_value="0208 903 1002", input_type="VALUE")
        ItemClear(response=response, item_loader=item_loader, item_name="floor_plan_images", input_value="//div[@id='floorplan']//img/@src", input_type="M_XPATH")

        
        if response.xpath("//ul[@class='bullets']/li[contains(.,'Studio') or contains(.,'studio')]/text()").get():
            item_loader.add_value("room_count", "1")
            item_loader.add_value("property_type", "studio")
        else:
            ItemClear(response=response, item_loader=item_loader, item_name="room_count", input_value="//li[@class='grey-link']//span/text()[contains(.,'BED')]", input_type="F_XPATH", get_num=True, split_list={" ":0})
            item_loader.add_value("property_type", response.meta.get('property_type'))
        
        address = response.xpath("//h1/text()").get()
        if address:
            zipcode = ""
            if "," in address:
                zipcode = address.split(",")[-1].strip()
            else:
                item_loader.add_value("city", address.strip())
            
            if zipcode:
                if not zipcode.split(" ")[0].isalpha():
                    if zipcode.count(" ") ==1:
                        item_loader.add_value("zipcode", zipcode)
                        item_loader.add_value("city", address.split(",")[-2].strip())
                else:
                    item_loader.add_value("city", zipcode.strip())
            else:
                item_loader.add_value("city", address.strip())
                
        energy_label = "".join(response.xpath("//div[contains(@class,'description')]//text()").getall())
        if energy_label:
            if "epc rating" in energy_label.lower():
                energy_label = energy_label.lower().split("epc rating")[1].replace("\r"," ").replace(":","").replace("-","").strip().split(" ")[0]
                item_loader.add_value("energy_label", energy_label.upper())
            
            if "floor" in energy_label:
                floor = energy_label.split("floor")[0].strip().split(" ")[-1]
                
                not_list = ["lami","carpe","each","wood","sepera", "quali", "of","place"]
                status = True
                for i in not_list:
                    if i in floor.lower():
                        status = False
                if status:
                    item_loader.add_value("floor", floor.replace("-","").upper())
        
        from datetime import datetime
        import dateparser
        available_date = response.xpath("//div[contains(@class,'description')]//text()[contains(.,'Available')]").get()
        if available_date:
            if "available now" in available_date.lower() or "available immediately" in available_date.lower():
                item_loader.add_value("available_date", datetime.now().strftime("%Y-%m-%d"))
            else:
                available_date = available_date.split("Available")[1].replace(" in ","").replace("from","").strip()
                if " " in available_date:
                    date_parsed = dateparser.parse(available_date, date_formats=["%d/%m/%Y"])
                    if date_parsed:
                        date2 = date_parsed.strftime("%Y-%m-%d")
                        item_loader.add_value("available_date", date2)
        
        item_loader.add_value("external_id", response.url.split("property-details/")[1].split("/")[0])
        
        yield Request(
            response.url.split("?")[0]+"/printable-details",
            callback=self.print_details,
            meta={
                "item_loader":item_loader,
            }
        )
    
    def print_details(self, response):
        item_loader = response.meta["item_loader"]

        bathroom_count = response.xpath("//p[@class='rooms']/text()[contains(.,'Bathroom')]").get()
        if bathroom_count:
            bathroom_count = bathroom_count.lower().split("bathroom")[0].split("|")[-1].strip()
            item_loader.add_value("bathroom_count", bathroom_count)
        
        item_loader.add_value("landlord_email", "info@propertyhubltd.com")


        yield item_loader.load_item()