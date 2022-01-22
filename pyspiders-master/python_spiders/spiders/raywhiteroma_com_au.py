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
from python_spiders.helper import ItemClear
class MySpider(Spider): 
    name = 'raywhiteroma_com_au'
    execution_type='testing'
    country='australia'
    locale='en'
   
    def start_requests(self):
        start_urls = [
            {
                "url" : [
                    "https://raywhiteroma.com.au/properties/residential-for-rent?category=UNT&keywords=&minBaths=0&minBeds=0&minCars=0&rentPrice=&sort=updatedAt+desc&suburbPostCode=",
                ],
                "property_type" : "apartment",
            },
            {
                "url" : [
                    "https://raywhiteroma.com.au/properties/residential-for-rent?category=HSE&keywords=&minBaths=0&minBeds=0&minCars=0&rentPrice=&sort=updatedAt+desc&suburbPostCode=",
                ],
                "property_type" : "house"
            }
        ]
        for url in start_urls:
            for item in url.get("url"):
                yield Request(item,
                            callback=self.parse,
                            meta={'property_type': url.get('property_type')})

    # 1. FOLLOWING
    def parse(self, response):

        for item in response.xpath("//div[@class='proplist_item']/a/@href").getall():
            yield Request(response.urljoin(item), callback=self.populate_item, meta={"property_type":response.meta["property_type"]})

    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        item_loader.add_value("property_type", response.meta.get('property_type'))
        item_loader.add_value("external_link", response.url)
        
        ItemClear(response=response, item_loader=item_loader, item_name="external_source", input_value="Raywhiteroma_Com_PySpider_australia", input_type="VALUE")
        ItemClear(response=response, item_loader=item_loader, item_name="title", input_value="//title/text()", input_type="F_XPATH")
        ItemClear(response=response, item_loader=item_loader, item_name="address", input_value="//h1[contains(@class,'pdp_address')]//text()", input_type="M_XPATH")
        ItemClear(response=response, item_loader=item_loader, item_name="zipcode", input_value="//h1[contains(@class,'pdp_address')]//text()", input_type="M_XPATH", split_list={" ":-1})
        ItemClear(response=response, item_loader=item_loader, item_name="city", input_value="//h1[contains(@class,'pdp_address')]/span/text()", input_type="F_XPATH", split_list={",":0})
        ItemClear(response=response, item_loader=item_loader, item_name="rent", input_value="//span[contains(@class,'pdp_price')]/text()[not(contains(.,'Price'))]", input_type="F_XPATH", get_num=True, per_week=True,split_list={" ":0}, replace_list={"$":"", ",":""})
        ItemClear(response=response, item_loader=item_loader, item_name="deposit", input_value="//span[contains(@class,'pdp_price')]/text()[contains(.,'/')]", input_type="F_XPATH", get_num=True,split_list={"/":1, " ":0}, replace_list={"$":"", ",":""})
        ItemClear(response=response, item_loader=item_loader, item_name="currency", input_value="AUD", input_type="VALUE")
        ItemClear(response=response, item_loader=item_loader, item_name="room_count", input_value="//span[contains(.,'Bedroom')]/parent::div/following-sibling::div//text()", input_type="F_XPATH", get_num=True)
        ItemClear(response=response, item_loader=item_loader, item_name="square_meters", input_value="//span[contains(.,'Building')]/parent::div/following-sibling::div//text()", input_type="F_XPATH", get_num=True, split_list={"m²":0})
        ItemClear(response=response, item_loader=item_loader, item_name="bathroom_count", input_value="//span[contains(.,'Bathroom')]/parent::div/following-sibling::div//text()", input_type="F_XPATH", get_num=True)
        
        desc = " ".join(response.xpath("//div[@class='pdp_description_content']//text()").getall())
        if desc:
            desc = re.sub('\s{2,}', ' ', desc.strip())
            item_loader.add_value("description", desc)
        
        import dateparser
        month = "".join(response.xpath("//div[contains(@class,'available')]//span[@class='event_month']//text()").getall())
        day = response.xpath("//div[contains(@class,'available')]//span[@class='event_date']//text()").get()
        if day or month:
            available_date = day + " " + month
            date_parsed = dateparser.parse(available_date, date_formats=["%d/%m/%Y"])
            if date_parsed:
                date2 = date_parsed.strftime("%Y-%m-%d")
                item_loader.add_value("available_date", date2)
        
        if "floor" in desc:
            floor = desc.split("floor")[0].strip().split(" ")[-1].replace("th","").replace("rd","")
            if floor.isdigit():
                item_loader.add_value("floor", floor)
        
        item_loader.add_value("external_id", response.url.split("/")[-1])
        images = [x for x in response.xpath("//noscript//@src | //meta[@property='og:image']/@content").getall() if "www.googletagmanager.com" not in x]
        if images:
            item_loader.add_value("images", images)
        ItemClear(response=response, item_loader=item_loader, item_name="parking", input_value="//span[contains(.,'Parking')]/parent::div/following-sibling::div//text()", input_type="F_XPATH", tf_item=True)
        # ItemClear(response=response, item_loader=item_loader, item_name="furnished", input_value="//li[contains(.,'Furnished')]/text()", input_type="F_XPATH", tf_item=True)
        furnished=response.xpath("//h3[@class='charlie']/text()").get()
        if furnished and "furnished" in furnished.lower():
            item_loader.add_value("furnished",True)

        ItemClear(response=response, item_loader=item_loader, item_name="terrace", input_value="//li[contains(.,'Terrace')]/text()", input_type="F_XPATH", tf_item=True)
        ItemClear(response=response, item_loader=item_loader, item_name="balcony", input_value="//li[contains(.,'Balcon')]/text()", input_type="F_XPATH", tf_item=True)
        ItemClear(response=response, item_loader=item_loader, item_name="elevator", input_value="//li[contains(.,'Lift') or contains(.,'lift')]/text()", input_type="F_XPATH", tf_item=True)
        ItemClear(response=response, item_loader=item_loader, item_name="swimming_pool", input_value="//li[contains(.,'pool') or contains(.,'Pool')]/text()", input_type="F_XPATH", tf_item=True)
        ItemClear(response=response, item_loader=item_loader, item_name="washing_machine", input_value="//li[contains(.,'washing') or contains(.,'Washing')]/text()", input_type="F_XPATH", tf_item=True)
        ItemClear(response=response, item_loader=item_loader, item_name="dishwasher", input_value="//li[contains(.,'dishwasher') or contains(.,'Dishwasher')]/text()", input_type="F_XPATH", tf_item=True)
        ItemClear(response=response, item_loader=item_loader, item_name="latitude", input_value="//script[contains(.,'latitude')]/text()", input_type="F_XPATH", split_list={'latitude":':1, ',':0})
        ItemClear(response=response, item_loader=item_loader, item_name="longitude", input_value="//script[contains(.,'longitude')]/text()", input_type="F_XPATH", split_list={'longitude":':1, '}':0})
        ItemClear(response=response, item_loader=item_loader, item_name="landlord_name", input_value="Ray White Roma", input_type="VALUE")
        ItemClear(response=response, item_loader=item_loader, item_name="landlord_phone", input_value="+61 (7) 4622 2688", input_type="VALUE")
        ItemClear(response=response, item_loader=item_loader, item_name="landlord_email", input_value="roma.qld@raywhite.com", input_type="VALUE")
        
        yield item_loader.load_item()