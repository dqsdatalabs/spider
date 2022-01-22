# -*- coding: utf-8 -*-
# Author: Mehmet Kurtipek

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
    name = 'darwin_eldersrealestate_com_au'
    execution_type='testing'
    country='australia'
    locale='en'
    post_url = "https://darwin.eldersrealestate.com.au/wp-admin/admin-ajax.php?action=search"
    def start_requests(self):

        start_urls = [
            {
                "type":"Apartment & unit",
                "property_type":"apartment",
            },
            {
                "type":"House",
                "property_type":"house",
            },
            {
                "type":"Townhouse",
                "property_type":"house",
            },
            {
                "type":"Villa",
                "property_type":"house",
            },
        ]

        
        for item in start_urls:
            formdata = {
                "listing_type": "residential",
                "sale_type": "rent",
                "status": "current",
                "property_type[]": item.get("type"),
                "count": "1000",
                "response": "grid",
                "sort": "date.desc",
            }
            yield FormRequest(
                url=self.post_url,
                callback=self.parse,
                formdata=formdata,
                meta={
                    "property_type":item["property_type"],
                }
            )

    # 1. FOLLOWING
    def parse(self, response):
        data = json.loads(response.body)
        sel = Selector(text=data["data"], type="html")
        for item in sel.xpath("//div[@class='property-card__body']/a[@class='property-card__link']/@href").getall():
            follow_url = response.urljoin(item)
            yield Request(follow_url, callback=self.populate_item, meta={"property_type":response.meta["property_type"]})
        
    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)
        item_loader.add_value("external_link", response.url)
        item_loader.add_value("property_type", response.meta["property_type"])

        from python_spiders.helper import ItemClear
        ItemClear(response=response, item_loader=item_loader, item_name="external_source", input_value="Darwin_Eldersrealestate_Com_PySpider_australia", input_type="VALUE")
        ItemClear(response=response, item_loader=item_loader, item_name="title", input_value="//title//text()", input_type="F_XPATH")
        ItemClear(response=response, item_loader=item_loader, item_name="city", input_value="//h2[contains(@class,'title')]/text()", input_type="F_XPATH", split_list={" ":0})
        
        address = " ".join(response.xpath("//h1[contains(@class,'title')]/text() | //h2[contains(@class,'title')]/text()").getall())
        if address:
            address = re.sub('\s{2,}', ' ', address.strip())
            item_loader.add_value("address", address)

        ItemClear(response=response, item_loader=item_loader, item_name="room_count", input_value="substring-after(//li/strong[contains(.,'Bedroom')]/following-sibling::text(),':')", input_type="F_XPATH", get_num=True)
        ItemClear(response=response, item_loader=item_loader, item_name="bathroom_count", input_value="substring-after(//li/strong[contains(.,'Bathroom')]/following-sibling::text(),':')", input_type="F_XPATH", get_num=True)
        # ItemClear(response=response, item_loader=item_loader, item_name="rent", input_value="//div[@class='property__price']/text()[contains(.,'$')]", input_type="F_XPATH", get_num=True, per_week=True, split_list={" ":0})
        ItemClear(response=response, item_loader=item_loader, item_name="currency", input_value="AUD", input_type="VALUE")

        rent = " ".join(response.xpath("//div[@class='property__price']/text()[contains(.,'$')]").getall()).strip().split(" ")[0].replace("$","")
        if rent:
            price = int(float(rent))
            print(price)
            item_loader.add_value("rent", price*4)



        desc = " ".join(response.xpath("//section[contains(@class,'content')]//text()").getall())
        if desc:
            desc = re.sub('\s{2,}', ' ', desc.strip())
            item_loader.add_value("description", desc)
        
        if "sqm" in desc:
            sqm = desc.split("sqm")[0].strip().split(" ")[-1]
            if int(sqm) <1000:
                item_loader.add_value("square_meters", sqm)
        
        if "floor " in desc:
            floor = desc.split("floor ")[0].strip().split(" ")[-1]
            if "low" not in floor:
                item_loader.add_value("floor", floor)
        
        item_loader.add_value("external_id", response.url.split("-")[-1].split("/")[0])

        zipcode = response.xpath("//h2[contains(@class,'title')]/text()").get()
        if zipcode: 
            zipcode = zipcode.strip().split(" ")[-1]
            item_loader.add_value("zipcode", f"NT {zipcode}")
        
        ItemClear(response=response, item_loader=item_loader, item_name="swimming_pool", input_value="//li[contains(.,'Pool')]//text()", input_type="F_XPATH", tf_item=True)
        ItemClear(response=response, item_loader=item_loader, item_name="balcony", input_value="//li[contains(.,'Balcony')]//text()", input_type="F_XPATH", tf_item=True)
        ItemClear(response=response, item_loader=item_loader, item_name="terrace", input_value="//li[contains(.,'Terrace')]//text()", input_type="F_XPATH", tf_item=True)
        ItemClear(response=response, item_loader=item_loader, item_name="furnished", input_value="//li[contains(.,' furnished') or contains(.,'Furnished')]//text()", input_type="F_XPATH", tf_item=True)
        ItemClear(response=response, item_loader=item_loader, item_name="elevator", input_value="//section[contains(@class,'content')]//text()[contains(.,'Lift')]", input_type="F_XPATH", tf_item=True)
        ItemClear(response=response, item_loader=item_loader, item_name="parking", input_value="//ul[contains(@class,'property__key-features')]/li[contains(.,'Park')]/text()[not(contains(.,'0'))]", input_type="F_XPATH", tf_item=True)
        ItemClear(response=response, item_loader=item_loader, item_name="latitude", input_value="//a/@data-lat", input_type="F_XPATH")
        ItemClear(response=response, item_loader=item_loader, item_name="longitude", input_value="//a/@data-lng", input_type="F_XPATH")
        ItemClear(response=response, item_loader=item_loader, item_name="images", input_value="//div[@class='property__carousel']//@src", input_type="M_XPATH")
        ItemClear(response=response, item_loader=item_loader, item_name="available_date", input_value="//div[contains(@class,'available-date')]//text()", input_type="M_XPATH", split_list={"Available":1, "from":-1})
        
        item_loader.add_value("landlord_phone", "08 8946 0500")
        item_loader.add_value("landlord_name", "Elders Real Estate Darwin")

        if not response.xpath("//div[contains(text(),'CURRENTLY NO VACANCIES')]").get():
            yield item_loader.load_item()

