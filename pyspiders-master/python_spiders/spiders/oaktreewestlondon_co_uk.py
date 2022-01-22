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
    name = 'oaktreewestlondon_co_uk'
    execution_type = 'testing'
    country = 'united_kingdom'
    locale = 'en'

    def start_requests(self):
        start_urls = [
            {
                "url" : [
                    "https://www.oaktreewestlondon.co.uk/search/?showstc=on&showsold=on&instruction_type=Letting&address_keyword=&minprice=&maxprice=&property_type=Flat%2FApartment",
                ],
                "property_type" : "apartment",
            },
            {
                "url" : [
                    "https://www.oaktreewestlondon.co.uk/search/?showstc=on&showsold=on&instruction_type=Letting&address_keyword=&minprice=&maxprice=&property_type=House",
                    "https://www.oaktreewestlondon.co.uk/search/?showstc=on&showsold=on&instruction_type=Letting&address_keyword=&minprice=&maxprice=&property_type=Maisonette",
                    
                ],
                "property_type" : "house"
            },
        ]
        for url in start_urls:
            for item in url.get("url"):
                yield Request(item,
                            callback=self.parse,
                            meta={'property_type': url.get('property_type'), "base_url":item})


    # 1. FOLLOWING
    def parse(self, response):
        page = response.meta.get("page", 2)
        seen = False
        for item in response.xpath("//a[contains(.,'Property Details')]/@href").getall():
            follow_url = response.urljoin(item)
            yield Request(follow_url, callback=self.populate_item, meta={"property_type":response.meta["property_type"]})
            seen = True
         
        if page == 2 or seen:
            base_url = response.url.split("=")[-1]
            p_url = f"https://www.oaktreewestlondon.co.uk/search/{page}.html?showstc=on&showsold=on&instruction_type=Letting&address_keyword=&minprice=&maxprice=&property_type={base_url}"
            yield Request(
                p_url,
                callback=self.parse,
                meta={"page":page+1, "property_type":response.meta["property_type"]})
         
    
    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)
        
        status = response.xpath("//h1[contains(.,'Find That Page')]/text()").get()
        if status:
            return
        zipcode = response.xpath("//meta[@property='og:description']/@content").get()
        if zipcode:
            zipcode = " ".join(zipcode.split(".")[0].strip().split(" ")[-2:])
            if not zipcode.replace(" ","").isalpha() and "=" not in zipcode:
                item_loader.add_value("zipcode", zipcode)
        item_loader.add_value("property_type", response.meta.get('property_type'))
        item_loader.add_value("external_link", response.url.split("&propertyNum")[0])
        extenalid=response.url.split("details/")[-1].split("/")[0]
        if extenalid:
            item_loader.add_value("external_id",extenalid)

        from python_spiders.helper import ItemClear
        ItemClear(response=response, item_loader=item_loader, item_name="external_source", input_value="Oaktreewestlondon_Co_PySpider_united_kingdom", input_type="VALUE")
        ItemClear(response=response, item_loader=item_loader, item_name="external_id", input_value="//p[strong[.='Reference:']]/text()", input_type="F_XPATH")
        ItemClear(response=response, item_loader=item_loader, item_name="address", input_value="//h1/text()", input_type="F_XPATH", split_list={":":-1})
        ItemClear(response=response, item_loader=item_loader, item_name="city", input_value="//h1/text()", input_type="F_XPATH", split_list={",":-1})
        # ItemClear(response=response, item_loader=item_loader, item_name="city", input_value="//h4[@class='ico_magnify']/text()", input_type="F_XPATH", split_list={":":-1, ",":-2})
        ItemClear(response=response, item_loader=item_loader, item_name="title", input_value="//title/text()", input_type="F_XPATH")
        ItemClear(response=response, item_loader=item_loader, item_name="description", input_value="//h2[contains(.,'Description')]/../text()", input_type="M_XPATH")
        ItemClear(response=response, item_loader=item_loader, item_name="square_meters", input_value="//h2[contains(.,'Description')]/../text()[contains(.,'sq ft')]", input_type="F_XPATH", get_num=True, split_list={"sq ft":0, " ":-1}, sq_ft=True)
        ItemClear(response=response, item_loader=item_loader, item_name="room_count", input_value="//span[@class='property-bedrooms']/text()", input_type="F_XPATH", get_num=True, split_list={"bed":0, ",":-1})
        ItemClear(response=response, item_loader=item_loader, item_name="bathroom_count", input_value="//span[@class='property-bathrooms']/text()", input_type="F_XPATH", get_num=True, split_list={"bath":0, ",":-1})
        ItemClear(response=response, item_loader=item_loader, item_name="rent", input_value="//div[contains(@class,'price')]/text()", input_type="F_XPATH", get_num=True, split_list={" ":0, "£":1}, replace_list={",":""})
        ItemClear(response=response, item_loader=item_loader, item_name="currency", input_value="GBP", input_type="VALUE")
        ItemClear(response=response, item_loader=item_loader, item_name="available_date", input_value="//div[contains(@class,'price')]/text()[contains(.,'Available:')]", input_type="F_XPATH", lower_or_upper=0, split_list={":":1, " ":0})
        ItemClear(response=response, item_loader=item_loader, item_name="images", input_value="//div[@id='carousel--property']//@data-background", input_type="M_XPATH")
        ItemClear(response=response, item_loader=item_loader, item_name="latitude", input_value="//script[contains(.,'map')]/text()", input_type="F_XPATH", split_list={"&q=":1, "%2C":0})
        ItemClear(response=response, item_loader=item_loader, item_name="longitude", input_value="//script[contains(.,'map')]/text()", input_type="F_XPATH", split_list={"%2C":1, '"':0})
        ItemClear(response=response, item_loader=item_loader, item_name="parking", input_value="//br/following-sibling::text()[contains(.,'Parking') or contains(.,'parking')]", input_type="F_XPATH", tf_item=True)
        ItemClear(response=response, item_loader=item_loader, item_name="furnished", input_value="//br/following-sibling::text()[contains(.,'Furnished')]", input_type="F_XPATH", tf_item=True)
        ItemClear(response=response, item_loader=item_loader, item_name="landlord_name", input_value="Oaktree (West London) Limited", input_type="VALUE")
        ItemClear(response=response, item_loader=item_loader, item_name="landlord_phone", input_value="020 8997 8533", input_type="VALUE")
        ItemClear(response=response, item_loader=item_loader, item_name="landlord_email", input_value="enquiries@oaktreewestlondon.co.uk", input_type="VALUE")
       
        yield item_loader.load_item()