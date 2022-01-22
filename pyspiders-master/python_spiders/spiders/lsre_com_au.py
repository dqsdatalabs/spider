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
import dateparser

class MySpider(Spider):
    name = 'lsre_com_au'
    execution_type='testing'
    country='australia'
    locale='en'
    custom_settings = {              
        "PROXY_ON" : True,
        "CONCURRENT_REQUESTS": 3,        
        "COOKIES_ENABLED": False,        
        "RETRY_TIMES": 3,        
    }
    download_timeout = 120
    def start_requests(self):
        start_urls = [
            {
                "url" : [
                    "https://lsre.com.au/wp-json/api/listings/all?priceRange=&category=Apartment&limit=18&type=rental&status=current&address=&paged=1",
                ],
                "property_type" : "apartment",
            },
            {
                "url" : [
                    "https://lsre.com.au/wp-json/api/listings/all?priceRange=&category=House%2CTownhouse%2CVilla&limit=18&type=rental&status=current&address=&paged=1",
                ],
                "property_type" : "house"
            },
            {
                "url" : [
                    "https://lsre.com.au/wp-json/api/listings/all?priceRange=&category=Studio&limit=18&type=rental&status=current&address=&paged=1",
                ],
                "property_type" : "studio"
            },
        ]
        for url in start_urls:
            for item in url.get("url"):
                yield Request(item,
                            callback=self.parse,
                            meta={'property_type': url.get('property_type')})

    # 1. FOLLOWING
    def parse(self, response):

        page = response.meta.get("page", 2)
        seen = False

        data = json.loads(response.body)
        if data["status"].upper() == 'SUCCESS':
            seen = True
            for item in data["results"]:           
                yield Request(response.urljoin(item["slug"]), callback=self.populate_item, meta={"property_type":response.meta["property_type"]})

        if page == 2 or seen: 
            yield Request(response.url.split('&paged=')[0] + f"&paged={page}", callback=self.parse, meta={"property_type":response.meta["property_type"], "page":page+1})

    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        item_loader.add_value("property_type", response.meta.get('property_type'))
        item_loader.add_value("external_link", response.url)
        item_loader.add_value("external_id", response.url.split("-")[-1])
        
        address = response.xpath("normalize-space(//span[@itemprop='streetAddress']/text())").get()
        address2 = "".join(response.xpath("//div[contains(@class,'entry-header-wrapper')]//span[@class='sub-add']//text()").getall())
        if address or address2:
            item_loader.add_value("address", address+" "+address2.strip())
            
        item_loader.add_value("zipcode", item_loader.get_collected_values("address")[0].split(',')[-1].strip())
        city = response.xpath("//div[contains(@class,'entry-header-wrapper')]//span[@itemprop='addressLocality']//text()").get()
        if city:
            item_loader.add_value("city", city)
        
        zipcode = response.xpath("//div[contains(@class,'entry-header-wrapper')]//span[@itemprop='postalCode']//text()").get()
        if zipcode:
            item_loader.add_value("zipcode", zipcode)
        
        from python_spiders.helper import ItemClear
        ItemClear(response=response, item_loader=item_loader, item_name="external_source", input_value="Lsre_Com_PySpider_australia", input_type="VALUE")
        ItemClear(response=response, item_loader=item_loader, item_name="title", input_value="//title/text()", input_type="F_XPATH")
        # ItemClear(response=response, item_loader=item_loader, item_name="available_date", input_value="normalize-space(//div[contains(@class,'date-available')]/text())", input_type="F_XPATH", split_list={"Available":1})
        ItemClear(response=response, item_loader=item_loader, item_name="bathroom_count", input_value="//span[contains(.,'Bath')]/following-sibling::span/text()", input_type="F_XPATH", get_num=True)
        ItemClear(response=response, item_loader=item_loader, item_name="rent", input_value="//span[contains(@class,'price')]/text()", input_type="F_XPATH", get_num=True, split_list={".":0}, per_week=True)
        ItemClear(response=response, item_loader=item_loader, item_name="currency", input_value="AUD", input_type="VALUE")
                
        desc = " ".join(response.xpath("//div[contains(@class,'property-description')]//p//text()").getall())
        if desc:
            desc = re.sub('\s{2,}', ' ', desc.strip())
            item_loader.add_value("description", desc)

        if response.xpath("//span[contains(.,'Bed')]/following-sibling::span/text()").get():
            ItemClear(response=response, item_loader=item_loader, item_name="room_count", input_value="//span[contains(.,'Bed')]/following-sibling::span/text()", input_type="F_XPATH", get_num=True)
        elif response.xpath("//span[contains(.,'Studio')]/text()").get() or "studio" in desc.lower():
            item_loader.add_value("room_count", "1")
        
        if "sqm" in desc:
            square_meters = desc.split("sqm")[0].strip().split(" ")[-1]
            item_loader.add_value("square_meters", square_meters)
        
        if "floor " in desc:
            floor = desc.split("floor ")[0].strip().split(" ")[-1]
            if "with" not in floor and "mid" not in floor:
                item_loader.add_value("floor", floor)

        available_date=response.xpath("//div[@class='property-date-available']/text()").get()
        if available_date:
            if "from" in available_date:
                date2 =  available_date.split("from")[1].strip()
                date_parsed = dateparser.parse(
                    date2, date_formats=["%m-%d-%Y"]
                )
                date3 = date_parsed.strftime("%Y-%m-%d")
                item_loader.add_value("available_date", date3)

        
        ItemClear(response=response, item_loader=item_loader, item_name="latitude", input_value="//script[contains(.,'LatLng')]/text()", input_type="F_XPATH", split_list={"LatLng(":1, ",":0})
        ItemClear(response=response, item_loader=item_loader, item_name="longitude", input_value="//script[contains(.,'LatLng')]/text()", input_type="F_XPATH", split_list={"LatLng(":1, ",":1, ")":0})
        ItemClear(response=response, item_loader=item_loader, item_name="images", input_value="//div[@class='slick-container']//@data-lazy", input_type="M_XPATH")
        ItemClear(response=response, item_loader=item_loader, item_name="terrace", input_value="//div[contains(@class,'property-description')]//p//text()[contains(.,'terrace')]", input_type="F_XPATH", tf_item=True)
        ItemClear(response=response, item_loader=item_loader, item_name="elevator", input_value="//div[contains(@class,'property-description')]//p//text()[contains(.,'lift')]", input_type="F_XPATH", tf_item=True)
        ItemClear(response=response, item_loader=item_loader, item_name="balcony", input_value="//div[contains(@class,'property-description')]//p//text()[contains(.,'balcony')]", input_type="F_XPATH", tf_item=True)
        ItemClear(response=response, item_loader=item_loader, item_name="parking", input_value="//li[contains(.,'Garage')]/text() | //span[contains(.,'Car')]/following-sibling::span/text()", input_type="F_XPATH", tf_item=True)
        ItemClear(response=response, item_loader=item_loader, item_name="landlord_name", input_value="//h3/a/text()", input_type="F_XPATH")
        ItemClear(response=response, item_loader=item_loader, item_name="landlord_phone", input_value="//div[contains(@class,'agent-mobile')]//a[contains(@href,'tel')]/span", input_type="F_XPATH")
        ItemClear(response=response, item_loader=item_loader, item_name="landlord_email", input_value="//div[contains(@class,'agent-email')]//a[contains(@href,'mail')]/span", input_type="F_XPATH")

        parking = " ".join(response.xpath("//div[@class='icon-content car']/span[2]/text()").getall())
        if parking:
            item_loader.add_value("parking", True)

        item_loader.add_value("landlord_name", "Laing+Simmons Parramatta")
        item_loader.add_value("landlord_phone", "(02) 9635 4000")
        item_loader.add_value("landlord_email", "parramatta@lsre.com.au")

        status = response.xpath("//span[contains(@class,'price')]/text()").get()
        if "$" in status:
            yield item_loader.load_item()