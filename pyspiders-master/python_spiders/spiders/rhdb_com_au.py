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
    name = 'rhdb_com_au'
    execution_type='testing'
    country='australia'
    locale='en'
    custom_settings = {
        "PROXY_ON":"True",
        "HTTPCACHE_ENABLED": False
    }
    headers = {
        ":authority": "www.raineandhorne.com.au",
        ":method": "GET",
        ":path": "/doublebaybondibeach/search/properties?listing_type=residential&offer_type_code=rental&page=1&per_page=12&property_type=Apartment&status=active&surrounding_suburbs=0",
        ":scheme": "https",
        "upgrade-insecure-requests": "1",
        "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/90.0.4430.93 Safari/537.36",
    }
    
    def start_requests(self):
        start_urls = [
            {
                "url" : [
                    "https://www.raineandhorne.com.au/doublebaybondibeach/search/properties?listing_type=residential&offer_type_code=rental&page=1&per_page=12&property_type=Apartment&status=active&surrounding_suburbs=0",
                    "https://www.raineandhorne.com.au/doublebaybondibeach/search/properties?listing_type=residential&offer_type_code=rental&page=1&per_page=12&property_type=Unit&status=active&surrounding_suburbs=0",
                ],
                "property_type" : "apartment",
            },
            {
                "url" : [
                    "https://www.raineandhorne.com.au/doublebaybondibeach/search/properties?listing_type=residential&offer_type_code=rental&page=1&per_page=12&property_type=DuplexSemi-detached&status=active&surrounding_suburbs=0",
                    "https://www.raineandhorne.com.au/doublebaybondibeach/search/properties?listing_type=residential&offer_type_code=rental&page=1&per_page=12&property_type=House&status=active&surrounding_suburbs=0",
                    "https://www.raineandhorne.com.au/doublebaybondibeach/search/properties?listing_type=residential&offer_type_code=rental&page=1&per_page=12&property_type=Townhouse&status=active&surrounding_suburbs=0",
                ],
                "property_type" : "house"
            },
        ]
        for url in start_urls:
            for item in url.get("url"):
                yield Request(item,
                            callback=self.parse,
                            headers=self.headers,
                            meta={'property_type': url.get('property_type')})

    # 1. FOLLOWING
    def parse(self, response):
        page = response.meta.get("page", 2)

        for item in response.xpath("//div[contains(@class,'results-list__item')]/div/a[not(@id)]/@href").getall():
            yield Request(response.urljoin(item), callback=self.populate_item, meta={"property_type":response.meta["property_type"]})
        
        if response.xpath("//a[@id='load-more']/@href").get():
            new_url = response.url.replace("&page=" + str(page - 1), "&page=" + str(page))
            yield Request(new_url, callback=self.parse, headers=self.headers, meta={"property_type":response.meta["property_type"], "page":page+1})

    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)
        item_loader.add_value("property_type", response.meta.get('property_type'))
        item_loader.add_value("external_link", response.url)

        from python_spiders.helper import ItemClear
        ItemClear(response=response, item_loader=item_loader, item_name="external_source", input_value="Rhdb_Com_PySpider_australia", input_type="VALUE")
        ItemClear(response=response, item_loader=item_loader, item_name="city", input_value="//span[@itemprop='addressLocality']//text()", input_type="F_XPATH", replace_list={",":""})
        # ItemClear(response=response, item_loader=item_loader, item_name="zipcode", input_value="//span[@itemprop='postalCode']//text()", input_type="F_XPATH")
        zipcode1=response.xpath("//span[@itemprop='postalCode']//text()").get()
        zipcode2=response.xpath("//span[@itemprop='addressRegion']//text()").get()
        if zipcode1 and zipcode2:
            item_loader.add_value("zipcode",zipcode2.replace("\n","")+" "+zipcode1.replace("\n",""))
        address = " ".join(response.xpath("//span[@itemprop='address']//text()").getall())
        if address:
            address = re.sub('\s{2,}', ' ', address.strip())
            item_loader.add_value("address", address)
        
        ItemClear(response=response, item_loader=item_loader, item_name="rent", input_value="//h4[contains(@class,'desc-price')]/text()[not(contains(.,'Price on Application'))]", input_type="F_XPATH", get_num=True, per_week=True, split_list={" ":0})
        ItemClear(response=response, item_loader=item_loader, item_name="currency", input_value="AUD", input_type="VALUE")
        ItemClear(response=response, item_loader=item_loader, item_name="title", input_value="//h2//text()", input_type="F_XPATH")
        
        desc = " ".join(response.xpath("//div[@itemprop='description']//text()").getall())
        if desc:
            desc = re.sub('\s{2,}', ' ', desc.strip())
            item_loader.add_value("description", desc)
        
        if "sqm" in desc:
            square_meters = desc.split("sqm")[0].strip().split(" ")[-1].replace("c.","")
            item_loader.add_value("square_meters", int(float(square_meters)))
        
        not_list = ["whole", "stained"]
        if "floor " in desc:
            floor = desc.split("floor ")[0].strip().split(" ")[-1]
            if "whole" not in floor.lower() and "stained" not in floor.lower():
                item_loader.add_value("floor", floor)
        
        ItemClear(response=response, item_loader=item_loader, item_name="room_count", input_value="//section[@class='details-desc']//li[@class='beds']/text()", input_type="F_XPATH", get_num=True)
        ItemClear(response=response, item_loader=item_loader, item_name="bathroom_count", input_value="//section[@class='details-desc']//li[@class='baths']/text()", input_type="F_XPATH", get_num=True)
        ItemClear(response=response, item_loader=item_loader, item_name="longitude", input_value="//script[contains(.,'coordinates')]/text()", input_type="F_XPATH", split_list={"coordinates: [":1, ",":0})
        ItemClear(response=response, item_loader=item_loader, item_name="latitude", input_value="//script[contains(.,'coordinates')]/text()", input_type="F_XPATH", split_list={"coordinates: [":1, ",":1, "]":0})
        
        images = [x.split("background-image: url(")[1].split(")")[0] for x in response.xpath("//div[contains(@class,'slider__slide')]//@style").getall()]
        if images:
            item_loader.add_value("images", images)
        
        from datetime import datetime
        import dateparser
        available_date = response.xpath("normalize-space(//h4[contains(@class,'desc-price')]/text()[contains(.,'Available')])").get()
        if available_date:
            if "now" in available_date.lower():
                item_loader.add_value("available_date", datetime.now().strftime("%Y-%m-%d"))
            else:
                date_parsed = dateparser.parse(available_date.split(":")[1].strip(), date_formats=["%d/%m/%Y"])
                if date_parsed:
                    date2 = date_parsed.strftime("%Y-%m-%d")
                    item_loader.add_value("available_date", date2)
        furnished = response.xpath("//div[@itemprop='description']//text()[contains(.,'Furnished') or contains(.,'furnished')]").get()
        if furnished:
            if "unfurnished" in furnished.lower():
                item_loader.add_value("furnished", False)
            elif "furnished" in furnished.lower():
                item_loader.add_value("furnished", True)              
        ItemClear(response=response, item_loader=item_loader, item_name="parking", input_value="//section[@class='details-desc']//li[@class='cars']/text()[.!='0']", input_type="F_XPATH", tf_item=True)
        ItemClear(response=response, item_loader=item_loader, item_name="external_id", input_value="//section[@class='details-desc']//li[contains(.,'ID')]/span[2]/text()", input_type="F_XPATH")
        ItemClear(response=response, item_loader=item_loader, item_name="landlord_name", input_value="//div[@class='agent-office']/a[1]/text()", input_type="F_XPATH")
        ItemClear(response=response, item_loader=item_loader, item_name="landlord_phone", input_value="//button/@data-phone", input_type="F_XPATH")
        namecheck=item_loader.get_output_value("landlord_name")
        if not namecheck:
            name=response.xpath("//img[@class='logo']/@alt").get()
            if name:
                item_loader.add_value("landlord_name",name)
        phonecheck=item_loader.get_output_value("landlord_phone")
        if not phonecheck:
            phone="".join(response.xpath("//div[@class='menu-item'][1]//p[2]//text()[last()]").getall())
            if phone:
                item_loader.add_value("landlord_phone",phone.replace("\n","").strip())
            
                
        
        yield item_loader.load_item()