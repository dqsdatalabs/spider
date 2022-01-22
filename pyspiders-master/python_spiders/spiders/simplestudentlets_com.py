# -*- coding: utf-8 -*-
# Author: Mehmet Kurtipek

from scrapy.loader.processors import MapCompose
from scrapy import Spider
from scrapy import Request,FormRequest
from scrapy.selector import Selector
from w3lib.html import remove_tags
from python_spiders.loaders import ListingLoader
import json
from python_spiders.helper import ItemClear
import re

class MySpider(Spider):
    name = 'simplestudentlets_com'
    execution_type = 'testing'
    country = 'united_kingdom'
    locale = 'en'
    start_urls = ["http://www.simplestudentlets.com/student/"]

    # 1. FOLLOWING
    def parse(self, response):
        page = response.meta.get("page", 2)
        seen = False
        for item in response.xpath("//div[@class='card-header']/a"):
            follow_url = response.urljoin(item.xpath("./@href").get())
            yield Request(follow_url, callback=self.populate_item)
            seen = True
        
        if page == 2 or seen:
            p_url = f"http://www.simplestudentlets.com/student/page/{page}/"
            yield Request(
                p_url,
                callback=self.parse,
                meta={"page":page+1})
        
        
    
    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)
        item_loader.add_value("external_source", "Simplestudentlets_PySpider_united_kingdom")
        item_loader.add_value("external_link", response.url)

        prop_type = response.xpath("//span[contains(text(),'Property Type')]/text()").get()
        if prop_type and "studio" in prop_type.lower():
            prop_type = "studio"
        elif prop_type and ("house" in prop_type.lower() or "detached" in prop_type.lower() or "villa" in prop_type.lower()):
            prop_type = "house"
        elif prop_type and ("apartment" in prop_type.lower() or "terrace" in prop_type.lower() or "flat" in prop_type.lower()):
            prop_type = "apartment"
        else:
            prop_type = "student_apartment"
        item_loader.add_value("property_type", prop_type)
    
        ItemClear(response=response, item_loader=item_loader, item_name="title", input_value="//header/h1//text()", input_type="F_XPATH")
        ItemClear(response=response, item_loader=item_loader, item_name="address", input_value="//header/h1//text()", input_type="F_XPATH")
        ItemClear(response=response, item_loader=item_loader, item_name="external_id", input_value="//span[@class='ref'][contains(.,'Ref')]/text()", input_type="F_XPATH", split_list={":":1})
        ItemClear(response=response, item_loader=item_loader, item_name="available_date", input_value="//span[@class='ref'][contains(.,'Available')]/text()", input_type="F_XPATH", split_list={" ":-1})
        ItemClear(response=response, item_loader=item_loader, item_name="room_count", input_value="//aside//div[@class='bed-info']/span//text()", get_num=True, input_type="F_XPATH", split_list={" ":0})
        ItemClear(response=response, item_loader=item_loader, item_name="bathroom_count", input_value="//aside//div[@class='shower-info']/span//text()", get_num=True, input_type="F_XPATH", split_list={" ":0})
        ItemClear(response=response, item_loader=item_loader, item_name="furnished", input_value="//ul/li[contains(.,' furnished') or contains(.,'Furnished')]//text()", input_type="F_XPATH", tf_item=True)
        ItemClear(response=response, item_loader=item_loader, item_name="washing_machine", input_value="//ul/li[contains(.,'Washer') or contains(.,'Washing')]//text()", input_type="F_XPATH", tf_item=True)
        ItemClear(response=response, item_loader=item_loader, item_name="dishwasher", input_value="//ul/li[contains(.,'Dishwasher')]//text()", input_type="F_XPATH", tf_item=True)
        ItemClear(response=response, item_loader=item_loader, item_name="parking", input_value="//ul/li[contains(.,'parking') or contains(.,'Parking')]//text()", input_type="F_XPATH", tf_item=True)
        ItemClear(response=response, item_loader=item_loader, item_name="balcony", input_value="//li[contains(.,'Balcony') or contains(.,'balcony')]/text()", input_type="F_XPATH", tf_item=True)
        
        rent = "".join(response.xpath("//header/div[@class='price']/span//text()").getall())
        if rent:
            item_loader.add_value("currency", "GBP")
            if "pppcm" in rent:
                rent = rent.split("pppcm")[0].split("£")[-1].strip().split(".")[0]
            else:
                rent = rent.split("£")[1].split(" ")[0].strip().split(".")[0]
                rent = str(int(rent)*4)
            item_loader.add_value("rent", rent.strip())

        address = response.xpath("//header/h1//text()").get()
        if address:
            zipcode = address.replace(",","").strip().split(" ")
            if not zipcode[-2].isalpha():
                zipcode = zipcode[-2]+" "+zipcode[-1]
                item_loader.add_value("zipcode", zipcode)
                city = address.split(zipcode)[0].strip().strip(",")
                if "," in city:
                    if not city.split(",")[-1].strip().split(" ")[0].isdigit():
                        item_loader.add_value("city", city.split(",")[-1].strip())
                    else: item_loader.add_value("city", city.strip().split(" ")[-1])
                else: item_loader.add_value("city", city.strip())
            else: item_loader.add_value("address", address.split(",")[-2])
                
        desc = " ".join(response.xpath("//div[@class='pf-content']//text()").getall())
        if desc:
            desc = re.sub('\s{2,}', ' ', desc.strip())
            item_loader.add_value("description", desc)
        
        if "deposit of" in desc:
            deposit = desc.split("deposit of")[1].split("\u00a3")[1].strip().split(" ")[0]
            if "." in deposit:
                item_loader.add_value("deposit", deposit.split(".")[0])
            else:
                item_loader.add_value("deposit", deposit)
        
        lng = response.xpath("//div[@id='map']/@data-longitude").get()
        if lng:
            if float(lng) != 0:
                item_loader.add_value("longitude", lng)
        
        lat = response.xpath("//div[@id='map']/@data-latitude").get()
        if lat:
            if float(lat) != 0:
                item_loader.add_value("latitude", lat)

                
        ItemClear(response=response, item_loader=item_loader, item_name="energy_label", input_value="//div[@class='energy-graph']/h4/text()", input_type="F_XPATH", split_list={"-":1}, lower_or_upper=1)
        ItemClear(response=response, item_loader=item_loader, item_name="images", input_value="//div[@class='property-images']//img/@src", input_type="M_XPATH")
        
        ItemClear(response=response, item_loader=item_loader, item_name="landlord_name", input_value="PROPERTY SOLUTIONS", input_type="VALUE")
        ItemClear(response=response, item_loader=item_loader, item_name="landlord_phone", input_value="0121 472 1133", input_type="VALUE")
        ItemClear(response=response, item_loader=item_loader, item_name="landlord_email", input_value="mail@propertysolutionsuk.com", input_type="VALUE")
        
        yield item_loader.load_item()


