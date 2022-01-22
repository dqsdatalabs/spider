# -*- coding: utf-8 -*-
# Author: Mehmet Kurtipek

from scrapy.loader.processors import MapCompose
from scrapy import Spider
from scrapy import Request,FormRequest
from scrapy.selector import Selector 
from python_spiders.loaders import ListingLoader
import json
import re

class MySpider(Spider):
    name = 'londonroomz_co_uk'
    execution_type = 'testing'
    country = 'united_kingdom'
    locale = 'en'

    def start_requests(self): 
        start_urls = [
            {
                "url" : [
                    "https://londonroomz.co.uk/flats/",
                ],
                "property_type" : "apartment",
            },
            {
                "url" : [
                    "https://londonroomz.co.uk/rooms/",
                ],
                "property_type" : "room",
            },
            {
                "url" : [
                    "https://londonroomz.co.uk/houses/",
                    
                ],
                "property_type" : "house"
            },
        ]
        for url in start_urls:
            for item in url.get("url"):
                yield Request(item.format(""),
                            callback=self.parse,
                            meta={'property_type': url.get('property_type')})


    # 1. FOLLOWING
    def parse(self, response):
        for item in response.xpath("//div[@class='elementor-row']/div[contains(@class,'elementor-col-33')]"):
            status = " ".join(item.xpath(".//h2[contains(@class,'elementor-heading-title')]//text()").getall())
            if status and "agreed" in status.lower():
                continue
            follow_url = response.urljoin(item.xpath(".//div[@class='elementor-image']/a[contains(@href,'properties')]/@href").get())
            yield Request(follow_url, callback=self.populate_item, meta={"property_type":response.meta["property_type"]})
           
         
    
    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        prop_type = "".join(response.xpath("//section[contains(.,'Property') and contains(@class,'inner')]//div[@class='elementor-row']/div[last()]//h2//text()").getall())
        if prop_type and "flat" in prop_type.lower():
            item_loader.add_value("property_type", "apartment")
        elif prop_type and "room" in prop_type.lower():
            item_loader.add_value("property_type", "room")
        else:
            item_loader.add_value("property_type", response.meta.get('property_type'))
        item_loader.add_value("external_link", response.url)

        external_id = response.xpath("//link[@rel='shortlink']/@href").get()
        if external_id:
            item_loader.add_value("external_id", external_id.split("p=")[-1])
        
        from python_spiders.helper import ItemClear
        ItemClear(response=response, item_loader=item_loader, item_name="external_source", input_value="Londonroomz_Co_PySpider_united_kingdom", input_type="VALUE")
        city = response.xpath("//div[@id='content']//section[contains(@class,'top')]/div/div/div[1]/div/div/div[1]//h2/text()").get()
        if city:
            ItemClear(response=response, item_loader=item_loader, item_name="address", input_value="//div[@id='content']//section[contains(@class,'top')]/div/div/div[1]/div/div/div[1]//h2/text()", input_type="F_XPATH")
            
            city = city.strip().split(" ")
            if city[-1].isalpha():
                pass
            elif city[-2].isalpha():
                item_loader.add_value("zipcode", city[-1])
            elif city[-3].isdigit():
                item_loader.add_value("zipcode", city[-2]+city[-1])
        elif response.xpath("//h1/text()"):
            address = response.xpath("normalize-space(//h1/text())").get()
            item_loader.add_value("address", address)
            item_loader.add_value("zipcode", f"{address.split(' ')[-2]} {address.split(' ')[-1]}")
        
        item_loader.add_value("city", "London")
        
        ItemClear(response=response, item_loader=item_loader, item_name="title", input_value="//title/text()", input_type="F_XPATH")
        ItemClear(response=response, item_loader=item_loader, item_name="description", input_value="//h4[contains(.,'Property Description')]/following-sibling::*//text() | //h3[contains(.,'Description')]/following-sibling::*//text()", input_type="M_XPATH")
        #ItemClear(response=response, item_loader=item_loader, item_name="room_count", input_value="//h4[contains(.,'Description')]/following-sibling::*//text()[contains(.,'Bedroom')]", input_type="F_XPATH", get_num=True, lower_or_upper=0, split_list={"bedroom":0, " ":-1})
        ItemClear(response=response, item_loader=item_loader, item_name="bathroom_count", input_value="//section[contains(.,'Bath Rooms') and contains(@class,'inner')]//div[@class='elementor-row']/div[last()]//h2/text()", input_type="F_XPATH", get_num=True)
        ItemClear(response=response, item_loader=item_loader, item_name="rent", input_value="//section[contains(.,'Rent') and contains(@class,'inner')]//div[@class='elementor-row']/div[last()]//h2/text()[not(contains(.,'Room'))]", input_type="F_XPATH", get_num=True, split_list={"-":0})
        ItemClear(response=response, item_loader=item_loader, item_name="currency", input_value="GBP", input_type="VALUE")
        ItemClear(response=response, item_loader=item_loader, item_name="available_date", input_value="//section[contains(.,'Available Date') and contains(@class,'inner')]//div[@class='elementor-row']/div[last()]//h2/text()", input_type="F_XPATH")
        ItemClear(response=response, item_loader=item_loader, item_name="deposit", input_value="//section[contains(.,'Deposit') and contains(@class,'inner')]//div[@class='elementor-row']/div[last()]//h2/text()", input_type="F_XPATH", get_num=True, split_list={"-":0})
        ItemClear(response=response, item_loader=item_loader, item_name="images", input_value="//div[contains(@class,'image-carousel swiper-wrapper')]//img/@src", input_type="M_XPATH")
        ItemClear(response=response, item_loader=item_loader, item_name="latitude", input_value="//script[contains(.,'latitude')]/text()", input_type="F_XPATH", split_list={'"latitude":"':-1, '"':0})
        ItemClear(response=response, item_loader=item_loader, item_name="longitude", input_value="//script[contains(.,'latitude')]/text()", input_type="F_XPATH", split_list={'"longitude":"':-1, '"':0})
        ItemClear(response=response, item_loader=item_loader, item_name="terrace", input_value="//section[contains(.,'Property type') and contains(@class,'inner')]//div[@class='elementor-row']/div[last()]//h2/text()[contains(.,'Terrace')]", input_type="F_XPATH", tf_item=True)
        ItemClear(response=response, item_loader=item_loader, item_name="landlord_name", input_value="LondonRoomZ", input_type="VALUE")
        ItemClear(response=response, item_loader=item_loader, item_name="landlord_phone", input_value="02036413739", input_type="VALUE")
        ItemClear(response=response, item_loader=item_loader, item_name="landlord_email", input_value="Contact@londonroomz.co.uk", input_type="VALUE")
        #ItemClear(response=response, item_loader=item_loader, item_name="square_meters", input_value="//section[contains(.,'Room Size') and contains(@class,'inner')]//div[@class='elementor-row']/div[last()]//h2/text()", input_type="F_XPATH", get_num=True, sq_ft=True)
        
        square_meters=response.xpath("//section[contains(.,'Room Size') and contains(@class,'inner')]//div[@class='elementor-row']/div[last()]//h2/text()").get()
        if square_meters:
            if not square_meters.isalpha(): 
                square_meters=re.findall("\d+",square_meters)
                item_loader.add_value("square_meters",square_meters)

            


        if not item_loader.get_collected_values("zipcode"):
            zipcode = response.xpath("//section[contains(.,'Property') and contains(@class,'inner')]/../div[contains(@class,'heading')]//h2//text()").get()
            if zipcode:
                zipcode = " ".join(zipcode.strip().split(' ')[-2:]).strip()
                if not zipcode.split(" ")[0].isalpha():
                    item_loader.add_value("zipcode", zipcode)
        
        yield item_loader.load_item()