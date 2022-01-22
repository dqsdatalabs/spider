# -*- coding: utf-8 -*-
# Author: Mehmet Kurtipek

from scrapy.loader.processors import MapCompose
from scrapy import Spider, item
from scrapy import Request,FormRequest
from scrapy.selector import Selector
from w3lib.html import remove_tags
from python_spiders.loaders import ListingLoader
import json
from urllib.parse import urljoin 
import re

class MySpider(Spider): 
    name = 'student_cribs_com'
    execution_type='testing'
    country='united_kingdom'
    locale='en'
    external_source="Studentcribs_PySpider_united_kingdom_en"
    start_urls = ["https://student-cribs.com/locations/"]
    
    custom_setting={
        "PROXY_ON": True
    }

    # 1. FOLLOWING
    def parse(self, response):
        for item in response.xpath("//a[@class='city-link']/@href").getall():
            f_url = response.urljoin(item)
            yield Request(
                f_url, 
                callback=self.parse_locations, 
            )
        
    def parse_locations(self, response):

        for item in response.xpath("//div[@class='list-map__item__content']//form//@action").getall(): 
            f_url = response.urljoin(item)
            rent=response.xpath("//div[@class='list-map__item__description']/span[@class='property-features']/span/following-sibling::text()").get()
            status =response.xpath("//div[@class='list-map__item__photo-badge list-map__item__photo-badge--available']/text()").get()
            if status and "available" in status.lower():
                yield Request(
                    f_url, 
                    callback=self.populate_item,
                    meta={'rent': rent} 
                )
    # 2. SCRAPING level 2 
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)
         
        item_loader.add_value("external_source", self.external_source)
        item_loader.add_value("external_link", response.url)
        if len(response.url)<50:
            return 
        externalid=response.xpath("//link[@rel='shortlink']/@href").get()
        if externalid:
            item_loader.add_value("external_id",externalid.split("p=")[-1])
        dontallow=response.xpath("//div[@class='waiting-list-message']/text()").get()
        if dontallow and "taken off" in dontallow:
            return  


        title = response.xpath("//title/text()").get()
        if title:
            item_loader.add_value("title", re.sub('\s{2,}', ' ', title.strip()))
        # rent=response.xpath("//script[contains(.,'Purchase')]").get()
        # if rent:
        #     price=rent.split("value: '")[1].split(".")[0]
        #     currency=rent.split('currency: "')[1].split('",')[0]
        #     item_loader.add_value("rent", int(price)*4)
        #     item_loader.add_value("currency", currency)
        rent=response.meta.get('rent')
        if rent:
            rent=re.findall("\d+",rent)
            item_loader.add_value("rent", int(rent[0])*4)
        item_loader.add_value("currency", "GBP")
                
        room_count=response.xpath("//div[contains(@class,'crib-info')]//img[contains(@src,'bed')]//following-sibling::span//span/text()").get()
        if room_count:
            print(room_count)
            item_loader.add_value("room_count", room_count)
            if room_count == "1":
                item_loader.add_value("property_type", "room") 
            else:    
                item_loader.add_value("property_type", "student_apartment")
        # roomcheck=str(item_loader.get_output_value("room_count"))
        # if roomcheck and "0" in roomcheck:
        #     return 

        
        bathroom_count =response.xpath("//div[contains(@class,'crib-info')]//img[contains(@src,'shower')]//following-sibling::span//span/text()").get()
        if bathroom_count :
            bathroom_count = bathroom_count.strip().split(" ")[0]
            item_loader.add_value("bathroom_count ", bathroom_count )

        address = response.xpath("//h1/text()").get()
        if address:
            item_loader.add_value("address", address.strip())
            item_loader.add_value("zipcode", address.split(',')[-1].strip())
        city=response.url
        if city:
            city=city.split("locations/")[-1].split("/")[0]
            item_loader.add_value("city",city.upper()) 
            

        bathroom_count = response.xpath("//span[contains(.,'Bathrooms')]/following-sibling::span/text()").get()
        if bathroom_count:
            item_loader.add_value("bathroom_count", bathroom_count.strip())
        
        latitude=response.xpath("//div[@class='crib-details__map']/div/@data-lat").get()
        longitude=response.xpath("//div[@class='crib-details__map']/div/@data-lng").get()
        if latitude or longitude:
            item_loader.add_value("longitude", longitude)
            item_loader.add_value("latitude", latitude)
        
        dishwasher=response.xpath("//div[contains(@class,'features__list')]//div[contains(.,'Dishwasher')]/text()").get()
        if dishwasher:
            item_loader.add_value("dishwasher", True)
        
        washing_machine=response.xpath("//div[contains(@class,'features__list')]//div[contains(.,'Washer')]/text()").get()
        if washing_machine:
            item_loader.add_value("washing_machine", True)
        
        desc="".join(response.xpath("//div[@class='single-cribs-description']//text()").getall())
        if desc:
            desc=re.sub('\s{2,}', ' ', desc)
            item_loader.add_value("description", desc.strip().replace('\n',' '))
        
        parking=response.xpath("//div[contains(@class,'details__list')]/div/span[contains(.,'Parking')]/following-sibling::span/text()").get()
        if parking!="0":
            item_loader.add_value("parking", True)
        
        images=[x for x in response.xpath("//img[contains(@class,'crib-single-image')]//@src").getall()]
        if images:
            item_loader.add_value("images", images)
        
        floor_plan_images = [x for x in response.xpath("//div/h2[contains(.,'Floor')]/parent::div//a/@href").getall()]
        if floor_plan_images:
            item_loader.add_value("floor_plan_images", floor_plan_images)
        
        
        item_loader.add_value("landlord_name", "STUDENT CRIBS")
        item_loader.add_value("landlord_phone", "0203 758 7000")
        
        if not room_count:
            url = "https://student-cribs.com/wp-admin/admin-ajax.php"
            formdata = {
                "action": "getCribsBySlugAjax",
                "slug": f"{response.url.split('/')[-2]}"
            }
            
            yield FormRequest(
                url,
                dont_filter=True,
                callback=self.get_room_count,
                formdata=formdata,
                meta={
                    "item_loader": item_loader,
                    "slug": f"{response.url.split('/')[-2]}",
                    "base_url": response.url
                }
            )
        else:
            yield item_loader.load_item()
    
    def get_room_count(self, response):
        data = json.loads(response.body)
    
        item_loader = response.meta.get('item_loader')
        room_count = str(data["bedrooms_available"])
        if room_count and not room_count=="0":
            item_loader.add_value("room_count", room_count)
        if room_count=="0":
            return 
        if room_count == "1":
            item_loader.add_value("property_type", "room")
        else:
            item_loader.add_value("property_type", "student_apartment")
        
        if not item_loader.get_collected_values("bathroom_count"):
            item_loader.add_value("bathroom_count", data["bathrooms_amount"])
        
        yield item_loader.load_item()