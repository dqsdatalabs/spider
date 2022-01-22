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
    name = 'glbpropertyservices_com'
    execution_type='testing'
    country='united_kingdom'
    locale='en'
    external_source="GlbPropertyservices_PySpider_united_kingdom_en"
    start_urls = ["https://glbpropertyservices.com/"]


    def parse(self, response):

        token = response.xpath("//input[@name='token']/@value").get()
        
        formdata = {
            "type": "2",
            "keywords": "",
            "bedroomsMin": "",
            "bedroomsMax": "",
            "priceMin": "",
            "priceMax": "",
            "token": token,
            "post": "TRUE",
        }

        yield FormRequest(
            url="https://glbpropertyservices.com/search-result/",
            callback=self.jump,
            formdata=formdata,
        )

    # 1. FOLLOWING
    def jump(self, response):

        token = response.xpath("//input[@name='token']/@value").get()

        page = response.meta.get("page", 1)

        seen = False
        for item in response.xpath("//div[@class='Col-Price']/a/@href").extract():
            follow_url = response.urljoin(item)
            yield Request(follow_url, callback=self.populate_item)
            seen = True
        
        if page == 1 or seen:
            formdata = {
                "dbase_equal": "2",
                "limit": "",
                "featured_bin": "",
                "uploaded": "",
                "price_min": "",
                "price_max": "",
                "bedrooms_min": "",
                "bedrooms_max": "",
                "id_equal": "",
                "area_like": "",
                "token": token,
                "post": "TRUE",
                "page": str(page),
            }
            yield FormRequest(
                url="https://glbpropertyservices.com/search-result/",
                callback=self.jump,
                formdata=formdata,
                meta={"page":page+1}
            )
        
    
    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        item_loader.add_value("external_link", response.url)
        item_loader.add_value("external_source", self.external_source)

        desc = "".join(response.xpath("//div[@class='Row-Description']/p/text()").getall())
        if "student" in desc.lower():
            item_loader.add_value("property_type", "room")
        elif desc and ("apartment" in desc.lower() or "flat" in desc.lower() or "maisonette" in desc.lower()):
            item_loader.add_value("property_type", "apartment")
        elif desc and "house" in desc.lower():
             item_loader.add_value("property_type", "house")
        elif desc and "studio" in desc.lower():
             item_loader.add_value("property_type", "studio")
        elif desc and "Room" in desc:
             item_loader.add_value("property_type", "room")
        else:
            return
        
        title = response.xpath("//h2/text()").get() 
        item_loader.add_value("title", title)
        
        if title:
            address = title.split("Bedroom")[1].strip(",").strip()
            item_loader.add_value("address", address)
            item_loader.add_value("city", address.split(",")[-1].strip())
        
        rent = "".join(response.xpath("//div[@class='Price']//text()").getall())
        if "Month" in rent or "PCM" in rent:
            item_loader.add_value("rent", rent.split("£")[1].strip().replace(",",""))
        elif "Week" in rent:
            price = rent.split("£")[1].strip()
            item_loader.add_value("rent", str(int(price)*4))
        else:
            item_loader.add_value("rent", rent.split("£")[1].strip().replace(",",""))

        item_loader.add_value("currency", "GBP")
        
        # if "student" in desc.lower():
        #     item_loader.add_value("room_count", "1")
        #     print(response.url) 
        # else:
        rentroomcheck=item_loader.get_output_value("rent")
        if int(rentroomcheck)<900:
            item_loader.add_value("room_count","1")
        else:
            room_count = response.xpath("//span[@title='bedrooms']//text()").get()
            if room_count:
                item_loader.add_value("room_count", room_count)
        if "student" in desc.lower():
            item_loader.add_value("bathroom_count","1")
        else:
            bathroom_count = response.xpath("//span[@title='bathrooms']//text()").get()
            if bathroom_count:
                item_loader.add_value("bathroom_count", bathroom_count)

        external_id = response.xpath("//span[@title='ref']//text()").get()
        if external_id:
            item_loader.add_value("external_id", external_id.split(":")[1])

        lat_lng = response.xpath("//script[contains(.,'lng')]/text()").get()
        if lat_lng:
            lat = lat_lng.split("lat:")[1].split(",")[0].strip()
            lng = lat_lng.split("lng:")[1].split("}")[0].strip()
            item_loader.add_value("latitude", lat)
            item_loader.add_value("longitude", lng)
        
        desc = "".join(response.xpath("//div[@class='Row-Description']//p//text()").getall())
        if desc:
            item_loader.add_value("description", re.sub('\s{2,}', ' ', desc.strip()))
        
        images = [ x for x in response.xpath("//div[contains(@class,'owl-carousel')]//a/img/@src").getall()]
        if images:
            item_loader.add_value("images", images)
        
        furnished = response.xpath(
            "//div[@class='Row-Description']//p//text()[contains(.,'Furnish') or contains(.,'furnish')]"
            ).get()
        unfurnished = response.xpath(
            "//div[@class='Row-Description']//p//text()[contains(.,'Unfurnish') or contains(.,'unfurnish')]"
            ).get()
        if unfurnished:
            item_loader.add_value("furnished", False)
        elif furnished:
            item_loader.add_value("furnished", True)
        
        # washing_machine = response.xpath(
        #     "//div[@class='Row-Description']//p//text()[contains(.,'Washing Machine') or contains(.,'washing Machine')]"
        #     ).get()
        # if washing_machine:
        #     item_loader.add_value("washing_machine", True)
            
        # dishwasher = response.xpath(
        #     "//div[@class='Row-Description']//p//text()[contains(.,'dishwasher') or contains(.,'Dishwasher')]"
        #     ).get()
        # if dishwasher:
        #     item_loader.add_value("dishwasher", True)
            
        # balcony = response.xpath(
        #     "//div[@class='Row-Description']//p//text()[contains(.,'balcony') or contains(.,'Balcony')]"
        #     ).get()
        # if balcony:
        #     item_loader.add_value("balcony", True)
        
        # parking = response.xpath(
        #     "//div[@class='Row-Description']//p//text()[contains(.,'parking') or contains(.,'Parking')]"
        #     ).get()
        # if parking:
        #     item_loader.add_value("parking", True)
            
        # terrace = response.xpath(
        #     "//div[@class='Row-Description']//p//text()[contains(.,'terrace') or contains(.,'Terrace')]"
        #     ).get()
        # if terrace:
        #     item_loader.add_value("terrace", True)
            
        # elevator = response.xpath(
        #     "//div[@class='Row-Description']//p//text()[contains(.,'lift') or contains(.,'Lift')]"
        #     ).get()
        # if elevator:
        #     item_loader.add_value("elevator", True)
        
        deposit = response.xpath("//div[@class='Row-Description']//p//text()[contains(.,'Deposit')]").get()
        if deposit:
            deposit = deposit.split("(Deposit")[1].split(")")[0].replace("£","").strip()
            item_loader.add_value("deposit", deposit)
        
        
        item_loader.add_value("landlord_name", "GLB Property Services Ltd") 
        item_loader.add_value("landlord_phone", "01926 356893") 
        item_loader.add_value("landlord_email", "info@glbpropertyservices.com") 
        
        status = response.xpath("//span[contains(@class,'listing-detail')][contains(.,'LET AGREED')]/text()").get()
        if not status:
            yield item_loader.load_item()