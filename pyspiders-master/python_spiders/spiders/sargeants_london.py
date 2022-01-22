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
    name = 'sargeants_london'
    execution_type = 'testing'
    country = 'united_kingdom'
    locale = 'en'
    thousand_separator = ','
    scale_separator = '.' 
    
    def start_requests(self):
        start_urls = [
            {
                "property_type" : "apartment",
                "type" : "flat"
            },
            {
                "property_type" : "house",
                "type" : "house"
            },
        ]
        for item in start_urls:
            formdata = {
                "price[min]": "",
                "price[max]": "8001",
                "status": "for-rent",
                "listing_type": item["type"],
                "bedrooms[min]": "0",
                "bedrooms[max]": "10",
                "bathrooms[min]": "",
                "bathrooms[max]": "",
                "action": "listing_search",
                "_ref": "browser",
                "_ajax_nonce": "9398d20a4f",
                "timestamp": "1610687704548",
            }
            yield FormRequest(
                "http://www.sargeants.london/wp-admin/admin-ajax.php",
                callback=self.parse,
                formdata=formdata,
                #dont_filter=True,
                meta={
                    "property_type":item["property_type"],
                    "type":item["type"]
                })

    # 1. FOLLOWING
    def parse(self, response):
        page = response.meta.get("page", 2)
        seen = False
        xml_data = response.xpath("//*[self::response_data]/text()").getall()
        sel = Selector(text=xml_data[1], type="html")
        for item in sel.xpath("//a[@class='setInfoIcon']/@href").getall():
            follow_url = item
            yield Request(follow_url, callback=self.populate_item, meta={"property_type":response.meta["property_type"]})
        
        if page == 2 or seen:
            p_type = response.meta["type"]
            formdata = {
                "price[min]": "",
                "price[max]": "8001",
                "status": "for-rent",
                "listing_type": p_type,
                "bedrooms[min]": "0",
                "bedrooms[max]": "10",
                "bathrooms[min]": "",
                "bathrooms[max]": "",
                "action": "listing_search",
                "_ref": "browser",
                "_ajax_nonce": "9398d20a4f",
                "paged": str(page),
                "timestamp": "1610687704548",

            }
            url = "http://www.sargeants.london/wp-admin/admin-ajax.php"
            yield FormRequest(
                url,
                callback=self.parse,
                formdata=formdata,
                #dont_filter=True,
                meta={
                    "property_type":response.meta["property_type"],
                    "page":page+1,
                    "type":p_type,
                })
        
    
    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        item_loader.add_value("external_link", response.url)
        item_loader.add_value("property_type", response.meta["property_type"])
        
        item_loader.add_value("external_source", "Sargeants_PySpider_united_kingdom")

        item_loader.add_xpath("title", "//h1/text()")        
        address = response.xpath("//h1/text()").extract_first()
        if address:
            item_loader.add_value("address", address)
        city = response.xpath("//ul/li/span[@class='state']//text()").extract_first()
        if city:
            item_loader.add_value("city", city.strip())

        rent = " ".join(response.xpath("//span[@class='priceHead']//text()").extract())
        if rent:
             item_loader.add_value("rent_string",rent )    

        room_count =response.xpath("//ul[@class='listing-meta hidden-phone']/li[span[contains(.,'bedroom')]]/text()[.!='n/a']").extract_first()    
        if room_count:
            item_loader.add_value("room_count", room_count.strip())
        else:
            room_count =response.xpath("//ul[@class='listing-meta hidden-phone']/li[contains(@class,'icon-text icon-bed')]/text()[normalize-space()][.!='n/a']").extract_first()    
            if room_count and "Studio" in room_count:
                item_loader.add_value("room_count", "1")

        item_loader.add_xpath("bathroom_count","//ul[@class='listing-meta hidden-phone']/li[span[contains(.,'bathroom')]]/text()[normalize-space()][.!='n/a']")
   
        square =response.xpath("//ul[@class='listing-meta hidden-phone']/li[contains(@class,'icon-text icon-area')]/text()[normalize-space()][.!='n/a']").extract_first()    
        if square:     
            if "ft" in square:
                square = square.split("ft")[0].strip()
                square = str(int(float(square) * 0.09290304))
            item_loader.add_value("square_meters", square)

        images = [x for x in response.xpath("//div[@id='single-listing-gallery']//li/div/div[1]/@data-src").extract()]
        if images:
            item_loader.add_value("images", images)
        floor_plan_images = [x for x in response.xpath("//div[@class='dataHold floorplan']//li//img/@src").extract()]
        if floor_plan_images:
            item_loader.add_value("floor_plan_images", floor_plan_images)

        latlng =response.xpath("//iframe[@id='schoolMaps']/@data-latlng").extract_first()    
        if latlng:
            item_loader.add_value("latitude",latlng.split(",")[0].strip())
            item_loader.add_value("longitude",latlng.split(",")[1].strip())
            
        parking =response.xpath("//div[@class='single-listing-description']//li[contains(.,'Parking')]//text()").extract_first()    
        if parking:
            item_loader.add_value("parking", True)
        furnished =response.xpath("//div[@class='single-listing-description']//li[contains(.,'Furnished') or contains(.,'furnished') ]//text()").extract_first()    
        if furnished:
            if "unfurnished" in furnished.lower():
                item_loader.add_value("furnished", False)
            elif "furnished" in furnished.lower():
                item_loader.add_value("furnished", True)
    
        desc = " ".join(response.xpath("//div[@class='single-listing-description']/p//text()").extract())
        if desc:
            item_loader.add_value("description", desc.strip())
        landlord_name =response.xpath("//div[@class='agent-card-info']//h6[@class='agent-card-name']/text()").extract_first()    
        if landlord_name:
            item_loader.add_value("landlord_name",landlord_name.split())
        else:
            item_loader.add_value("landlord_name", "Sargeants")

        item_loader.add_value("landlord_phone", "020 8799 3800")
        item_loader.add_value("landlord_email", "info@sargeants.london")   
        
        yield item_loader.load_item()
