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
    name = 'fhres_co_uk'
    execution_type = 'testing'
    country = 'united_kingdom'
    locale = 'en'
    external_source="Fhres_Co_PySpider_united_kingdom"
    
     
    def start_requests(self):
        form_data={
            "sortorder": "price-desc",
            "RPP": "12",
            "OrganisationId": "56662b8d-9259-40d2-a75f-e57495dbef2a",
            "WebdadiSubTypeName": "Rentals",
            "Status": "{2a50fde6-8f09-4d01-9514-7a856e206d04},{e9617465-c405-4b6a-abc9-fdbfc499145c},{59c95297-2dca-4b55-9c10-220a8d1a5bed}",
            "includeSoldButton": "true",
            "incsold": "true",

        }
        url = "https://www.fhres.co.uk/api/set/results/grid"
        yield FormRequest(
            url,
            callback=self.parse,
            formdata=form_data,
        )

    # 1. FOLLOWING
    def parse(self, response):
        page = response.meta.get("page", 2)
        seen = False
        for item in response.xpath("//a[@class='property-description-link']/@href").getall():
            follow_url = response.urljoin(item)
            yield Request(follow_url, callback=self.populate_item)
            seen = True
         
        if page == 2 or seen:
            form_data={
                "sortorder": "price-desc",
                "RPP": "12",
                "OrganisationId": "56662b8d-9259-40d2-a75f-e57495dbef2a",
                "WebdadiSubTypeName": "Rentals",
                "Status": "{2a50fde6-8f09-4d01-9514-7a856e206d04},{e9617465-c405-4b6a-abc9-fdbfc499145c},{59c95297-2dca-4b55-9c10-220a8d1a5bed}",
                "includeSoldButton": "true",
                "page":str(page),
                "incsold": "true",

            } 

            yield FormRequest(
                "https://www.fhres.co.uk/api/set/results/grid",
                callback=self.parse,
                formdata=form_data,
               
            )

    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)
        item_loader.add_value("external_source", self.external_source)
        item_loader.add_value("external_link", response.url)
        f_text =response.xpath("//h2[@class='color-primary mobile-left']/text()").get()
        if f_text:
            if "flat" in f_text.lower():
                item_loader.add_value("property_type","apartment")
        if f_text:
            if "house" in f_text.lower():
                item_loader.add_value("property_type","house")
        
        
        title=response.xpath("//title//text()").get()
        if title:
            item_loader.add_value("title", title)
        
        city = response.xpath("//span[@class='city']/text()").get()
        if city:
            item_loader.add_value("city", city.replace(",","").strip())
        county=response.xpath("//span[@class='county']/text()").get()
        if not county:
            county=""
        zipcode=response.xpath("//span[@class='displayPostCode']/text()").get()
        if zipcode:
            item_loader.add_value("zipcode", zipcode)
        address=item_loader.get_output_value("city")+" "+county+item_loader.get_output_value("zipcode")
        if address:
            item_loader.add_value("address",address)

        rent =response.xpath("//span[@class='nativecurrencyvalue']/text()").get()
        if rent:
            price = rent.replace(",","").strip()
            item_loader.add_value("rent_string", price)
            item_loader.add_value("currency","EUR")
        
        room_count = response.xpath("//ul[@class='FeaturedProperty__list-stats']//li[1]/span/text()").get()
        if room_count and not "0"==room_count:
            item_loader.add_value("room_count", room_count)
            
        bathroom_count = response.xpath("//ul[@class='FeaturedProperty__list-stats']//li[2]/span/text()").get()
        if bathroom_count:
            item_loader.add_value("bathroom_count", bathroom_count)
        
        # square_meters = response.xpath("//div[contains(@class,'property-feature')]//div//text()[contains(.,'SQ FT')]").get()
        # if square_meters:
        #     square_meters = square_meters.split("SQ FT")[0].strip().split(" ")[-1].replace(",","")
        #     sqm = str(int(int(square_meters)* 0.09290304))
        #     item_loader.add_value("square_meters", sqm)
        
        desc = " ".join(response.xpath("//section[@id='description']//p//text()").getall())
        if desc:
            desc = re.sub('\s{2,}', ' ', desc.strip())
            item_loader.add_value("description", desc)
        
        images = [response.urljoin(x.split(": url(")[-1].replace(")","")) for x in response.xpath("//div[@class='image-gallery']//div[@class='owl-image']/@style").getall()]
        if images:
            item_loader.add_value("images", images)
        
        floor_plan_images = [x for x in response.xpath("//img[@title='floorplan']/@data-src").getall()]
        if floor_plan_images:
            item_loader.add_value("floor_plan_images", floor_plan_images)
        
        external_id = response.url
        if external_id:
            item_loader.add_value("external_id", external_id.split("property/")[-1].split("/")[0])
        
        # floor = response.xpath(
        #     "//div[contains(@class,'property-feature')]//div//text()[contains(.,'FLOOR') or contains(.,'Floor') or contains(.,'floor')]").get()
        # if floor:
        #     item_loader.add_value("floor", floor.split("FLOOR")[0].strip())
        
        latitude_longitude = response.xpath("//section[@id='maps']/@data-cords").get()
        if latitude_longitude:
            latitude = latitude_longitude.split("lat")[-1].split(",")[0].split(":")[-1].replace('"',"")
            longitude = latitude_longitude.split("lng")[-1].split(",")[0].split("-")[-1].split("}")[0].replace('"',"").strip()      
            item_loader.add_value("longitude", longitude)
            item_loader.add_value("latitude", latitude)
        
        # unfurnished = response.xpath("//div[contains(@class,'property-feature')]//div//text()[contains(.,'Unfurnished')]").get()
        # if unfurnished:
        #     item_loader.add_value("furnished", False)
        
        terrace = response.xpath("//li[contains(.,'GARDEN')]/text()").get() 
        if terrace:
            item_loader.add_value("terrace", True)
        parking = response.xpath("//li[contains(.,'PARKING')]/text()").get() 
        if parking:
            item_loader.add_value("parking", True)
        
        # from datetime import datetime
        # available_date = response.xpath(
        #     "//div[contains(@class,'property-feature')]//div//text()[contains(.,'AVAILABLE')]").get()
        # if available_date:
        #     if "IMMEDIATELY" in available_date:
        #         item_loader.add_value("available_date", datetime.now().strftime("%Y-%m-%d"))
        
        
        item_loader.add_value("landlord_name", "FIELDHOUSE")
        item_loader.add_value("landlord_phone", "44 (0)20 7013 0770 ")
        item_loader.add_value("landlord_email", "battersea@fhres.co.uk")
        
        yield item_loader.load_item()


