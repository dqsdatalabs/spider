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
    name = 'philipalexander_net'
    execution_type='testing'
    country='united_kingdom'
    locale='en'   
    start_urls = ["https://www.philipalexander.net/search/?showstc=on&instruction_type=Letting&bedrooms=&minpricew=&maxpricew=&n=18"]

    # 1. FOLLOWING
    def parse(self, response):

        page = response.meta.get("page", 2)

        seen = False
        for item in response.xpath("//a[contains(@class,'btn detailsbtn')]/@href").extract():
            follow_url = response.urljoin(item)
            yield Request(follow_url, callback=self.populate_item)
            seen = False
        
        if page == 2 or seen:
            p_url = f"https://www.philipalexander.net/search/{page}.html?showstc=on&instruction_type=Letting&bedrooms=&minpricew=&maxpricew=&n=18"
            yield Request(p_url, callback=self.parse, meta={'page': page+1})
    
    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        status = response.xpath("//div[@class='pdprice']/span[@class='status']/text()").get()
        if status and "Let Agreed" in status:
            return 
        externalid=response.url
        if externalid:
            item_loader.add_value("external_id",externalid.split("details/")[-1].split("/")[0])

        item_loader.add_value("external_link", response.url)
        item_loader.add_value("external_source", "Philipalexander_PySpider_"+ self.country + "_" + self.locale)
        
        prop_type = response.xpath("//p[@class='type']/text()").get()
        if prop_type and ("apartment" in prop_type.lower() or "flat" in prop_type.lower() or "maisonette" in prop_type.lower()):
            item_loader.add_value("property_type", "apartment")
        elif prop_type and "house" in prop_type.lower():
             item_loader.add_value("property_type", "house")
        elif prop_type and "studio" in prop_type.lower():
             item_loader.add_value("property_type", "studio")
        else:
            return
        
        title = response.xpath("//title/text()").get()
        if title:
            item_loader.add_value("title", re.sub('\s{2,}', ' ', title.strip()))

        address = response.xpath("//div[@class='pdname']/h3[2]/text()").get()
        if address:
            zipcode = address.strip().split(" ")[-1]
            city = address.split(zipcode)[0].strip().strip(",").split(",")[-1]
            item_loader.add_value("address", address)
            item_loader.add_value("city", city.strip())
            item_loader.add_value("zipcode", zipcode)
            
        rent = response.xpath("//p[@class='price']/text()").get()
        if rent and "Week" in rent:
                price = str(int(rent.strip().split("£")[1].split(" ")[0])*4)
                item_loader.add_value("rent", price)
        item_loader.add_value("currency", "GBP")
        
        available_date = response.xpath("//p[@class='availability']//text()").get()
        if available_date:
            available_date = available_date.split("from")[1].strip()
            date_parsed = dateparser.parse(
                        available_date, date_formats=["%d/%m/%Y"]
                    )
            if date_parsed:
                date2 = date_parsed.strftime("%Y-%m-%d")
                item_loader.add_value("available_date", date2)
                
        room_count = response.xpath("//ul/li[@class='bedrooms']/text()").get()
        room = response.xpath("//ul/li[@class='receptions']/text()").get()
        if room_count:
            item_loader.add_value("room_count", room_count)
        elif room:
            item_loader.add_value("room_count", room)
        
        bathroom_count = response.xpath("//ul/li[@class='bathrooms']/text()").get()
        if bathroom_count:
            item_loader.add_value("bathroom_count", bathroom_count)
        
        desc = "".join(response.xpath("//div[@class='pddescription']//text()").getall())
        if desc:
            item_loader.add_value("description", re.sub('\s{2,}', ' ', desc.strip()))
        
        images = [ x for x in response.xpath("//div[@id='full-width-slider']//img/@src").getall()]
        if images:
            item_loader.add_value("images", images)

        floor_plans = [ response.urljoin(x) for x in response.xpath("//li[@class='floorplans']/a/@href").getall()]
        if floor_plans:
            item_loader.add_value("floor_plan_images", floor_plans)
        
        floor = response.xpath("//ul[@class='property-features']/li//text()[contains(.,'floor') or contains(.,'Floor')]").get()
        if floor:
            floor = floor.lower().split("floor")[0].strip()
            item_loader.add_value("floor", floor.capitalize())
        
        lat_lng = response.xpath("//script[contains(.,'PropertyMap.renderStreetview')]/text()").get()
        if lat_lng:
            lat = lat_lng.split("PropertyMap.renderStreetview(svopt,")[1].split(",")[0].strip()
            lng = lat_lng.split("PropertyMap.renderStreetview(svopt,")[1].split(",")[1].split(",")[0].strip()
            item_loader.add_value("latitude", lat)
            item_loader.add_value("longitude", lng)
        
        furnished = response.xpath("//ul[@class='property-features']/li//text()[contains(.,'furnished') or contains(.,'Furnished')]").get()
        unfurnished = response.xpath("//ul[@class='property-features']/li//text()[contains(.,'unfurnished') or contains(.,'Unfurnished')]").get()
        if unfurnished:
            item_loader.add_value("furnished", False)
        elif furnished:
            if "Part" in furnished:
                item_loader.add_value("furnished", False)
            else:
                item_loader.add_value("furnished", True)
            
        balcony = response.xpath("//ul[@class='property-features']/li//text()[contains(.,'balcon') or contains(.,'Balcon')]").get()
        if balcony:
            item_loader.add_value("balcony", True)
        
        terrace = response.xpath("//ul[@class='property-features']/li//text()[contains(.,'terrace') or contains(.,'Terrace')]").get()
        if terrace:
            item_loader.add_value("terrace", True)
        
        parking = response.xpath("//ul[@class='property-features']/li//text()[contains(.,'parking') or contains(.,'Parking')]").get()
        if parking:
            item_loader.add_value("parking", True)
        
        item_loader.add_value("landlord_name", "PHILIP ALEXANDER")
        
        phone = response.xpath("//p[@class='tel']/text()").get()
        if phone:
            item_loader.add_value("landlord_phone", phone)
        
        item_loader.add_value("landlord_email", "info@philipalexander.net")
        

        yield item_loader.load_item()
