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
from word2number import w2n

class MySpider(Spider):
    name = 'netlettings_com'
    execution_type='testing'
    country='united_kingdom'
    locale='en'
    def start_requests(self):
        start_urls = [
            {
                "url" : [
                    "https://netlettings.com/search/?instruction_type=Letting&address_keyword=&minprice=&maxprice=&property_type=Penthouse",
                    "https://netlettings.com/search/?instruction_type=Letting&address_keyword=&minprice=&maxprice=&property_type=Flat",
                    "https://netlettings.com/search/?instruction_type=Letting&address_keyword=&minprice=&maxprice=&property_type=Apartment",
                ],  
                "property_type" : "apartment"
            },
            {
                "url" : [
                    "https://netlettings.com/search/?instruction_type=Letting&address_keyword=&minprice=&maxprice=&property_type=Mid+Terraced+House",
                    "https://netlettings.com/search/?instruction_type=Letting&address_keyword=&minprice=&maxprice=&property_type=Ground+Floor+Maisonette"
                ],
                "property_type" : "house"
            },
            {
                "url" : [
                    "https://netlettings.com/search/?instruction_type=Letting&address_keyword=&minprice=&maxprice=&property_type=Studio",
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
        for item in response.xpath("//img[@class='img-responsive']/../@href").extract():
            follow_url = response.urljoin(item)
            yield Request(follow_url, callback=self.populate_item, meta={'property_type': response.meta.get('property_type')})
            seen = True
        
        if page == 2 or seen:
            p_url = response.url.split("search/")[0] + f"search/{page}.html?" + response.url.split("?")[1]
            yield Request(p_url, callback=self.parse, meta={'property_type': response.meta.get('property_type'), "page":page+1})
    
    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)
        
        item_loader.add_value("property_type", response.meta.get('property_type'))
        item_loader.add_value("external_link", response.url)
        item_loader.add_value("external_source","Netlettings_PySpider_"+ self.country)
        
        title = response.xpath("//title/text()").get()
        if title:
            item_loader.add_value("title", title)
        
        address = response.xpath("//div/ol/parent::div/h2//text()").get()
        if address:
            item_loader.add_value("address", address)
            
            zipcode = address.split(" ")[-1]
            
            city = address.split(zipcode)[0].strip().strip(",")
            if "," in city:
                item_loader.add_value("city", city.split(",")[-1])
            else:
                item_loader.add_value("city", city)
                
            item_loader.add_value("zipcode", zipcode)
        
        rent = response.xpath("//div/ol/parent::div/p/text()").get()
        if rent:
            price = rent.split("£")[1].replace(",","").strip()
            if price != "0":
                item_loader.add_value("rent", price)
        item_loader.add_value("currency", "GBP")
        
        room_count = response.xpath("//div/ol/li[contains(.,'Bedroom')]//span/text()").get()
        if room_count:
            item_loader.add_value("room_count", room_count.strip().split(" ")[0])
        elif response.meta.get("property_type") == "studio":
            item_loader.add_value("room_count", "1")
            
        bathroom = response.xpath("//div/ul/li[contains(.,'bathroom')]//text()").get()
        if bathroom:
            bathroom = bathroom.strip().split(" ")[0]
            try:
                item_loader.add_value("bathroom_count", w2n.word_to_num(bathroom))
            except: pass
            
        desc = "".join(response.xpath("//div[@id='property-short-description']//text()").getall())
        if desc:
            item_loader.add_value("description", desc)
        
        if "sq ft" in desc:
            square_meters = desc.split("sq ft")[0].strip().split(" ")[-1]
            sqm = str(int(int(square_meters)* 0.09290304))
            item_loader.add_value("square_meters", sqm)
        
        if "floor" in desc:
            floor = desc.split("floor")[0].strip().split(" ")[-1]
            if "first" in floor:
                item_loader.add_value("floor", "1")
            else:
                floor = floor.replace("st","").replace("nd","").replace("rd","").replace("th","")
                if floor.isdigit():
                    item_loader.add_value("floor", floor)
                    
        
        images = [ x for x in response.xpath("//div[@class='carousel-inner']//img/@src").getall()]
        if images:
            item_loader.add_value("images", images)
        
        lat_lng = response.xpath("//script[contains(.,'lng')]/text()").get()
        if lat_lng:
            lat_lng = lat_lng.split("googlemap")[1]
            lat = lat_lng.split("&q=")[1].split("%2C")[0]
            lng = lat_lng.split("&q=")[1].split("%2C")[1].split('"')[0]
            item_loader.add_value("latitude", lat)
            item_loader.add_value("longitude", lng)
        
        floor_plan_images = response.xpath("//div[@id='property-floorplans']/img/@src").get()
        if floor_plan_images:
            item_loader.add_value("floor_plan_images", response.urljoin(floor_plan_images))
        
        terrace = response.xpath("//div/ul/li[contains(.,'Terrace')]//text()").get()
        if terrace:
            item_loader.add_value("terrace", True)
        
        furnished = response.xpath("//div/ul/li[contains(.,'Furnished')]//text()").get()
        if furnished:
            item_loader.add_value("furnished", True)
        
        balcony = response.xpath("//div/ul/li[contains(.,'Balcony')]//text()").get()
        if balcony:
            item_loader.add_value("balcony", True)
            
        parking = response.xpath("//div/ul/li[contains(.,'Parking')]//text()").get()
        if parking:
            item_loader.add_value("parking", True)
            
        item_loader.add_value("landlord_name", "NET LETTINGS")
            
        phone = response.xpath("//p/a/@href[contains(.,'tel')]").get()
        if phone:
            item_loader.add_value("landlord_phone", phone.split("tel:")[1])
            
        item_loader.add_value("landlord_email", "info@netlettings.com")

        if not item_loader.get_collected_values("bathroom_count"):
            brochure = response.xpath("//a[contains(.,'Brochure')]/@href").get()
            if brochure:
                yield Request(response.urljoin(brochure), callback=self.get_bathroom, meta={"item_loader": item_loader})
        else:      
            yield item_loader.load_item()

    def get_bathroom(self, response):

        item_loader = response.meta.get("item_loader")
        bathroom_count = response.xpath("//p[@class='rooms']/text()").get()
        if bathroom_count:
            item_loader.add_value("bathroom_count", bathroom_count.lower().split('bathroom')[0].split('|')[-1].strip())
        
        yield item_loader.load_item()
