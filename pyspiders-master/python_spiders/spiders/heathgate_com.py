# -*- coding: utf-8 -*-
# Author: Mehmet Kurtipek

from scrapy.loader.processors import MapCompose
from scrapy import Spider
from scrapy import Request,FormRequest
from scrapy.selector import Selector
from w3lib.html import remove_tags
from python_spiders.loaders import ListingLoader
import json
from urllib.parse import urljoin


class MySpider(Spider):
    name = 'heathgate_com'
    execution_type='testing'
    country='united_kingdom'
    locale='en' 
    start_urls = ["http://www.heathgate.com/?s=&status=rent&orderby=price&order=asc"]
    
    # 1. FOLLOWING
    def parse(self, response):
        
        for item in response.xpath("//div[@class='moretag-wrap']/a/@href").extract():
            f_url = response.urljoin(item)
            yield Request(
                f_url, 
                callback=self.populate_item, 
            )
        
        next_page = response.xpath("//a[contains(@class,'next')]/@href").get()
        if next_page:
            yield Request(
                url=response.urljoin(next_page),
                callback=self.parse,
            )
        
    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)
    
        item_loader.add_value("external_link", response.url)

        title = response.xpath("//h1[@class='entry-title']/text()").get()
        if title and self.get_prop_type(title):
            item_loader.add_value("property_type", self.get_prop_type(title))
        else:
            desc = response.xpath("//div[contains(@class,'description')]/p/text()").get()
            if desc and self.get_prop_type(desc):
                item_loader.add_value("property_type", self.get_prop_type(desc))
            else:
                return

        item_loader.add_value("external_source", "Heathgate_PySpider_" + self.country + "_" + self.locale)
      
        title = response.xpath("//div/h1[contains(@class,'entry-title')]//text()").extract_first()
        if title:
            item_loader.add_value("title", title)
            item_loader.add_value("address",title)
            item_loader.add_value("zipcode",title.split(" ")[-1].strip())
        city = response.xpath("//div[@class='title-listing-location']//text()").extract_first()
        if city:
            item_loader.add_value("city", city)
        desc = "".join(response.xpath("//div[contains(@class,'listing-description')]/p//text()").extract())
        if desc:
            item_loader.add_value("description", desc.strip())
            if "furniture" in desc.lower():
                item_loader.add_value("furnished", True)
           
        rent = "".join(response.xpath("//div[contains(@class,'listing-price')]//text()[normalize-space()]").getall()).strip()
        if rent and rent.lower() != 'rented':
            if "week" in rent:
                rent = str(int(float(rent.split('£')[-1].split('/')[0].strip().replace(',', ''))) * 4)
                item_loader.add_value("rent", rent)  
                item_loader.add_value("currency","GBP")
            elif 'month' in rent:
                item_loader.add_value("rent", rent.split('£')[-1].split('/')[0].strip().replace(',', ''))                 
                item_loader.add_value("currency","GBP")
        else:
            return
                   
        bathroom_count = response.xpath("//div/span[contains(.,'Bathroom')]/following-sibling::span//text()").get()
        if bathroom_count:
            item_loader.add_value("bathroom_count", bathroom_count.strip())
        
        pets_allowed = response.xpath("//a[contains(.,'Pets Allowed')]").get()
        if pets_allowed:
            item_loader.add_value("pets_allowed", True)
        
        floor = response.xpath("//a[contains(.,'Floor ')]/text()").get()
        if floor:
            floor = "".join(filter(str.isnumeric, floor.lower().split('floor')[0].strip())).strip()
            if floor.isnumeric():
                item_loader.add_value("floor", floor)
            
        elevator = response.xpath("//a[contains(.,'Lift')]").get()
        if elevator:
            item_loader.add_value("elevator", True)
        
        washing_machine = response.xpath("//a[contains(.,'Washing Machine')]").get()
        if washing_machine:
            item_loader.add_value("washing_machine", True)
        
        item_loader.add_xpath("external_id", "//div/span[contains(.,'Listing ID')]/following-sibling::span//text()")  
        
        room_count = response.xpath("//div/span[contains(.,'Bedroom')]/following-sibling::span//text()").extract_first()
        if room_count:
            item_loader.add_value("room_count", room_count.strip()) 
        
        square_meters = response.xpath("//div/span[contains(.,'Size')]/following-sibling::span//text()").extract_first()
        if not square_meters:
            square_meters = response.xpath("//div[@id='wpsight-listing-features-2']//li[contains(.,'sq') or contains(.,'Sq')]//text()").extract_first()
        if square_meters:
            try:
                square_meters = square_meters.lower().split('sq')[0].strip().replace(',', '')
                sqm = str(int(float(square_meters)* 0.09290304))
                item_loader.add_value("square_meters", sqm)
            except:
                pass
        swimming_pool = response.xpath("//li[contains(.,'Swimming Pool')]//text()").get()
        if swimming_pool:
            item_loader.add_value("swimming_pool", True)       

        balcony = response.xpath("//li[contains(.,'Balcon')]//text()").get()
        if balcony:
            item_loader.add_value("balcony", True)   

        furnished = response.xpath("//ul[@class='clearfix']/li/a[contains(.,'furnished')]").get()
        if furnished:
            if "Unfurnished" not in  furnished:
                item_loader.add_value("furnished", True)
            else:
                item_loader.add_value("furnished", False)


        parking = response.xpath("//li[contains(.,'Parking')]//text()").get()
        if parking:
            item_loader.add_value("parking", True) 

        terrace = response.xpath("//li[contains(.,'Terrace')]//text()").get()
        if terrace:
            item_loader.add_value("terrace", True) 
      
        coordinate = response.xpath("//script[@type='text/javascript'][contains(.,'latLng')]//text()").extract_first()
        if coordinate:
            coordinate=coordinate.split("latLng:[")[1].split("],")[0]
            latitude = coordinate.split(',')[0].strip()
            longitude = coordinate.split(',')[1].strip()        
            item_loader.add_value("latitude", latitude)
            item_loader.add_value("longitude", longitude)

        images = [x for x in response.xpath("//ul[@class='slides']//img/@src").extract()]
        if images:
            item_loader.add_value("images", images)
            item_loader.add_value("external_images_count", len(images))

        floor_plan_images = [x for x in response.xpath("//div/a[contains(.,'Floorplan')]/@href").extract()]
        if floor_plan_images:
            item_loader.add_value("floor_plan_images", floor_plan_images)

        item_loader.add_value("landlord_name", "Heathgate")
        item_loader.add_value("landlord_phone", "020 7435 3344")
    
        yield item_loader.load_item()
    
    def get_prop_type(self, prop_str):
        if "apartment" in prop_str.lower() or "flat" in prop_str.lower() or "studio" in prop_str.lower():
            prop_type = "apartment"
        elif "house" in prop_str.lower():
            prop_type = "house"
        else:
            prop_type = None

        return prop_type
 
