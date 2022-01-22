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
    name = 'excel_property_co_uk'    
    execution_type='testing'
    country='united_kingdom'
    locale='en'
    thousand_separator = ','
    scale_separator = '.' 
    external_source = "Excel_Property_Co_PySpider_united_kingdom"
    start_urls = ["https://excel-property.co.uk/property-results-test/?address_keyword=&minimum_bedrooms=&minimum_rent=&maximum_rent=&department=residential-lettings"]

    # 1. FOLLOWING
    def parse(self, response):
        for item in response.xpath("//h2/a/@href").getall():
            # if item.xpath("./../h6/span/text()").get() and item.xpath("./../h6/span/text()").get().strip().lower() in ["let agreed"]:
            #     continue
            follow_url = response.urljoin(item)
            yield Request(follow_url, callback=self.populate_item)
       
        next_page = response.xpath("//a[@class='next page-numbers']/@href").get()
        if next_page:
            yield Request(response.urljoin(next_page), callback=self.parse)
    
    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)
      
        item_loader.add_value("external_link", response.url)
        item_loader.add_xpath("external_id", "substring-after(//link[@rel='shortlink']/@href,'?p=')")
        summary = "".join(response.xpath("//div[@class='summary-contents']/text()").get())
        if summary and ("apartment" in summary.lower() or "flat" in summary.lower() or "maisonette" in summary.lower()):
            item_loader.add_value("property_type", "apartment")
        elif summary and "house" in summary.lower():
             item_loader.add_value("property_type", "house")
        elif summary and "studio" in summary.lower():
             item_loader.add_value("property_type", "studio")
        else:
            return

        item_loader.add_value("external_source", "Excel_Property_Co_PySpider_united_kingdom")
        
        address = response.xpath("substring-before(//title/text(),'- Excel Property UK')").get()
        if address:
            item_loader.add_value("address", address.strip())
            item_loader.add_value("city", address.split(',')[-2])
            zipcode1= address.split(',')[-1].split("-")[0].strip().split(" ")[-1]
            zipcode2= address.split(',')[-1].split("-")[0].strip().split(" ")[-2]
            zipcode=zipcode2+" "+zipcode1
            item_loader.add_value("zipcode",zipcode)
            
        title = response.xpath("//title/text()").get()
        if title:
            item_loader.add_value("title", title.strip().replace('\xa0', ''))

        description = " ".join(response.xpath("//h4[contains(.,'Full Details')]/following-sibling::*//text()").getall()).strip()
        if description:
            item_loader.add_value("description", description.replace('\xa0', ''))
        else:
            description = " ".join(response.xpath("//div[@class='summary-contents']//text()").getall())
            if description:
                item_loader.add_value("description", description.replace('\xa0', '').strip())

        
        square_meters = response.xpath("//li[contains(text(),'sq m)')]/text()").get()
        if square_meters:
            item_loader.add_value("square_meters", str(int(float(square_meters.split('(')[-1].split('sq m')[0].strip()))))

        room_count = response.xpath("//span[contains(text(),'Bedrooms')]/following-sibling::text()").get()
        if room_count:
            item_loader.add_value("room_count", room_count.strip())
        
        bathroom_count = response.xpath("//span[contains(text(),'Bathrooms')]/following-sibling::text()").get()
        if bathroom_count:
            item_loader.add_value("bathroom_count", bathroom_count.strip())

        floor = "".join(response.xpath("//ul[@class='propfeatures col span_6']/li[contains(.,'floor') or contains(.,'Floor')]/text()").getall())
        if floor:
            item_loader.add_value("floor", floor.strip().split("floor")[0].replace("Wooden","").replace("Wood","").replace("Occupying entire","").replace("Iconic development;","").replace("Maisonette set over","").replace("Townhouse on","").replace("strip",""))
        rent = response.xpath("//div[@class='price']/span/text()").get()
        if rent:
            item_loader.add_value("rent_string", rent)
        else:
            rent = response.xpath("//div[@class='price']/text()").get()
            if rent:
                rent = rent.split('£')[-1].lower().split('pw')[0].strip().replace(',', '').replace('\xa0', '')
                if "pcm" in rent:
                    item_loader.add_value("rent", rent)
                else:
                    
                    item_loader.add_value("rent", int(float(rent))*4)
        item_loader.add_value("currency", 'GBP')
        
        images = [response.urljoin(x) for x in response.xpath("//img[@class='attachment-thumbnail size-thumbnail']/@src").getall()]
        if images:
            item_loader.add_value("images", images)
            item_loader.add_value("external_images_count", len(images))

        floor_plan_images = [x for x in response.xpath("//img[contains(@src,'FLP')]/@src").getall()]
        if floor_plan_images:
            item_loader.add_value("floor_plan_images", floor_plan_images)
        
        latitude_longitude = response.xpath("//script[contains(.,'LatLng')]/text()").get()
        if latitude_longitude:
            item_loader.add_value("latitude", latitude_longitude.split('LatLng(')[1].split(',')[0].strip())
            item_loader.add_value("longitude", latitude_longitude.split('LatLng(')[1].split(',')[1].split(')')[0].strip())
        
        floor = response.xpath("//li[contains(.,' floor')]/text()").get()
        if floor:
            item_loader.add_value("floor", "".join(filter(str.isnumeric, floor.split('floor')[0])).strip())
        
        parking = response.xpath("//li[contains(.,'parking') or contains(.,'Parking')]").get()
        if parking:
            item_loader.add_value("parking", True)
        
        elevator = response.xpath("//li[contains(.,'lift') or contains(.,'Lift')]").get()
        if elevator:
            item_loader.add_value("elevator", True)

        item_loader.add_value("landlord_phone", '020 7691 9000')
        item_loader.add_value("landlord_email", 'info@excel-property.co.uk')
        item_loader.add_value("landlord_name", 'Excel Property')

        yield item_loader.load_item()
