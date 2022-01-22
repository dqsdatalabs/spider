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
import re


class MySpider(Spider):
    name = 'rawlinsestates_co_uk'
    execution_type='testing'
    country='united_kingdom'
    locale='en'
    thousand_separator = ','
    scale_separator = '.'
    def start_requests(self):

        start_urls = [
            {
                "url" : "https://www.rawlinsestates.co.uk/?action=epl_search&post_type=rental&property_category=Flat",
                "property_type" : "apartment"
            },
            {
                "url" : "https://www.rawlinsestates.co.uk/?action=epl_search&post_type=rental&property_category=Maisonette",
                "property_type" : "house"
            },
            {
                "url" : "https://www.rawlinsestates.co.uk/?action=epl_search&post_type=rental&property_category=Semi-detached",
                "property_type" : "house"
            },
            {
                "url" : "https://www.rawlinsestates.co.uk/?action=epl_search&post_type=rental&property_category=Cottage",
                "property_type" : "house"
            },
            {
                "url" : "https://www.rawlinsestates.co.uk/?action=epl_search&post_type=rental&property_category=Studio",
                "property_type" : "studio"
            },
            {
                "url" : "https://www.rawlinsestates.co.uk/?action=epl_search&post_type=rental&property_category=House",
                "property_type" : "house"
            },
            {
                "url" : "https://www.rawlinsestates.co.uk/?action=epl_search&post_type=rental&property_category=Apartment",
                "property_type" : "apartment"
            },
            {
                "url" : "https://www.rawlinsestates.co.uk/?action=epl_search&post_type=rental&property_category=Townhouse",
                "property_type" : "house"
            },
        ] #LEVEL-1

        for url in start_urls:
            yield Request(url=url.get('url'),
                                 callback=self.parse,
                                 meta={'property_type': url.get('property_type')})


    # 1. FOLLOWING
    def parse(self, response):

        for item in response.xpath("//h3[@class='entry-title']/a/@href").extract():
            f_url = response.urljoin(item)
            yield Request(
                f_url, 
                callback=self.populate_item, 
                meta={"property_type" : response.meta.get("property_type")},
            )
        
        next_page = response.xpath("//a[@rel='next']/@href").get()
        if next_page:
            yield Request(
                url=response.urljoin(next_page),
                callback=self.parse,
                meta={"property_type" : response.meta.get("property_type")},
            )
        

    
    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)
        property_type = response.meta.get('property_type')
        
        item_loader.add_value("external_link", response.url)
      
        type_studio=response.xpath("//div[@class='epl-excerpt-content']//text()[contains(.,'Studio')]").extract_first()
        if type_studio:
            property_type = "studio"
      
        item_loader.add_value("property_type",property_type)
        item_loader.add_value("external_source", "Rawlinsestates_PySpider_"+ self.country + "_" + self.locale)
        
        zipcode=response.xpath("//span[@class='item-pcode']//text()").extract_first()
        if zipcode:
            item_loader.add_value("zipcode",zipcode.strip())
        title=response.xpath("//div[contains(@class,'epl-property-details')]/text()[normalize-space()]").extract_first()
        if title:
            item_loader.add_value("title",title)
            item_loader.add_value("address",title)
            try:
                if zipcode:
                    zipcode = zipcode.split(" ")[0]
                    city = title.split(",")[-1]
                    if city.strip() == zipcode:
                        city = title.split(",")[-2]
                    else:
                        city = city.replace(zipcode,"").strip()
                    item_loader.add_value("city",city.strip())
            except:
                pass
        price = response.xpath("//span[@class='page-price-rent']//text()").extract_first()
        if price:
            if "." in price:
                numbers = re.findall(r'\d+(?:\.\d+)?', price.replace(",",""))
                if numbers:
                    rent = int(float(numbers[0]))
                    item_loader.add_value("rent", rent)
                    item_loader.add_value("currency", "GBP")
            else:
                item_loader.add_value("rent_string", price)
           
     
        room = response.xpath("//span[@title='Bedrooms']//text()").extract_first()
        if room:
            item_loader.add_value("room_count", room)
        else:
            room = response.xpath("//div[@class='epl-excerpt-content']//p//text()").extract_first()
            room_count = roomCount(room)
            item_loader.add_value("room_count", room_count)

        desc ="".join(response.xpath("//div[h5[contains(.,'Description')]]//p/text()").extract())    
        if desc:
            item_loader.add_value("description", desc.replace("\n",""))
        if "washing machine" in desc.lower():
            item_loader.add_value("washing_machine", True)
        
        square_meters = response.xpath("//div[h5[contains(.,'Description')]]//p/text()[contains(.,'Approximately')]").get()
        if square_meters:
            square_meters = square_meters.split("sq")[0].strip().split(" ")[-1].replace("(","")
            item_loader.add_value("square_meters", square_meters)

        from datetime import datetime
        import dateparser
        available_date = response.xpath("//div[h5[contains(.,'Description')]]//p/text()[contains(.,'Available')]").get()
        if available_date:
            available_date = available_date.split("Available")[1]
            if not ("now" in available_date.lower() or "immediately" in available_date.lower()):
                available_date = available_date.split(".")[0].strip()
                if "from" in available_date.lower():
                    available_date = available_date.split("from")[1].strip()
                elif "–" in available_date.lower():
                    available_date = available_date.split("–")[0].strip()
                else:
                    available_date = available_date.replace("beginning","").replace("early","").replace("end","").strip()
                date_parsed = dateparser.parse(available_date.strip(), date_formats=["%d/%m/%Y"])
                if date_parsed:
                    date2 = date_parsed.strftime("%Y-%m-%d")
                    item_loader.add_value("available_date", date2)
                                   
        
        parking = response.xpath("//div[contains(@class,'epl-tab-wrapper')]//li[contains(.,'parking')]//text()").extract_first()
        if parking:
            item_loader.add_value("parking", True)
        
        furnished = response.xpath("//ul//li[contains(.,'Furnished')]//text()[not(contains(.,'Unfurnished') or contains(.,'unfurnished'))]").extract_first()
        if furnished:
            if "furnished" in furnished.lower():
                item_loader.add_value("furnished", True)
            if "unfurnished" in furnished:
                item_loader.add_value("furnished", False)
            
    
        images = [x for x in response.xpath("//dl[@class='gallery-item']//a/@href").extract()]
        if images is not None:
            item_loader.add_value("images", images)      

        item_loader.add_value("landlord_phone", "020 8371 0033")
        item_loader.add_value("landlord_email", "info@rawlinsestates.co.uk")
        item_loader.add_value("landlord_name", "Rawlins Estates")
        
        price_status = response.xpath("//span[contains(@class,'sold-status')]//text()").extract_first()

        if not price_status:
            yield item_loader.load_item()
        
def roomCount(room):                                                    
    if "Bed" in room:
        try:
            room_count = room.lower().split('bed')[0].strip().replace('\xa0', '').split(' ')[-1].strip()    
            if room_count.isdigit():
                return room_count
            elif "one" in room_count: 
                return "1"
        except:
            pass               
    elif "studio" in room.lower():
        return "1"
    elif "room" in room.lower():
        try:
            room_count=room.lower().split("room")[0].strip().replace('\xa0', '')
            if room_count.isdigit():
                return room_count
            elif "one" in room_count: 
                return "1"
        except: pass