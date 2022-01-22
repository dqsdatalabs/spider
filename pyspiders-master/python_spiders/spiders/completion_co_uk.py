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
    name = 'completion_co_uk'    
    execution_type='testing'
    country='united_kingdom'
    locale='en'
    start_urls = ["https://www.completion.co.uk/properties-to-let"]

    # 1. FOLLOWING
    def parse(self, response):

        for item in response.xpath("//div[@class='eapow-property-thumb-holder']/a/@href").extract():
            follow_url = response.urljoin(item)
            yield Request(follow_url, callback=self.populate_item)
        
        next_page = response.xpath("//a[.='Next']/@href").get()
        if next_page:
            yield Request(response.urljoin(next_page), callback=self.parse)
    
    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        item_loader.add_value("external_link", response.url)

        desc = "".join(response.xpath("//div[@id='propdescription']//p/text()").getall())
        if desc and ("apartment" in desc.lower() or "flat" in desc.lower() or "maisonette" in desc.lower()):
            item_loader.add_value("property_type", "apartment")
        elif desc and "house" in desc.lower():
             item_loader.add_value("property_type", "house")
        elif desc and "studio" in desc.lower():
             item_loader.add_value("property_type", "studio")
        else:
            return

        item_loader.add_value("external_source", "Completion_PySpider_"+ self.country + "_" + self.locale)

        external_id = response.xpath("//div/div[@class='eapow-sidecol'][contains(.,'Ref')]/text()").extract_first()     
        if external_id:   
            item_loader.add_value("external_id",external_id.replace(":","").strip()) 
        title = response.xpath("//div/h1/text()[normalize-space()]").extract_first()     
        if title:   
            item_loader.add_value("title",title.strip()) 
        address =", ".join(response.xpath("//div[contains(@class,'eapow-mainaddress')]/address//text()").extract())
        if address:   
            item_loader.add_value("address",address.strip()) 
        city = response.xpath("//div[contains(@class,'eapow-mainaddress')]/address/text()").extract_first()     
        if city:   
            zipcode = city.split(" ")[-2]+ " "+city.split(" ")[-1]
            item_loader.add_value("city",city.replace(zipcode,"").strip()) 
            item_loader.add_value("zipcode",zipcode.strip()) 
        
        rent = response.xpath("//div//small[@class='eapow-detail-price']//text()").extract_first()
        if rent:
            item_loader.add_value("rent_string", rent.replace(",","."))
        room = response.xpath("//div[@class='room-icon']/i[@class='flaticon-bed']/following-sibling::span/text()[.!='0']").extract_first()
        if room:
            item_loader.add_value("room_count", room.strip())
        if not room:
            room = response.xpath("//div[@class='starItems']//li[contains(.,'STUDIO')]/text()").extract_first()
            if room:
                item_loader.add_value("room_count", "1")
        bathroom_count = response.xpath("//div[@class='room-icon']/i[@class='flaticon-bath']/following-sibling::span/text()").extract_first()
        if bathroom_count:
            item_loader.add_value("bathroom_count", bathroom_count.strip())
 
        furnished = response.xpath("//div[@class='starItems']//li[contains(.,'furnished') or contains(.,'FURNISHED')]/text()").extract_first()
        if furnished:
            if "unfurnished" in furnished.lower() and " or " not in furnished.lower() :
                item_loader.add_value("furnished", True)
            else:
                item_loader.add_value("furnished", True)
        floor = response.xpath("//div[@class='starItems']//li[contains(.,'Floor ') or contains(.,'FLOOR')]/text()").extract_first()
        if floor:
            if "WOODEN" not in floor.upper():
                floor = floor.lower().split("floor")[0]
                item_loader.add_value("floor", floor)
        balcony = response.xpath("//div[@class='starItems']//li[contains(.,'BALCONY') or contains(.,'Balcony')]/text()").extract_first()
        if balcony:
            item_loader.add_value("balcony", True)
        elevator = response.xpath("//div[@class='starItems']//li[contains(.,'LIFT') or contains(.,'Lift')]/text()").extract_first()
        if elevator:
            item_loader.add_value("elevator", True)
        parking = response.xpath("//div[@class='starItems']//li[contains(.,'PARKING ') or contains(.,'Parking')]/text()").extract_first()
        if parking:
            item_loader.add_value("parking", True)

        map_coordinate = response.xpath("//script[@type='text/javascript']//text()[contains(.,'lat:') and contains(.,'lon:')]").extract_first()
        if map_coordinate:
            latitude = map_coordinate.split('lat: "')[1].split('",')[0].strip()
            longitude = map_coordinate.split('lon: "')[1].split('",')[0].strip()
            if latitude and longitude:
                item_loader.add_value("longitude", longitude)
                item_loader.add_value("latitude", latitude)
            
        desc = " ".join(response.xpath("//div[@id='propdescription']//text()[normalize-space()][not(contains(.,'innerHTML ') or contains(.,'JavaScript enabled') )]").extract())
        if desc:
            item_loader.add_value("description", desc.strip())
  

        available_date = response.xpath("substring-after(//div[@id='propdescription']//text()[contains(.,'RENTAL UNTIL')],'RENTAL UNTIL')").extract_first()
        if available_date:  
            try:                
                newformat = dateparser.parse(available_date, languages=['en']).strftime("%Y-%m-%d")
                item_loader.add_value("available_date", newformat)
            except:
                pass
        images = [x for x in response.xpath("//div[@id='slider']//li/img/@data-src").extract()]
        if images:
            item_loader.add_value("images", images)      

        item_loader.add_value("landlord_phone", "0208 527 7007")
        item_loader.add_value("landlord_email", "enquiries@completion.co.uk")
        item_loader.add_value("landlord_name", "Completion Sales and Lettings")
      

        yield item_loader.load_item()
