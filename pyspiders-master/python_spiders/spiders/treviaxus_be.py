# -*- coding: utf-8 -*-
# Author: Mehmet Kurtipek


from scrapy.loader.processors import MapCompose
from scrapy import Spider
from scrapy import Request,FormRequest
from scrapy.selector import Selector
from w3lib.html import remove_tags
from python_spiders.loaders import ListingLoader
import json
from datetime import date
import dateparser


class MySpider(Spider):
    name = 'treviaxus_be'
    execution_type='testing'
    country='belgium'
    locale='nl'
    def start_requests(self):
        start_urls = [
            {"url": "https://www.trevi-axus.be/te-huur/appartementen", "property_type": "apartment"},
	        {"url": "https://www.trevi-axus.be/te-huur/woningen", "property_type": "house"}, 
        ]  # LEVEL 1
        
        for url in start_urls:
            yield Request(url=url.get('url'),
                            callback=self.parse,
                            meta={'property_type': url.get('property_type')
                        })

    # 1. FOLLOWING
    def parse(self, response):
        property_type = response.meta.get("property_type")

        for follow_url in response.xpath("//a[@class='property-contents ']/@href").extract():
            yield Request(follow_url, callback=self.populate_item, meta={"property_type":property_type})

    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)
        item_loader.add_value("property_type", response.meta.get('property_type'))
        item_loader.add_value("external_link", response.url)
        title = " ".join(response.xpath("//section[@id='property-title']//div[@class='category']/p/text()").extract())
        if title:
            item_loader.add_value("title", title.strip())
            
        item_loader.add_value("external_source", "Trevi_Axus_PySpider_belgium")
      
        address = response.xpath("//section[@id='property-title']//div[@class='location']/span/text()").extract_first()
        if address:
            item_loader.add_value("address",address.strip()) 
            city = address.split(" ")[-1] 
            item_loader.add_value("city",city.strip())

            zipcode = address.split(",")[-1].strip().split(" ")[0]
            item_loader.add_value("zipcode", zipcode.strip())
        
        item_loader.add_xpath("bathroom_count", "//dt[contains(.,'Badkamers')]/following-sibling::dd/text()")

        room_count =response.xpath("//dt[contains(.,'Slaapkamers')]/following-sibling::dd/text()").extract_first()
        if room_count:                
            item_loader.add_value("room_count",room_count) 
    
        rent =response.xpath("//dt[contains(.,'Prijs')]/following-sibling::dd/text()").extract_first()
        if rent:                
            item_loader.add_value("rent_string",rent.replace(" ",""))

        square = response.xpath("//dt[contains(.,'Bewoonbare')]/following-sibling::dd/text()").extract_first()
        if square:
            square_meters =  square.split(" ")[0].strip().split(",")[0]
            item_loader.add_value("square_meters",square_meters) 

        terrace =response.xpath("//dt[.='Terras']/following-sibling::dd/text()").extract_first()    
        if terrace:
            if terrace.strip().lower() != "ja":
                item_loader.add_value("terrace", False)
            else:
                item_loader.add_value("terrace", True)
    

        desc = " ".join(response.xpath("//details/div//text()").extract())
        if desc:
            item_loader.add_value("description", desc.strip())
              
        images = [response.urljoin(x) for x in response.xpath("//ul[@class='detail-slider']/li/a/@href").extract()]
        if images:
                item_loader.add_value("images", images)
        
        floor = response.xpath("//dt[contains(.,'gebouw')]/following-sibling::dd/text()").get()
        if floor:
            item_loader.add_value("floor", floor.strip())
        
        parking = response.xpath("//dt[contains(.,'arages') or contains(.,'arking')]/following-sibling::dd/text()").get()
        if parking:
            try:
                parking_count = int(parking)
                if parking_count == 0:
                    item_loader.add_value("parking", False)
                elif parking_count > 0:
                    item_loader.add_value("parking", True)
            except:
                if parking == "nee" or parking == "non":
                    item_loader.add_value("parking", False)
                else:
                    item_loader.add_value("parking", True)
        
        elevator = response.xpath("//dt[contains(.,'Lift')]/following-sibling::dd/text()").get()
        if elevator:
            try:
                elevator_count = int(elevator)
                if elevator_count == 0:
                    item_loader.add_value("elevator", False)
                elif elevator_count > 0:
                    item_loader.add_value("elevator", True)
            except:
                if elevator == "nee" or parking == "non":
                    item_loader.add_value("elevator", False)
                else:
                    item_loader.add_value("elevator", True)
        
        balcony = response.xpath("//dt[contains(.,'Balcon')]/following-sibling::dd/text()").get()
        if balcony:
            try:
                balcony_count = int(balcony)
                if balcony_count == 0:
                    item_loader.add_value("balcony", False)
                elif balcony_count > 0:
                    item_loader.add_value("balcony", True)
            except:
                if balcony == "nee" or parking == "non":
                    item_loader.add_value("balcony", False)
                else:
                    item_loader.add_value("balcony", True)
        
        
        item_loader.add_value("external_id", response.url.split("/")[-1])

        epc = response.xpath("//dt[contains(.,'EPC')]/following-sibling::dd/span/@class").get()
        if epc:
            epc = epc.split("class_")[1].strip()
            item_loader.add_value("energy_label", epc.upper())

        today = date.today()
        available_date = "".join(response.xpath("//dt[contains(.,'Beschikbaarheid')]/following-sibling::dd/text()").getall())
        if available_date:
            available_date = available_date.replace("Vanaf datum", "").strip()
            if "onmiddellijk" in available_date.lower():
                item_loader.add_value("available_date", today.strftime("%Y-%m-%d"))
            else:
                date_parsed = dateparser.parse(available_date)
                if date_parsed:
                    item_loader.add_value("available_date", date_parsed.strftime("%Y-%m-%d"))

        item_loader.add_xpath("landlord_name", "//div[@class='name']/text()")
        if not item_loader.get_collected_values("landlord_name"):
            item_loader.add_value("landlord_name", "Trevi Axus")
        item_loader.add_xpath("landlord_phone", "//div[@class='numbers']/a/text()")
        item_loader.add_value("landlord_email", "info@trevi-axus.be")

        yield item_loader.load_item()