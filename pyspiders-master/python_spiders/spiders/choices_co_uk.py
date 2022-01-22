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
    name = 'choices_co_uk'
    execution_type='testing'
    country='united_kingdom'
    locale='en'
    thousand_separator = ','
    scale_separator = '.' 
    external_source="Choices_Co_PySpider_united_kingdom"
    start_urls = ["https://www.choices.co.uk/search-results/?xbranch=&dep=to-rent"]

    # 1. FOLLOWING
    def parse(self, response):
        # url = "https://www.choices.co.uk/property-details/?rpid=rps_chc-LND200238"
        # yield Request(url, callback=self.populate_item)
        for item in response.xpath("//a[contains(@class,'anim-hover hover-parent')]/@href").extract():
            follow_url = response.urljoin(item)
            yield Request(follow_url, callback=self.populate_item)
        
        next_page = response.xpath("//a[.='»']/@href").get()
        if next_page:
            yield Request(response.urljoin(next_page), callback=self.parse)
    
    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        status = response.xpath("//span[contains(@class,'property-status')]/text()").get()
        if (status and "under offer" in status.lower().strip()) or (status and 'withdrawn' in status.lower().strip()):
            return
        elif status and status.lower().strip() == 'let':
            return
        
        item_loader.add_value("external_link", response.url)

        prop_type = "".join(response.xpath("//span[contains(@class,'property-type')]/text()").getall())
        if get_p_type_string(prop_type):
            item_loader.add_value("property_type", get_p_type_string(prop_type))
        else:
            prop_type = "".join(response.xpath("//div[@class='property-desc']/p/text()").getall())
            if get_p_type_string(prop_type):
                item_loader.add_value("property_type", get_p_type_string(prop_type))
            else:
                return

        item_loader.add_value("external_source", self.external_source)
        
        title = response.xpath("//div/h1/text()").extract_first()
        if title:          
            item_loader.add_value("title", title.strip())
        #     address = ""
        #     if "half price rent" not in title.lower():
        #         if " in" in title:
        #             address = title.split(" in")[1].strip()
        #             if "House" in address:
        #                 address = ""
        #             elif " -" in address:
        #                 address = title.split(" -")[1].strip()
        #         elif " -" in title:
        #             address = title.split(" -")[1].strip()
        #         elif "," in title:
        #             address = title.split(",")[1].strip()
        #         if address:
        #             item_loader.add_value("address", address.strip())
        #             city = address
        #             if "," in address:
        #                 city = city.split(",")[1].replace(".","")
        #             if city.isalpha():
        #                 item_loader.add_value("city", city.strip())

        addres = ""
        address =  response.xpath("//div/h1[@class='property-address']/text()[not(contains(.,'House') or contains(.,'Apartment') or contains(.,'RENT') or contains(.,'Double') or contains(.,'month'))]").extract_first()
        if address:

            if "rent in" in address.lower():
                addres = address.split("in")[1].strip()

            elif "," in address:
                addres = address.strip()
            
            elif "-" in address.lower():
                addres = address.split("-")[-1].strip()

            item_loader.add_value("address", addres.strip())
            city = ""
            if "-" in addres:
                city =  addres.split("-")[0]
                item_loader.add_value("zipcode", addres.split("-")[1].strip())
            else:
                city =  addres
            item_loader.add_value("city", city)
        else:
            address = response.xpath("//div[@class='property-desc']//p[contains(., 'Rent in')]/text()").get()
            if address:
                item_loader.add_value('address', address.split('in')[-1].strip())
            
        available_date=response.xpath("//div[@class='property-desc']/p[contains(.,'Available')]/text()[not(contains(.,'Double') or contains(.,'EPC') or contains(.,'furnished'))]").get()
        date3= ""
        if available_date:
            date2 =  available_date.split("Available")
            if len(date2)>2:
                date2=date2[1].strip().replace("from","").replace("on","").strip().replace("!","").replace(".","").strip().replace("immediately","now")
                if "now" in str(date2).lower():
                    date3 = date2.split(" ")[0].strip()
                elif "-" in date2:
                    date3 = date2.split("-")[0].strip().replace("TODAY","now")
                else:
                    date3 = date2
                if date3:
                    
                    date_parsed = dateparser.parse(
                        date3, date_formats=["%m-%d-%Y"]
                    )
                    if date_parsed is not None:
                        date4 = date_parsed.strftime("%Y-%m-%d")
                        item_loader.add_value("available_date", date4)
 
        ext_id = response.xpath("substring-after(//p[@class='property-meta']/span[@class='property-ref']//text(),':')").extract_first()
        if ext_id:
            item_loader.add_value("external_id", ext_id.strip())
  
        room_count = response.xpath("substring-after(//span[@title='Bedrooms']/text(),':')").get()
        if room_count and room_count.strip() !="0":
            item_loader.add_value("room_count", room_count)     
        
        bathroom_count=response.xpath("substring-after(//span[@title='Bathrooms']/text(),':')").get()
        if bathroom_count and bathroom_count.strip() !="0":
            item_loader.add_value("bathroom_count", bathroom_count)

        rent = response.xpath("//p[@class='property-meta']/span[@class='property-price']//text()").extract_first()
        if rent:
            if 'or' in rent.lower():
                rent = rent.split("pcm")[-1].split("£")[-1]
                if 'pw' in rent:
                    rent = re.sub(r'\D', '', rent)
                    rent = int(rent) * 4 
            elif "pcm" in rent.lower():
                rent = rent.split("pcm")[0].split("£")[-1]
            item_loader.add_value("rent_string", rent)    
            item_loader.add_value("currency", "GBP")    
     
        desc = " ".join(response.xpath("//div[@class='property-desc']//text()[normalize-space()]").extract())
        if desc:
            item_loader.add_value("description",desc.strip())      
            
       
        images = [response.urljoin(x) for x in response.xpath("//div[@class='magnific-gallery']//img/@src").extract()]
        if images:
            item_loader.add_value("images", images)   

        map_coordinate = response.xpath("//li/a[contains(@href,'maps.google.co')]/@href").extract_first()
        if map_coordinate:
            map_coordinate = map_coordinate.split("spn=")[1].split("&")[0].strip()
            latitude = map_coordinate.split(",")[0].strip()
            longitude = map_coordinate.split(",")[1].strip()
            if latitude and longitude:
                item_loader.add_value("longitude", longitude)
                item_loader.add_value("latitude", latitude)

            
        parking = response.xpath("//ul[contains(@class,'property-action')]/li//text()[contains(.,'parking') or contains(.,'Parking') or contains(.,'garage')]").get()
        if parking:
            item_loader.add_value("parking", True)  
        furnished = response.xpath("//ul[contains(@class,'property-action')]/li//text()[contains(.,'Furnished') or contains(.,'furnished')]").get()
        if furnished:
            item_loader.add_value("furnished", True)  
        elif not furnished and desc:
            if "unfurnished" in desc.lower():
                item_loader.add_value("furnished", False)  
            elif "furnished" in desc.lower():
                item_loader.add_value("furnished", True) 
        floor = response.xpath("//ul[contains(@class,'property-action')]/li//text()[contains(.,'Floor')]").get()
        if floor:
            item_loader.add_value("floor", floor.split("Floor")[0].strip())  
        elevator = response.xpath("//ul[contains(@class,'property-action')]/li//text()[contains(.,'Lift') or contains(.,'lift')]").get()
        if elevator:
            item_loader.add_value("elevator", True)  
        balcony = response.xpath("//ul[contains(@class,'property-action')]/li//text()[contains(.,'balcony') or contains(.,'Balcony')]").get()
        if balcony:
            item_loader.add_value("balcony", True) 
        terrace = response.xpath("//ul[contains(@class,'property-action')]/li//text()[contains(.,'terrace') or contains(.,'Terrace')]").get()
        if terrace:
            item_loader.add_value("terrace", True) 
             
        available_date = response.xpath("//div[@class='property-desc']//text()[contains(.,'Available')]").get()
        if available_date:       
            try: 
                date_parsed = dateparser.parse(available_date.split("Available")[1].strip(), languages=['en'])
                if date_parsed:
                    date2 = date_parsed.strftime("%Y-%m-%d")
                    item_loader.add_value("available_date", date2)
            except:
                pass
                
        energy = response.xpath("//div[@class='property-desc']//text()[contains(.,'EPC')]").get()
        if energy:
            energy_label = energy.strip().split(" ")[-1].upper().replace(".","").strip()
            if energy_label in ["A","B","C","D","E","F","G"]:
                item_loader.add_value("energy_label", energy_label)       
        landlord_email = response.xpath("substring-after(//div[contains(@class,'widget-box')]//p//text()[contains(.,'Email')],':')").get()
        if landlord_email:
            item_loader.add_value("landlord_email", landlord_email)
        landlord_phone = response.xpath("substring-after(//div[contains(@class,'widget-box')]//p//text()[contains(.,'Tel')],':')").get()
        if landlord_phone:
            item_loader.add_value("landlord_phone", landlord_phone)
        if not landlord_phone:
            item_loader.add_value("landlord_phone", "01737 783560")
        item_loader.add_xpath("landlord_name", "//div[contains(@class,'widget-box')]//div[contains(@class,'title-fill')]//text()")  
        

        yield item_loader.load_item()

def get_p_type_string(p_type_string):
    if p_type_string and "studio" in p_type_string.lower():
        return "studio"
    elif p_type_string and ("apartment" in p_type_string.lower() or "flat" in p_type_string.lower() or "maisonette" in p_type_string.lower()):
        return "apartment"
    elif p_type_string and "house" in p_type_string.lower():
        return "house"    
    elif p_type_string and "student" in p_type_string.lower():
        return "student_apartment"
    else:
        return None
