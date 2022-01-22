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
    name = 'expata_nl'
    execution_type = 'testing'
    country = 'netherlands'
    locale = 'nl'
    start_urls = ["https://www.expata.nl/huuraanbod"]

    # 1. FOLLOWING
    def parse(self, response):
        page = response.meta.get("page", 2)
        seen = False
        for item in response.xpath("//a[@class='grey-border']"):
            status = item.xpath(".//p[@class='label']/text()").get()
            if status and "verhuurd" in status.lower():
                continue
            follow_url = response.urljoin(item.xpath("./@href").get())
            yield Request(follow_url, callback=self.populate_item)
            seen = True
        
        if page == 2 or seen:
            p_url = f"https://www.expata.nl/huuraanbod?page={page}"
            yield Request(
                p_url,
                callback=self.parse,
                meta={"page":page+1})
        
        
    
    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        item_loader.add_value("external_link", response.url)
        item_loader.add_value("external_id", response.url.split("-")[-1])

        f_text = "".join(response.xpath("//p[contains(@class,'description')]/text()").getall())
        if get_p_type_string(f_text):
            item_loader.add_value("property_type", get_p_type_string(f_text))
        else:
            return
        
        item_loader.add_value("external_source", "Expata_PySpider_netherlands")
        
        title =", ".join(response.xpath("//div[@class='left-column']/h1//text()").extract()) 
        if title:
            title = re.sub("\s{2,}", " ", title)
            item_loader.add_value("address",title.strip() ) 
            item_loader.add_value("title",title.strip() ) 

        item_loader.add_xpath("city", "//li[span[contains(.,'Plaats')]]/span[2]/text()[.!='0']")
        room_count = response.xpath("//li[span[contains(.,'Slaapkamers')]]/span[2]/text()[.!='0']").extract_first() 
        if not room_count:    
            room_count = response.xpath("//li[span[contains(.,'Kamer')]]/span[2]/text()").extract_first() 
        if room_count: 
            item_loader.add_value("room_count",room_count) 

        rent = response.xpath("//p[@class='price']//text()").extract_first() 
        if rent: 
            item_loader.add_value("rent_string",rent)      

        deposit = response.xpath("//p[contains(@class,'description')]//text()[contains(.,'waarborgsom') and contains(.,'€')]").extract_first() 
        if deposit: 
            item_loader.add_value("deposit",deposit)          
        else:
            deposit = response.xpath("//p[contains(@class,'description')]//text()[contains(.,'borg:') and contains(.,'maanden')] | //p[contains(@class,'description')]//text()[(contains(.,'deposit') or contains(.,'Deposit')) and contains(.,'month')]").extract_first() 
            if deposit: 
                if "deposit" in deposit.lower():
                    deposit = deposit.split("month")[0].strip().split(" ")[-1].strip()
                elif "maanden" in deposit:
                    deposit = deposit.split("maanden")[0].strip().split(" ")[-1].strip()
                deposit_value = ""
                if deposit.replace(".","").isdigit():
                    deposit_value = deposit
                elif "een" in deposit.lower() or "one" in deposit.lower(): 
                    deposit_value = "1"
                if "twee" in deposit.lower() or "two" in deposit.lower():
                    deposit_value = "2"
                if rent and deposit_value:
                    rent = rent.split("€")[1].split(",-")[0].replace(".","")
                    deposit_value = float(deposit_value) * int(float(rent))
                if deposit_value:
                    item_loader.add_value("deposit",int(float(deposit_value))) 
            else:
                deposit = response.xpath("//p[contains(@class,'description')]//text()[contains(.,'borg')]").get()
                if deposit:
                    if "x" in deposit:
                        deposit = deposit.strip().split("x")[0].strip().split(" ")[-1].replace(",",".")
                    else:
                        deposit = deposit.strip().split("maand")[0].strip().split(" ")[-1]
                    rent = rent.split("€")[1].split(",-")[0].replace(".","").strip()
                    deposit = float(deposit) * int(float(rent))
                    item_loader.add_value("deposit", int(float(deposit)))
        utilities = response.xpath("//p[contains(@class,'description')]//text()[(contains(.,'service') or contains(.,'Service')) and contains(.,'€') and not(contains(.,'rent')) and not(contains(.,' huur '))]").extract_first() 
        if utilities: 
            utilities = utilities.split("€")[1].strip().split(" ")[0].replace(",-","")
            item_loader.add_value("utilities",int(float(utilities.replace(",","."))))  

        available_date = response.xpath("//li[span[contains(.,'Beschikbaarheid')]]/span[2]/text()").extract_first() 
        if available_date and "direct" not in available_date.lower():
            date_parsed = dateparser.parse(available_date.strip())
            if date_parsed:
                date2 = date_parsed.strftime("%Y-%m-%d")
                item_loader.add_value("available_date", date2)
      
        square =response.xpath("//li[span[contains(.,'Woonoppervlakte')]]/span[2]/text()").extract_first()
        if square:
            square_meters =  square.split("m")[0].strip()
            item_loader.add_value("square_meters", square_meters) 

        furnished =response.xpath("//li[span[contains(.,'Interieur')]]/span[2]/text()").extract_first()    
        if furnished:
            if "gemeubileerd" in furnished.lower() or "gestoffeerd" in furnished.lower():
                item_loader.add_value("furnished", True)
            else:
                item_loader.add_value("furnished", False)    
      
        desc = " ".join(response.xpath("//p[contains(@class,'description')]//text()").extract())
        if desc:
            item_loader.add_value("description", desc.strip())
              
        images = [x for x in response.xpath("//ul[@class='slides']/li/div/@data-image").extract()]
        if images:
            item_loader.add_value("images", images)
        item_loader.add_xpath("latitude", "//div[@id='mapview-canvas']/@data-lat")
        item_loader.add_xpath("longitude", "//div[@id='mapview-canvas']/@data-lng")

        item_loader.add_value("landlord_name", "Expata Real Estate")
        item_loader.add_value("landlord_phone", "070 2163555")
        item_loader.add_value("landlord_email", "info@expata.nl")   
              
        yield item_loader.load_item()


def get_p_type_string(p_type_string):
    if p_type_string and "student" in p_type_string.lower():
        return "student_apartment"
    elif p_type_string and "studio" in p_type_string.lower():
        return "studio"
    elif p_type_string and ("apartment" in p_type_string.lower() or "appartement" in p_type_string.lower() or "bovenwoning" in p_type_string.lower()):
        return "apartment"
    elif p_type_string and ("house" in p_type_string.lower() or "cottage" in p_type_string.lower() or "terrace" in p_type_string.lower()):
        return "house"
    else:
        return None