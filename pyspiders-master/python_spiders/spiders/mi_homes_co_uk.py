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
from word2number import w2n
from datetime import datetime
import dateparser
import re 
 
class MySpider(Spider):
    name = 'mi_homes_co_uk' 
    execution_type = 'testing'  
    country = 'united_kingdom'
    locale = 'en'
    start_urls = ["https://mi-homes.co.uk/search_results/?address_keyword=&department=residential-lettings"]
   
    # 1. FOLLOWING
    def parse(self, response):
        page = response.meta.get("page", 2)
        for item in response.xpath("//div[@class='thumbnail']/a"):
            status = item.xpath("./div/text()").get()
            if status and "to let" not in status.lower():
                continue 
            follow_url = response.urljoin(item.xpath("./@href").get())
            yield Request(follow_url, callback=self.populate_item)
        
        next_page = response.xpath("//a[contains(@class,'next')]/@href").get()
        if next_page:
            p_url = f"https://mi-homes.co.uk/search_results/page/{page}/?address_keyword&department=residential-lettings"
            yield Request(
                p_url,
                callback=self.parse,
                meta={"page":page+1})
        
        
    
    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)
    
        item_loader.add_value("external_link", response.url)
        item_loader.add_value("external_source", "Mi_Homes_Co_PySpider_united_kingdom")
        item_loader.add_xpath("title", "//title/text()")

        f_text = " ".join(response.xpath("//div[contains(text(),'type')]/following-sibling::div/text()").getall())
        prop_type = ""
        if get_p_type_string(f_text): 
            prop_type = get_p_type_string(f_text) 
        else: 
            f_text = " ".join(response.xpath("//div[contains(@class,'propertydescription')]//text()").getall())
            if get_p_type_string(f_text):
                prop_type = get_p_type_string(f_text)
 
        if prop_type:
            item_loader.add_value("property_type", prop_type)



        
        address = "".join(response.xpath("//div[@class='socialbox']/h4/text()").getall())
        if address:
            item_loader.add_value("address", re.sub("\s{2,}", " ", address))

        zipcode= "".join(response.xpath("//div[@class='locationsec']/span[.='Location:']/following-sibling::text()").getall())
        if zipcode:
            zipcode = zipcode.split(",")[-1].strip() 
    
            if not zipcode.replace(" ","").isalpha(): 
               item_loader.add_value("zipcode", zipcode)
            if zipcode=='north finchley':
               item_loader.add_value("zipcode", 'n12')

 

        cityzip="".join(response.xpath("//div[@class='socialbox']/h4/text()").getall())
        if cityzip: 

            city1=cityzip.split(",")[-1].strip()
            city2=cityzip.split(",")[-2]

            if re.findall("\d+",city1): 
               item_loader.add_value("city",city2) 
            if not re.findall("\d+",city1):
               item_loader.add_value("city",city1)

        rent = "".join(response.xpath("substring-before(//div[@class='propertyattrbtm']/div/text()[contains(.,'£')],'p')").getall())
        if rent:
            item_loader.add_value("rent_string",rent.strip())

        room_count = response.xpath("//div[@class='propertyattrbtm']/div[contains(.,'bedroom')]/following-sibling::div/text()").get()
        if room_count:
            if room_count !="0":
                item_loader.add_value("room_count",room_count)
            else:
                room_count = "".join(response.xpath("//h2[contains(.,'Studio')]/text()").getall())
                if "studio" in room_count.lower():
                    item_loader.add_value("room_count","1")

        bathroom_count = response.xpath("//div[@class='propertyattrbtm']/div[contains(.,'bathroom')]/following-sibling::div/text()").get()
        if bathroom_count:
            item_loader.add_value("bathroom_count",bathroom_count.strip())

         
        available_date=response.xpath("//div[@class='propertyattrbtm']/div[contains(.,'available')]/following-sibling::div/text()").get()
        if available_date: 
            date2 = available_date 
            date_parsed = dateparser.parse(
                date2, date_formats=["%m-%d-%Y"]
            ) 
            date3 = date_parsed.strftime("%Y-%m-%d")
            item_loader.add_value("available_date", date3)
        else:
            available_date=response.xpath("//div[contains(@class,'propfeatures')]/ul/li[contains(.,'Available')]/text()").get()
            if available_date:
                date2 = available_date.split("Available")[1].replace("on","").replace("from","").strip()
                date_parsed = dateparser.parse(
                    date2, date_formats=["%m-%d-%Y"]
                )
                date3 = date_parsed.strftime("%Y-%m-%d")
                item_loader.add_value("available_date", date3)

        
        description = " ".join(response.xpath("//div[contains(@class,'propertydescription')]/text()").getall()).strip()   
        if description:
            item_loader.add_value("description", description.replace('\xa0', '').strip())

        meters = " ".join(response.xpath("//div[contains(@class,'propfeatures')]/ul/li[contains(.,'square')]/text()").getall()).strip()   
        if meters:
            if " feet" in meters:
                s_meters = meters.split("square")[0].strip().split(" ")[-1].replace(",","")
                sqm = str(int(float(s_meters) * 0.09290304))
                item_loader.add_value("square_meters", sqm)
            else:
                s_meters = meters.split("square")[0].strip().split(" ")[-1].replace(",","")
                item_loader.add_value("square_meters", s_meters)
        images = [x for x in response.xpath("//div[@class='property-carousel']/div/img/@src").extract()]
        if images is not None:
            item_loader.add_value("images", images)
        floor_plan_images = [x for x in response.xpath("//div[@id='floorplan']/img/@src").extract()]
        if floor_plan_images is not None:
            item_loader.add_value("floor_plan_images", list(set(floor_plan_images)))
        
        LatLng = "".join(response.xpath("substring-before(substring-after(//div[@id='transport']/script/text()[contains(.,'LatLng')],'LatLng('),',')").getall())
        if LatLng:  
            item_loader.add_value("latitude",LatLng)
            item_loader.add_xpath("longitude","substring-before(substring-after(substring-after(//div[@id='transport']/script/text()[contains(.,'LatLng')],'LatLng('),','),')')")
         
        
        furnished = "".join(response.xpath("//div[@class='propertyattrbtm']/div[contains(.,'furnished')]/following-sibling::div/text()").getall())
        if furnished:
            if "Unfurnished" in furnished : 
                item_loader.add_value("furnished",False)
            elif "Furnished" in furnished:
                item_loader.add_value("furnished",True) 
        
        
        item_loader.add_value("landlord_phone", "020 7323 9574")
        item_loader.add_value("landlord_email", "hello@mi-homes.co.uk")
        item_loader.add_value("landlord_name", "Mi Homes")
        yield item_loader.load_item() 
 


def get_p_type_string(p_type_string):
    if p_type_string and "student" in p_type_string.lower():
        return "student_apartment"
    elif p_type_string and "studio" in p_type_string.lower():
        return "studio"
    elif p_type_string and ("apartment" in p_type_string.lower() or "flat" in p_type_string.lower() or "etage" in p_type_string.lower()):
        return "apartment"
    elif p_type_string and ("house" in p_type_string.lower() or "woning" in p_type_string.lower() or "family home" in p_type_string.lower() or "mews" in p_type_string.lower() or "maisonette" in p_type_string.lower()):
        return "house"
    else:
        return None