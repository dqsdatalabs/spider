# -*- coding: utf-8 -*-
# Author: Mehmet Kurtipek

from scrapy.loader.processors import MapCompose
from scrapy import Spider
from scrapy import Request,FormRequest
from scrapy.selector import Selector
from w3lib.html import remove_tags
from python_spiders.loaders import ListingLoader
import json
import dateparser

class MySpider(Spider):
    name = 'pointproperties_info'
    execution_type = "testing"
    country = "united_kingdom"
    locale = "en"
    start_urls = ["https://www.pointproperties.info/properties/?page=1&propind=L&country=&town=&area=&MinPrice=&MaxPrice=&MinBeds=&BedsEqual=&PropType=&Furn=&Avail=&orderBy=PriceSearchAmount&orderDirection=ASC&areaId=&lat=&lng=&zoom=&searchbymap=&maplocations=&hideProps=1&location=&businessCategoryId=1&searchType=grid&lettingsClassificationRefIds=90,91,-1&sortBy=lowestPrice", "https://www.pointproperties.info/students/"]

    # 1. FOLLOWING
    def parse(self, response):
        for item in response.xpath("//div[@class='photo-cropped']/a"):
            follow_url = response.urljoin(item.xpath("./@href").get())
            yield Request(follow_url, callback=self.populate_item)
        
        next_page = response.xpath("//a[.='next']/@href").get()
        if next_page:
            yield Request(
                response.urljoin(next_page),
                callback=self.parse) 
        
        
    
    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        item_loader.add_value("external_source", "Pointproperties_PySpider_united_kingdom")
        item_loader.add_value("external_link", response.url)
        item_loader.add_xpath("title", "//div[@class='address']/text()")
        f_text = " ".join(response.xpath("//div[@class='bedswithtype']//text()").getall())
        prop_type = ""
        if get_p_type_string(f_text):
            prop_type = get_p_type_string(f_text)

        if "student" in response.url:
            prop_type = "student_apartment"

        if prop_type:
            item_loader.add_value("property_type", prop_type)
        else: return
        
        adres=""
        address = response.xpath("//div[@class='address']/text()").extract_first()
        if address:
            if "Bedroom" in address or "bedroom" in address:
                if "," in address:
                    adres = "".join(address.split(",")[1:]).strip()
                elif "." in address:
                    adres = address.split(".")[1].strip()
                elif "on" in address:
                    adres = "".join(address.split("on")[1]).strip()
                    
                else:
                   return
            else:
                
                adres = address.replace("Apartment  - ","").replace("Apartment 2,","").strip()
            
            if "," in adres:
                city = adres.split(",")[-1].strip()
                item_loader.add_value("city", city)
            elif "-" in adres:
                city = adres.split("-")[-1].strip()
                item_loader.add_value("city", city)
    
            item_loader.add_value("address", adres)

        external_id = "".join(response.xpath("//div[@class='reference']/text()").getall())
        if external_id:
            item_loader.add_value("external_id",external_id.split(":")[1].strip())  

        term = response.xpath("//span[@class='displaypricequalifier']/text()").get()
        rent = "".join(response.xpath("//div[@class='price']/span[@class='displayprice']/text()").getall())
        if rent:
            if term and "pw" in term:
                price = rent.split("£")[1].replace(",","").strip() 
                item_loader.add_value("rent",int(float(price))*4)
            elif term and "pcm" in term:
                price = rent.split("£")[1].replace(",","").strip()
                item_loader.add_value("rent", price)
                
        item_loader.add_value("currency","GBP") 

        room_count = "".join(response.xpath("//span[@class='beds']/text()").getall())
        if room_count:
            item_loader.add_value("room_count",room_count.strip()) 
        else:
            item_loader.add_xpath("room_count","//span[@class='receptions']/text()")

        bathroom_count = "".join(response.xpath("//span[@class='bathrooms']/text()").getall())
        if bathroom_count:
            item_loader.add_value("bathroom_count",bathroom_count.strip()) 

        description = " ".join(response.xpath("//div[@class='description']/text()").getall()).strip()   
        if description:
            item_loader.add_value("description", description.replace('\xa0', '').strip())

        item_loader.add_xpath("latitude", "substring-before(substring-after(//div[@id='maplinkwrap']/a/@href,'lat='),'&')")
        item_loader.add_xpath("longitude","substring-before(substring-after(//div[@id='maplinkwrap']/a/@href,'lng='),'&')")

        available_date=response.xpath("substring-after(//div[@class='available']/text(),': ')").get()
        if available_date:
            date_parsed = dateparser.parse(
                available_date, date_formats=["%d-%m-%Y"]
            )
            date3 = date_parsed.strftime("%Y-%m-%d")
            item_loader.add_value("available_date", date3)

        images = [x for x in response.xpath("//div[@class='propertyimagecontainer']/img/@src").getall()]
        if images:
            item_loader.add_value("images", images)


        floor_plan_images = [x for x in response.xpath("//div[@id='hiddenfloorplan']/div/div/img/@src").getall()]
        if floor_plan_images:
            item_loader.add_value("floor_plan_images", floor_plan_images)

        furnished = "".join(response.xpath("//span[@class='bedsWithTypePropType']/text()").getall())
        if furnished:
            if "terraced" in furnished.lower():
                item_loader.add_value("furnished",True)

        dishwasher = "".join(response.xpath("//div[@class='twocolfeaturelistcol2']/ul/li/text()[.='Dishwasher']").getall())
        if dishwasher:
            item_loader.add_value("dishwasher",True)

        washing_machine = "".join(response.xpath("//div[@class='twocolfeaturelistcol2']/ul/li/text()[.='Washing Machine']").getall())
        if washing_machine:
            item_loader.add_value("washing_machine",True)
        

        parking = response.xpath("//li[contains(.,'Parking') or contains(.,'parking')]/text()").get()
        if parking and "no" not in parking.lower():
            item_loader.add_value("parking", True)


        item_loader.add_value("landlord_phone", "0151 735 0275")
        item_loader.add_value("landlord_name", "Point Properties")
        item_loader.add_value("landlord_email", "mail@pointproperties.info")
        yield item_loader.load_item()


def get_p_type_string(p_type_string):
    if p_type_string and "share" in p_type_string.lower():
        return None
    elif p_type_string and "student" in p_type_string.lower():
        return "student_apartment"
    elif p_type_string and "studio" in p_type_string.lower():
        return "studio"
    elif p_type_string and ("apartment" in p_type_string.lower() or "flat" in p_type_string.lower() or "maisonette" in p_type_string.lower()):
        return "apartment"
    elif p_type_string and ("house" in p_type_string.lower() or "detached" in p_type_string.lower() or "terrace" in p_type_string.lower() or "bungalow" in p_type_string.lower()):
        return "house"
    else:
        return None