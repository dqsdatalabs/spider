# -*- coding: utf-8 -*-
# Author: Mehmet Kurtipek

from scrapy.loader.processors import MapCompose
from scrapy import Spider
from scrapy import Request,FormRequest
from scrapy.selector import Selector
from w3lib.html import remove_tags
from python_spiders.loaders import ListingLoader
import re
import dateparser

class MySpider(Spider):
    name = 'hotblackdesiato_co_uk'
    execution_type='testing'
    country='united_kingdom'
    locale='en'
    external_source = 'Hotblackdesiato_PySpider_united_kingdom'
    thousand_separator = ','
    scale_separator = '.'
    def start_requests(self):
        start_url = "https://hotblackdesiato.co.uk/properties/?type=let"
        yield Request(start_url, callback=self.parse)

    # 1. FOLLOWING
    def parse(self, response):
        for item in response.xpath("//div[@data-type='residential-lettings']"):
            follow_url =  item.xpath("./div/a/@href").get()
            lat =  item.xpath("./@data-lat").get()
            lng =  item.xpath("./@data-long").get()
            yield Request(follow_url, callback=self.populate_item, meta={"lat":lat,"lng":lng})
        

    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        item_loader.add_value("external_link", response.url)
        item_loader.add_value("external_source", self.external_source)  
        item_loader.add_xpath("title", "//title/text()")
        item_loader.add_value("longitude", response.meta.get('lat'))
        item_loader.add_value("latitude", response.meta.get('lng'))
        rented = response.xpath("//div[@class='mt-30']/div/text()[contains(.,'Let')]").get()
        if rented:
            return
        f_text = "".join(response.xpath("//div[h3[.='Description']]/div//text()").getall())
        if get_p_type_string(f_text):
            item_loader.add_value("property_type", get_p_type_string(f_text))
        else:
            return  
        room = response.xpath("//div[@class='container']//span[contains(.,'Bedroom')]/span/text()[.!='0']").get()
        if room:
            item_loader.add_value("room_count", room)
        elif get_p_type_string(f_text) and "studio" in get_p_type_string(f_text):
            item_loader.add_value("room_count", "1")

        item_loader.add_xpath("bathroom_count", "//div[@class='container']//span[contains(.,'Bathroom')]/span/text()")
        square_meters = response.xpath("//div[@class='c-features__item'][contains(.,'Sq') or contains(.,'sq') or contains(.,'SQ') ]/text()").get()
        if square_meters:
            unit_pattern = re.findall(r"[+-]? *((?:\d+(?:\.\d*)?|\.\d+)(?:[eE][+-]?\d+)?)\s*(Sq. Ft.|SQ|sq. ft.|Sq. ft.|sq. Ft.|sq|Sq)",square_meters.replace(",",""))
            if unit_pattern:
                square_title=unit_pattern[0][0]
                sqm = str(int(float(square_title) * 0.09290304))
                item_loader.add_value("square_meters", sqm)

        available_date=response.xpath("//div[@class='c-features__item'][contains(.,'AVAILABLE') or contains(.,'Available ')]/text()").get()
        if available_date:
            date_parsed = dateparser.parse(
                available_date.lower().split("available")[-1], date_formats=["%m-%d-%Y"]
            )
            if date_parsed:
                item_loader.add_value("available_date", date_parsed.strftime("%Y-%m-%d"))
        rent = response.xpath("//span[@class='inline-block mr-10']/text()").get()
        if rent:
            rent = rent.split("£")[1].strip().split(" ")[0].replace(",","")
            item_loader.add_value("rent", int(rent)*4)
        item_loader.add_value("currency", "GBP")
  
        address = response.xpath("//div[@class='container']//h1/text()").get()
        if address:
            address = address.split(" - ")[0]
            item_loader.add_value("title",address)
            item_loader.add_value("address",address)
            #item_loader.add_value("city", address.split(",")[-1].strip())
            item_loader.add_value("city", "London")

        zipcode = address.split(",")[-1].strip()
        if zipcode=="Camden":
            pass
        elif zipcode=="London":
            pass
        elif zipcode=="Islington":
            pass
        elif zipcode=="Bergholt Mews":
            pass
        else:
            item_loader.add_value("zipcode", zipcode)
   
        desc = "".join(response.xpath("//div[h3[.='Description']]/div//text()").extract())
        if desc:
            item_loader.add_value("description", desc.strip())

        images = [x for x in response.xpath("//img[@class='w-full h-full object-cover object-center']/@src").extract()]
        if images:
            item_loader.add_value("images", images) 

        floor_plan_images = [x for x in response.xpath("//div[@data-target='Floorplans']//img/@src").extract()]
        if floor_plan_images:
            item_loader.add_value("floor_plan_images", floor_plan_images) 

        energy_label = response.xpath("//div[@class='c-features__item'][contains(.,'EPC ')]/text()").get()
        if energy_label:
            item_loader.add_value("energy_label", energy_label.split("EPC")[-1].upper().split("BAND")[-1].replace("-","").replace("RATING","").strip())
        
        furnished = response.xpath("//div[@class='c-features__item'][contains(.,'Furnished') or contains(.,'FURNISHED') or contains(.,'furnished')]/text()").get()
        if furnished:
            if "unfurnished" in furnished.lower():   
                item_loader.add_value("furnished", False)
            elif "furnished" in furnished.lower():
                item_loader.add_value("furnished", True)

        terrace = response.xpath("//div[@class='c-features__item'][contains(.,'terrace')]/text()").get()
        if terrace:
            item_loader.add_value("terrace", True)        
        balcony = response.xpath("//div[@class='c-features__item'][contains(.,'BALCONY')]/text()").get()
        if balcony:
            item_loader.add_value("balcony", True) 
        item_loader.add_value("landlord_name", "Hotblack Desiato")
        item_loader.add_value("landlord_phone", "020 7226 0160")
   
        yield item_loader.load_item()

def get_p_type_string(p_type_string):
    if p_type_string and "student" in p_type_string.lower():
        return "student_apartment"
    elif p_type_string and "studio" in p_type_string.lower():
        return "studio"
    elif p_type_string and ("apartment" in p_type_string.lower() or "appartement" in p_type_string.lower() or "bovenwoning" in p_type_string.lower() or "flat" in p_type_string.lower()):
        return "apartment"
    elif p_type_string and ("house" in p_type_string.lower() or "cottage" in p_type_string.lower() or "terrace" in p_type_string.lower() or "maisonette" in p_type_string.lower()):
        return "house"    
    else:
        return None