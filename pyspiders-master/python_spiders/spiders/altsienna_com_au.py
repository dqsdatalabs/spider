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
    name = 'altsienna_com_au'
    execution_type='testing'
    country='australia'
    locale='en'
    thousand_separator = ','
    scale_separator = '.'       
    
    start_urls = ["https://www.altsienna.com.au/properties"]

    # 1. FOLLOWING
    def parse(self, response):
        for item in response.xpath("//div[@class='prop_archive']//div[@class='small_properties']/a"):
            status = item.xpath(".//div[@class='available']/text()").get()
            if status and ("leased" in status.lower()):
                continue
            follow_url = item.xpath("./@href").get()
            yield Request(follow_url, callback=self.populate_item)
        

    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        item_loader.add_value("external_link", response.url)
        p_type = "".join(response.xpath("//div[contains(@class,'text_content s_prop_text')]//text()").getall())
        if get_p_type_string(p_type):
            p_type = get_p_type_string(p_type)
            item_loader.add_value("property_type", p_type)
        else:
            return
        item_loader.add_value("external_source", "Altsienna_Com_PySpider_australia")          
        item_loader.add_xpath("title","//div[@class='s_prop_head']/h1//text()")
        item_loader.add_xpath("room_count", "//div[@class='s_prop_head']//li[@class='bedroom']/span/text()")
        item_loader.add_xpath("bathroom_count", "//div[@class='s_prop_head']//li[@class='bathroom']/span/text()")
        item_loader.add_xpath("rent_string", "//div[div[.='Monthly rental']]/div[2]/text()")
        item_loader.add_xpath("deposit", "//div[div[.='Bond']]/div[2]/text()")

        ext_id = response.xpath("//link[@rel='shortlink']/@href").get()
        if ext_id:
            ext_id = ext_id.split("?p=")[1].split("&")[0].strip()
            item_loader.add_value("external_id", ext_id)
  
        address = response.xpath("//div[@class='s_prop_head']/h1//text()").get()
        if address:
            item_loader.add_value("address", address.strip())
            item_loader.add_value("zipcode", f"VIC {address.split(',')[-1].strip().split(' ')[-1].strip()}")
            city = address.split(",")[-2].strip()
            if "Travancore" in address:
                city = "Travancore"
            if "Road" not in city or "Rd" not in city:
                item_loader.add_value("city", city.strip()) 

        furnished = response.xpath("//div[div[.='Furnished']]/div[2]/text()").get()
        if furnished:
            if furnished.strip().lower() == "no":
                item_loader.add_value("furnished", False)
            elif furnished.strip().lower() == "yes":
                item_loader.add_value("furnished", True)
        parking = response.xpath("//div[@class='s_prop_head']//li[@class='parking']/span/text()").get()
        if parking:
            item_loader.add_value("parking", True) if parking.strip() != "0" else item_loader.add_value("parking", False)
        
        available_date = response.xpath("//div[div[.='Available']]/div[2]/text()").get()
        if available_date:
            date_parsed = dateparser.parse(available_date.strip(), date_formats=["%d %m %Y"])
            if date_parsed:
                date2 = date_parsed.strftime("%Y-%m-%d")
                item_loader.add_value("available_date", date2)
        description = " ".join(response.xpath("//div[contains(@class,'s_prop_text')][1]/p//text()").getall()) 
        if description:
            item_loader.add_value("description", description.strip())

        images = [x.split("url('")[1].split("')")[0] for x in response.xpath("//div[@class='gp_gallery_wrap']//div[@class='gp_gallery_img horizontal']/@style").getall()]
        if images:
            item_loader.add_value("images", images)

        latitude_longitude = response.xpath("//a/@href[contains(.,'map')]").get()
        if latitude_longitude:
            latitude = latitude_longitude.split("/@")[1].split(",")[0]
            longitude = latitude_longitude.split("/@")[1].split(",")[1].split(",")[-1]
            item_loader.add_value("latitude", latitude)
            item_loader.add_value("longitude", longitude)
        
        item_loader.add_xpath("landlord_name", "//div[@class='s_prop_person']//div[@class='name']/text()")
        item_loader.add_xpath("landlord_phone", "//div[@class='s_prop_person']//div[@class='phone']/a/text()")
        item_loader.add_value("landlord_email", "propertymanagement@altsienna.com.au")
        
        yield item_loader.load_item()

def get_p_type_string(p_type_string):
    if p_type_string and "studio" in p_type_string.lower():
        return "studio"
    elif p_type_string and ("apartment" in p_type_string.lower() or "flat" in p_type_string.lower()):
        return "apartment"
    elif p_type_string and ("house" in p_type_string.lower() or "villa" in p_type_string.lower() or "home" in p_type_string.lower()):
        return "house"
    else:
        return None